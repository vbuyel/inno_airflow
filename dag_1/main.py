from pathlib import Path

import pendulum

from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

from dag_1.adapters import ParquetTableStore, TableStore
from dag_1.assets import NULL_PLACEHOLDER, PROCESSED_REVIEWS, PROCESSED_REVIEWS_FILE
from dag_1.transforms import CleanText, ReplaceNulls, SortByDate, Transform

DATA_DIR = Path("/opt/airflow/data")
DATA_FILE = str(DATA_DIR / "tiktok_google_play_reviews.csv")
GROUP_ID = "process_data"
IMPORT_TASK_ID = "csv_to_parquet"

STORE: TableStore = ParquetTableStore(staging_dir=DATA_DIR / "staging")

TRANSFORMS: list[Transform] = [
    ReplaceNulls(placeholder=NULL_PLACEHOLDER),
    SortByDate(column="at", placeholder=NULL_PLACEHOLDER),
    CleanText(column="content", placeholder=NULL_PLACEHOLDER),
]


def transform_task(transform: Transform):
    @task(task_id=transform.name)
    def run(location: str) -> str:
        return STORE.write(transform.apply(STORE.read(location)), transform.name)

    return run


def final_transform_task(transform: Transform):
    """Like `transform_task`, but writes the result as the PROCESSED_REVIEWS CSV, which triggers dags/dag_2."""

    @task(task_id=transform.name, outlets=[PROCESSED_REVIEWS])
    def run(location: str) -> str:
        return STORE.export_csv(transform.apply(STORE.read(location)), PROCESSED_REVIEWS_FILE)

    return run


@dag(
    dag_id="tiktok_reviews_to_mongo",
    # Runs once a day; each run's sensor then waits up to 24 h for the file to appear.
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["reviews"],
)
def tiktok_reviews_to_mongo():
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_default",
        filepath=DATA_FILE,
        poke_interval=30,
        timeout=24 * 60 * 60,
        mode="reschedule",
    )

    @task.branch
    def check_file_not_empty() -> str:
        return f"{GROUP_ID}.{IMPORT_TASK_ID}" if STORE.has_rows(DATA_FILE) else "log_empty_file"

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f'echo "File {DATA_FILE} is empty, nothing to process."',
    )

    @task(task_id=IMPORT_TASK_ID)
    def csv_to_parquet() -> str:
        return STORE.import_csv(DATA_FILE, IMPORT_TASK_ID)

    # Intermediate results are Parquet; the last transform writes the final CSV, which triggers
    # load_reviews_to_mongo (dags/dag_2).
    with TaskGroup(GROUP_ID) as process_data:
        location = csv_to_parquet()
        for transform in TRANSFORMS[:-1]:
            location = transform_task(transform)(location)
        final_transform_task(TRANSFORMS[-1])(location)

    wait_for_file >> check_file_not_empty() >> [log_empty_file, process_data]


tiktok_reviews_to_mongo()
