"""Loads the processed reviews CSV into MongoDB whenever tiktok_reviews_to_mongo (dags/dag_1) updates it."""

import os

from airflow.sdk import dag, task

from dag_1.adapters import DocumentSink, MongoSink, read_csv_records
from dag_1.assets import NULL_PLACEHOLDER, PROCESSED_REVIEWS, PROCESSED_REVIEWS_FILE

SINK: DocumentSink = MongoSink(
    conn_id=os.environ.get("MONGO_CONN_ID", "mongo_default"),
    database=os.environ.get("MONGO_DB", "airflow_data"),
    collection=os.environ.get("MONGO_COLLECTION", "reviews"),
)

# Stored as numbers and BSON Dates instead of strings. A "-" (missing value) stays "-".
INT_COLUMNS = ("score", "thumbsUpCount")
TIMESTAMP_COLUMNS = ("at", "repliedAt")


@dag(
    dag_id="load_reviews_to_mongo",
    schedule=[PROCESSED_REVIEWS],
    max_active_runs=1,
    tags=["reviews"],
)
def load_reviews_to_mongo():
    @task
    def load_to_mongo() -> int:
        records = read_csv_records(
            PROCESSED_REVIEWS_FILE,
            int_columns=INT_COLUMNS,
            timestamp_columns=TIMESTAMP_COLUMNS,
            placeholder=NULL_PLACEHOLDER,
        )
        return SINK.replace_all(records)

    load_to_mongo()


load_reviews_to_mongo()
