# TikTok reviews → MongoDB pipeline

Two Airflow DAGs process `data/tiktok_google_play_reviews.csv` (307,057 reviews, 80 MB) and load the result
into MongoDB (`airflow_data.reviews`).

```
dag_1  wait_for_file → check_file_not_empty ─┬→ log_empty_file
                                             └→ process_data: csv_to_parquet → replace_nulls
                                                              → sort_by_created_date → clean_content
                                                                                          │ updates asset
                                                                                          ▼
                                                     tiktok_reviews_processed (final CSV)
                                                                                          │ triggers
                                                                                          ▼
dag_2  load_to_mongo
```

![DAG in the Airflow UI](dag%20in%20airflow.png)

## dag_1: `tiktok_reviews_to_mongo`

Code: `dags/dag_1/` (`main.py` is the DAG, `transforms.py` the processing steps, `adapters.py` the storage
code, `assets.py` the asset it produces).

Runs daily (`@daily`, from 2026-11-01, no catch-up, one active run at a time).

| Task | What it does |
|---|---|
| `wait_for_file` | `FileSensor` that checks for the input file every 30 s for up to 24 h |
| `check_file_not_empty` | Branches: continues to `process_data` if the file has at least one data row, otherwise to `log_empty_file` |
| `log_empty_file` | Logs that the file is empty; the processing tasks are skipped and `dag_2` does not run |
| `csv_to_parquet` | Converts the input CSV to Parquet for the next steps |
| `replace_nulls` | Replaces empty and `null` cells with `-` |
| `sort_by_created_date` | Sorts the reviews by `at`, the review's creation time (the dataset has no `created_date` column) |
| `clean_content` | Removes emoji and other non-text characters from `content` and writes the final dataset, `data/processed/tiktok_reviews_processed.csv` |

The processing steps pass Parquet files to each other through `data/staging/`. Only the final dataset is CSV.

## dag_2: `load_reviews_to_mongo`

Code: `dags/dag_2/main.py`.

It has one task, `load_to_mongo`, which reads the final CSV and replaces the whole MongoDB collection with it,
so a rerun never duplicates documents. `score` and `thumbsUpCount` are stored as integers, `at` and
`repliedAt` as BSON Dates (UTC); `-` (missing value) stays `-`. A value that doesn't convert fails the task.

`dags/dag_2/queries.js` holds the MongoDB aggregations run on the loaded collection.

### How dag_2 is triggered

`dag_2` uses data-aware scheduling (Airflow 3 assets):

- `dags/dag_1/assets.py` defines the asset `tiktok_reviews_processed`, which is the final CSV.
- In `dag_1`, `clean_content` lists that asset as its outlet, so its success records an asset update.
- `dag_2` has `schedule=[PROCESSED_REVIEWS]`, so every update starts a run (run id `asset_triggered__…`).

If `dag_1` stops early, for example because the input file is empty, the asset is not updated and `dag_2`
does not run.

## Optimizations

### Result

| | Before | After |
|---|---|---|
| Input file → data in MongoDB | 17.6 – 20.2 s (one DAG) | **11.0 s** (`dag_1` 7.7 s + `dag_2` 3.3 s) |

The output did not change: all 307,057 rows match the old pipeline's output, in the same order.

### Where the time went before

| Part of the run | Time | Share |
|---|---|---|
| Reading and writing CSV between tasks | ~7.4 s | 39 % |
| Loading into MongoDB | ~3.6 s | 19 % |
| Scheduler gaps between tasks | ~3.2 s | 17 % |
| The transforms themselves | ~2.5 s | 13 % |
| Importing pandas in every task | ~1.0 s | 5 % |

### Per task

| Task | Before | After |
|---|---|---|
| `wait_for_file` | 0.07 s | 0.08 s |
| `check_file_not_empty` | 0.25 s | 0.28 s |
| `csv_to_parquet` | — | 0.36 s (new) |
| `replace_nulls` | 3.80 s | 0.55 s |
| `sort_by_created_date` | 2.17 s | 0.54 s |
| `clean_content` | 4.47 s | 1.43 s |
| `load_to_mongo` | 4.80 s | 2.92 s |

### What changed

**Staging between tasks.**
- *Before:* every task read an 80 MB CSV, parsed all values from text, and wrote the whole table back to CSV
  (~1.0 s per read, ~1.15 s per write).
- *After:* staging files are Parquet (`ParquetTableStore`). A write takes 0.15 s, a read 0.05 s, and the
  files are 54 MB.

**`csv_to_parquet` (new task).**
- *Before:* each step parsed the input CSV again.
- *After:* the CSV is parsed once with PyArrow's multi-threaded reader (`newlines_in_values=True`, because
  reviews can contain line breaks). `pd.read_csv(engine="pyarrow")` was tested and crashes when a multi-line
  review crosses one of its internal blocks.

**`replace_nulls`.**
- *Before:* `df.replace(r"(?i)^(null)?$", ..., regex=True)` ran a Python regex on ~3 million cells: 0.74 s.
- *After:* `col.str.fullmatch(...)` + `col.mask(...)` on Arrow-backed strings, in compiled code: 0.07 s.

**`sort_by_created_date`.**
- *Before:* the sort was already fast (0.07 s); the task was slow only because of CSV I/O.
- *After:* logic unchanged; Parquet staging made the task faster.

**`clean_content`.**
- *Before:* a Python function was called for every character of every review: 1.7 s.
- *After:* the same rule builds a translation table once per Unicode code point, and `str.translate` removes
  the characters in C: 0.58 s.

**`load_to_mongo`.**
- *Before:* the CSV was read with pandas and converted with `DataFrame.to_dict("records")`, building 307,000
  dicts slowly in Python: ~2.0 s before anything was sent to MongoDB.
- *After:* records are built by PyArrow (`Table.to_pylist()`) without pandas. The MongoDB delete and insert are
  unchanged.

### Behaviour changes

- Rows with too few fields now stop the run at `csv_to_parquet` with an error naming the row. Before, they were
  accepted with missing values.
- A cell containing only `null` plus a line break, or only a line break, now becomes `-` instead of `-`
  followed by a line break. This does not occur in the current data.

### Not changed

- **More workers / higher `worker_concurrency`:** the tasks run one after another, so it would not help.
- **Lower `scheduler_idle_sleep_time`:** might save 1 – 2 s of scheduler gaps at the cost of more CPU. Not tested.
- **Merging the three transforms into one task:** would save a few seconds but lose per-step retries and
  visibility in the UI.
