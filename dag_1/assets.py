"""Assets this DAG produces. Downstream DAGs import them to be scheduled on this DAG's output."""

from pathlib import Path

from airflow.sdk import Asset

# The fully processed reviews, as CSV. The last transform writes it.
PROCESSED_REVIEWS_FILE = "/opt/airflow/data/processed/tiktok_reviews_processed.csv"
PROCESSED_REVIEWS = Asset(name="tiktok_reviews_processed", uri=Path(PROCESSED_REVIEWS_FILE).as_uri())

# Marks a missing value in the processed file (and, in `content`, a review with no text left).
NULL_PLACEHOLDER = "-"
