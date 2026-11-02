"""Storage interfaces the DAGs depend on, and their Parquet, CSV and MongoDB implementations."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

log = logging.getLogger(__name__)


class TableStore(ABC):
    """Holds tables between processing steps."""

    @abstractmethod
    def has_rows(self, source: str) -> bool:
        """Whether the source CSV holds at least one data row."""

    @abstractmethod
    def import_csv(self, source: str, name: str) -> str:
        """Store the source CSV and return the location to read it back from."""

    @abstractmethod
    def read(self, location: str) -> pd.DataFrame: ...

    @abstractmethod
    def write(self, df: pd.DataFrame, name: str) -> str:
        """Store the table and return the location to read it back from."""

    @abstractmethod
    def export_csv(self, df: pd.DataFrame, destination: str) -> str:
        """Write the table as CSV to `destination`, outside the store, and return that path."""


class DocumentSink(ABC):
    """Final destination of the processed records."""

    @abstractmethod
    def replace_all(self, documents: list[dict]) -> int:
        """Make the documents the sink's whole content and return how many were stored."""


def read_csv_records(
    source: str,
    *,
    int_columns: tuple[str, ...] = (),
    timestamp_columns: tuple[str, ...] = (),
    timestamp_format: str = "%Y-%m-%d %H:%M:%S",
    placeholder: str | None = None,
) -> list[dict]:
    """Read a CSV as one dict per row.

    `int_columns` become ints and `timestamp_columns` become (UTC) datetimes; every other value stays the
    string it is in the file. A cell equal to `placeholder` is kept as the placeholder string instead of
    being converted. Any other value that does not convert raises.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    def to_int(values: pa.ChunkedArray) -> pa.ChunkedArray:
        return values.cast(pa.int64())

    def to_timestamp(values: pa.ChunkedArray) -> pa.ChunkedArray:
        return pc.strptime(values, format=timestamp_format, unit="s")

    table = _read_csv(source)
    converters = {column: to_int for column in int_columns} | {column: to_timestamp for column in timestamp_columns}
    with_placeholder = []
    for column, convert in converters.items():
        values = table[column]
        if placeholder is not None:
            missing = pc.equal(values, placeholder)
            if pc.any(missing).as_py():
                with_placeholder.append(column)
                values = pc.if_else(missing, pa.scalar(None, pa.string()), values)
        table = table.set_column(table.schema.get_field_index(column), column, convert(values))

    records = table.to_pylist()
    # The CSV is read with strings_can_be_null=False, so a None here can only be a placeholder cell.
    for record in records:
        for column in with_placeholder:
            if record[column] is None:
                record[column] = placeholder
    return records


def _read_csv(source: str) -> pa.Table:
    import pyarrow as pa
    import pyarrow.csv as csv

    # Quoted values may span lines, which the default parse options reject.
    parse_options = csv.ParseOptions(newlines_in_values=True)
    with csv.open_csv(source, parse_options=parse_options) as reader:
        columns = reader.schema.names
    return csv.read_csv(
        source,
        parse_options=parse_options,
        convert_options=csv.ConvertOptions(
            column_types={column: pa.string() for column in columns},
            strings_can_be_null=False,
        ),
    )


@dataclass(frozen=True)
class ParquetTableStore(TableStore):
    staging_dir: Path

    def has_rows(self, source: str) -> bool:
        import pandas as pd
        try:
            return not pd.read_csv(source, nrows=1).empty
        except pd.errors.EmptyDataError:
            return False

    def import_csv(self, source: str, name: str) -> str:
        import pyarrow.parquet as pq

        path = self._path(name)
        pq.write_table(_read_csv(source), path)
        return str(path)

    def read(self, location: str) -> pd.DataFrame:
        import pandas as pd
        return pd.read_parquet(location)

    def write(self, df: pd.DataFrame, name: str) -> str:
        path = self._path(name)
        df.to_parquet(path, index=False)
        return str(path)

    def export_csv(self, df: pd.DataFrame, destination: str) -> str:
        import pyarrow as pa
        import pyarrow.csv as csv

        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        csv.write_csv(pa.Table.from_pandas(df, preserve_index=False), destination)
        return destination

    def _path(self, name: str) -> Path:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        return self.staging_dir / f"{name}.parquet"


@dataclass(frozen=True)
class MongoSink(DocumentSink):
    conn_id: str
    database: str
    collection: str

    def replace_all(self, documents: list[dict]) -> int:
        from airflow.providers.mongo.hooks.mongo import MongoHook

        hook = MongoHook(mongo_conn_id=self.conn_id)
        hook.delete_many(self.collection, {}, mongo_db=self.database)
        hook.insert_many(self.collection, documents, mongo_db=self.database)

        log.info("Loaded %d documents into %s.%s", len(documents), self.database, self.collection)
        return len(documents)
