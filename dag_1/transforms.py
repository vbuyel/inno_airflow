from __future__ import annotations

import logging
import re
import string
import sys
import unicodedata
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger(__name__)


class Transform(ABC):
    """One processing step. `name` doubles as its Airflow task id and staging file name."""

    name: ClassVar[str]

    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame: ...


@dataclass(frozen=True)
class ReplaceNulls(Transform):
    placeholder: str

    name: ClassVar[str] = "replace_nulls"
    null_cell: ClassVar[str] = r"(?i)(null)?\n?"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.apply(lambda col: col.mask(col.str.fullmatch(self.null_cell), self.placeholder))


@dataclass(frozen=True)
class SortByDate(Transform):
    column: str
    placeholder: str

    name: ClassVar[str] = "sort_by_created_date"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        import pandas as pd

        dates = pd.to_datetime(df[self.column], errors="coerce", format="mixed")
        unparsed = int((dates.isna() & (df[self.column] != self.placeholder)).sum())
        if unparsed:
            log.warning("%d %s values are not dates, sorted last", unparsed, self.column)

        return df.loc[dates.sort_values(kind="stable", na_position="last").index]


@dataclass(frozen=True)
class CleanText(Transform):
    column: str
    placeholder: str

    name: ClassVar[str] = "clean_content"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        cleaned = df[self.column].map(self._strip_non_text)
        return df.assign(**{self.column: cleaned.where(cleaned != "", self.placeholder)})

    @classmethod
    def _strip_non_text(cls, text: str) -> str:
        """Keep letters, digits, combining marks, punctuation and whitespace."""
        kept = text.translate(cls._non_text_table())
        return re.sub(r"[ \t]+", " ", kept).strip()

    @classmethod
    @cache
    def _non_text_table(cls) -> dict[int, None]:
        """Evaluate `_is_text` once per code point, so `str.translate` can apply it in C."""
        return {cp: None for cp in range(sys.maxunicode + 1) if not cls._is_text(chr(cp))}

    @staticmethod
    def _is_text(char: str) -> bool:
        category = unicodedata.category(char)
        return (
            char in string.punctuation
            or char.isspace()
            or category[0] in "LNP"
            or (category in ("Mn", "Mc") and not "︀" <= char <= "️")
        )
