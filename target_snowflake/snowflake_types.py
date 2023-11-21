from __future__ import annotations

import datetime as dt
import typing as t

import snowflake.sqlalchemy.custom_types as sct


class TIMESTAMP_NTZ(sct.TIMESTAMP_NTZ):  # noqa: N801
    """Snowflake TIMESTAMP_NTZ type."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    @property
    def python_type(self):
        return dt.datetime


class NUMBER(sct.NUMBER):
    """Snowflake NUMBER type."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    @property
    def python_type(self):
        return float


class VARIANT(sct.VARIANT):
    """Snowflake VARIANT type."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    @property
    def python_type(self):
        return dict
