# Copyright (C) 2026 Meltano.

from __future__ import annotations

import datetime as dt
import sys
import typing as t

import snowflake.sqlalchemy.custom_types as sct
from sqlalchemy.types import DateTime

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TIMESTAMP_TZ(sct.TIMESTAMP_TZ):  # noqa: N801
    """Snowflake TIMESTAMP_TZ type."""

    @override
    @property
    def python_type(self):
        return dt.datetime

    @override
    def as_generic(self, allow_nulltype: bool = False) -> DateTime:
        return DateTime(timezone=True)


class TIMESTAMP_LTZ(sct.TIMESTAMP_LTZ):  # noqa: N801
    """Snowflake TIMESTAMP_LTZ type."""

    @override
    @property
    def python_type(self):
        return dt.datetime

    @override
    def as_generic(self, allow_nulltype: bool = False) -> DateTime:
        return DateTime(timezone=True)


class TIMESTAMP_NTZ(sct.TIMESTAMP_NTZ):  # noqa: N801
    """Snowflake TIMESTAMP_NTZ type."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    @override
    @property
    def python_type(self):
        return dt.datetime

    @override
    def as_generic(self, allow_nulltype: bool = False) -> DateTime:
        return DateTime()


class NUMBER(sct.NUMBER):
    """Snowflake NUMBER type."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    @override
    @property
    def python_type(self):
        return float


class VARIANT(sct.VARIANT):
    """Snowflake VARIANT type."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    @override
    @property
    def python_type(self):
        return dict
