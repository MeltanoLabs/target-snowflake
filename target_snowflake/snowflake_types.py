from __future__ import annotations

import datetime as dt
import typing as t

import snowflake.sqlalchemy.custom_types as sct
from sqlalchemy.types import DateTime
from typing_extensions import override

if t.TYPE_CHECKING:
    import sqlalchemy.sql.type_api


class TIMESTAMP_TZ(sct.TIMESTAMP_TZ):  # noqa: N801
    """Snowflake TIMESTAMP_TZ type."""

    @property
    def python_type(self):
        return dt.datetime

    @override
    def as_generic(self, allow_nulltype: bool = False) -> sqlalchemy.sql.type_api.TypeEngine[dt.datetime]:
        return DateTime(timezone=True)


class TIMESTAMP_LTZ(sct.TIMESTAMP_LTZ):  # noqa: N801
    """Snowflake TIMESTAMP_LTZ type."""

    @property
    def python_type(self):
        return dt.datetime

    @override
    def as_generic(self, allow_nulltype: bool = False) -> sqlalchemy.sql.type_api.TypeEngine[dt.datetime]:
        return DateTime(timezone=True)


class TIMESTAMP_NTZ(sct.TIMESTAMP_NTZ):  # noqa: N801
    """Snowflake TIMESTAMP_NTZ type."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    @property
    def python_type(self):
        return dt.datetime

    @override
    def as_generic(self, allow_nulltype: bool = False) -> sqlalchemy.sql.type_api.TypeEngine[dt.datetime]:
        return DateTime()


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
