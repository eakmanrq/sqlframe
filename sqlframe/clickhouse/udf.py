from __future__ import annotations

import typing as t

from sqlframe.base.udf import _BaseUDFRegistration

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.session import ClickHouseSession


class ClickHouseUDFRegistration(_BaseUDFRegistration["ClickHouseSession"]):
    pass
