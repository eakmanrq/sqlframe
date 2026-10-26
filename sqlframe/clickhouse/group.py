from __future__ import annotations

import typing as t

from sqlframe.base.group import _BaseGroupedData

if t.TYPE_CHECKING:
    from sqlframe.clickhouse.dataframe import ClickHouseDataFrame


class ClickHouseGroupedData(_BaseGroupedData["ClickHouseDataFrame"]):
    pass
