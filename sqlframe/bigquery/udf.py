from __future__ import annotations

import typing as t

from sqlframe.base.udf import _BaseUDFRegistration

if t.TYPE_CHECKING:
    from sqlframe.bigquery.session import BigQuerySession


class BigQueryUDFRegistration(_BaseUDFRegistration["BigQuerySession"]): ...
