from __future__ import annotations

import typing as t

from sqlframe.base.udf import _BaseUDFRegistration

if t.TYPE_CHECKING:
    from sqlframe.postgres.session import PostgresSession


class PostgresUDFRegistration(_BaseUDFRegistration["PostgresSession"]): ...
