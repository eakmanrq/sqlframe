from __future__ import annotations

from sqlframe.standalone.session import StandaloneSession
from sqlframe.trino.catalog import TrinoCatalog
from sqlframe.trino.readwriter import TrinoDataFrameWriter


class TrinoSession(StandaloneSession):
    """Plan with SQLFrame and execute Trino SQL using a supplied DB-API connection.

    Connected sessions can discover table columns from Trino. Offline sessions
    need schemas registered with ``catalog.add_table`` before reading tables.
    """

    _catalog = TrinoCatalog
    _writer = TrinoDataFrameWriter

    def _collect(self, *args, **kwargs):
        if not self._has_connection:
            raise ValueError("Trino collection requires a DB-API connection")
        return super()._collect(*args, **kwargs)

    class Builder(StandaloneSession.Builder):
        DEFAULT_OUTPUT_DIALECT = "trino"
        DEFAULT_EXECUTION_DIALECT = "trino"

        @property
        def session(self) -> TrinoSession:
            return TrinoSession(**self._session_kwargs)

        def getOrCreate(self) -> TrinoSession:
            return super().getOrCreate()  # type: ignore[return-value]

    builder = Builder()
