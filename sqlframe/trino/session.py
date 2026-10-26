from __future__ import annotations

from sqlframe.standalone.session import StandaloneSession


class TrinoSession(StandaloneSession):
    """Plan with SQLFrame and execute Trino SQL using a supplied DB-API connection.

    Table schemas must be registered with ``catalog.add_table`` before reading them.
    """

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
