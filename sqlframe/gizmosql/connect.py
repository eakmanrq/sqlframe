from __future__ import annotations

from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql import dbapi as flightsql_dbapi


class GizmoSQLAdbcCursor:
    def __init__(self, parent: "GizmoSQLConnection", inner):
        self._parent = parent
        self._inner = inner
        self._closed = False

    # --- Context manager protocol -------------------------------------------------

    def __enter__(self) -> "GizmoSQLAdbcCursor":
        if self._closed:
            raise RuntimeError("Cursor already closed")
        return self

    def __exit__(self, exc_type, exc, tb):
        # Always close; never suppress the original exception.
        try:
            self.close()
        except Exception:
            # Avoid raising from __exit__; at worst log here.
            pass
        return False

    # --- DB-API-ish surface used by SQLFrame -------------------------------------

    def execute(self, *args, **kwargs):
        if self._closed:
            raise RuntimeError("Cursor is closed")
        return self._inner.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        if self._closed:
            raise RuntimeError("Cursor is closed")
        return self._inner.executemany(*args, **kwargs)

    def fetchone(self):
        if self._closed:
            raise RuntimeError("Cursor is closed")
        return self._inner.fetchone()

    def fetchall(self):
        if self._closed:
            raise RuntimeError("Cursor is closed")
        return self._inner.fetchall()

    def fetchmany(self, size=None):
        if self._closed:
            raise RuntimeError("Cursor is closed")
        if size is not None:
            return self._inner.fetchmany(size)
        return self._inner.fetchmany()

    @property
    def description(self):
        return None if self._closed else self._inner.description

    def close(self):
        if self._closed:
            return
        self._closed = True

        inner = self._inner
        self._inner = None

        try:
            if inner is not None:
                # First, try to close the underlying dbapi cursor.
                try:
                    inner.close()
                except Exception:
                    # Don't leak teardown noise from underlying ADBC.
                    pass

                # IMPORTANT:
                # Neutralize future close/__del__ calls on the inner cursor to avoid
                # double-closing the same AdbcStatement (which caused "Underflow" and
                # now AttributeError when _stmt became None).
                try:

                    def _noop_close(*_args, **_kwargs):
                        return None

                    # If this works, __del__ will call the no-op instead of the real close().
                    inner.close = _noop_close
                except Exception:
                    # Best-effort; if we can't assign, we still tried.
                    pass
        finally:
            try:
                self._parent._unregister_cursor(self)
            except Exception:
                pass

    def __iter__(self):
        if self._closed:
            return iter(())
        return iter(self._inner)

    def __del__(self):
        try:
            self.close()
        except Exception:
            # Never raise from GC
            pass


class GizmoSQLConnection:
    """
    Wrapper around flightsql_dbapi.connect() that:

    - Tracks open cursors.
    - Ensures cursors are closed before the underlying ADBC connection.
    - Supports `with` for both connection and cursor.
    - Exposes ADBC-ish attributes expected by sqlframe.gizmosql.
    """

    def __init__(self, **connect_kwargs):
        # E.g. uri / host / port / db / token, etc.
        self._conn = flightsql_dbapi.connect(**connect_kwargs)
        self._cursors: set[GizmoSQLAdbcCursor] = set()
        self._closed = False

    # --- Context manager protocol -------------------------------------------------

    def __enter__(self) -> "GizmoSQLConnection":
        if self._closed:
            raise RuntimeError("Connection already closed")
        return self

    def __exit__(self, exc_type, exc, tb):
        # Match typical DB-API semantics:
        # - If exception: rollback best-effort.
        # - Else: commit best-effort.
        # - Always: close, but don't hide the original exception.
        try:
            if exc_type is not None:
                try:
                    self.rollback()
                except Exception:
                    # Don't override the real failure.
                    pass
            else:
                try:
                    self.commit()
                except Exception:
                    # No original exception: a commit failure *should* propagate.
                    raise
        finally:
            try:
                self.close()
            except Exception:
                # Avoid raising from __exit__; pytest + GC are grumpy about that.
                if exc_type is None:
                    # If you *really* want close errors visible when no other error,
                    # you could re-raise here instead of pass.
                    pass

        return False  # never suppress caller exceptions

    # --- API used by SQLFrame -----------------------------------------------------

    def cursor(self) -> GizmoSQLAdbcCursor:
        if self._closed:
            raise RuntimeError("Connection already closed")

        inner_cur = self._conn.cursor()
        cur = GizmoSQLAdbcCursor(self, inner_cur)
        self._cursors.add(cur)
        return cur

    def commit(self):
        if self._closed:
            return
        # Some ADBC drivers are effectively autocommit; be defensive.
        meth = getattr(self._conn, "commit", None)
        if callable(meth):
            return meth()

    def rollback(self):
        if self._closed:
            return
        meth = getattr(self._conn, "rollback", None)
        if callable(meth):
            try:
                return meth()
            except Exception:
                # Rollback is best-effort.
                pass

    def close(self):
        if self._closed:
            return
        self._closed = True

        # Close children first so ADBC is happy.
        for c in list(self._cursors):
            try:
                c.close()
            except Exception:
                # Don't let one bad cursor poison the rest.
                pass
        self._cursors.clear()

        # Then close underlying connection.
        try:
            self._conn.close()
        except Exception:
            # At this stage we're in teardown territory; avoid noisy RuntimeError /
            # IO errors from dead servers, etc.
            pass

    def __del__(self):
        # Destructor: best-effort, never raise (prevents PytestUnraisableExceptionWarning).
        try:
            self.close()
        except Exception:
            pass

    # --- Internal helper ----------------------------------------------------------

    def _unregister_cursor(self, cursor: GizmoSQLAdbcCursor):
        self._cursors.discard(cursor)

    # --- Pass-through properties expected by GizmoSQLCatalog / others ------------

    @property
    def adbc(self):
        """Expose inner dbapi connection if callers need low-level access."""
        return self._conn

    @property
    def adbc_current_catalog(self):
        # sqlframe.gizmosql.catalog.currentCatalog() expects this.
        return getattr(self._conn, "adbc_current_catalog", None)

    @property
    def adbc_current_schema(self):
        # Adjust if your underlying connection uses a different name.
        return getattr(self._conn, "adbc_current_schema", None)

    # Optional: some codepaths might look for `.database` / `.schema`
    @property
    def database(self):
        return self.adbc_current_catalog
