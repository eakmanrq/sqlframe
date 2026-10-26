"""Trino SQL generation and explicit-schema query execution."""

from sqlframe.trino.session import TrinoSession

from . import functions

__all__ = ["TrinoSession", "functions"]
