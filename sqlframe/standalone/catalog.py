import typing as t

from sqlframe.base.catalog import _BaseCatalog

if t.TYPE_CHECKING:
    from sqlframe.standalone.dataframe import StandaloneDataFrame
    from sqlframe.standalone.session import StandaloneSession


class StandaloneCatalog(_BaseCatalog["StandaloneSession", "StandaloneDataFrame"]):
    """User-facing catalog API, accessible through `SparkSession.catalog`."""

    pass
