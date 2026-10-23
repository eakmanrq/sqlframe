import pytest

from sqlframe import activate, deactivate
from sqlframe.base.util import classproperty
from sqlframe.spark import Column as SparkColumn
from sqlframe.spark import Row as SparkRow
from sqlframe.spark import (
    SparkCatalog,
    SparkDataFrame,
    SparkDataFrameNaFunctions,
    SparkDataFrameReader,
    SparkDataFrameStatFunctions,
    SparkDataFrameWriter,
    SparkGroupedData,
    SparkSession,
    SparkUDFRegistration,
)
from sqlframe.spark import Window as SparkWindow
from sqlframe.spark import WindowSpec as SparkWindowSpec
from sqlframe.spark import functions as SparkF
from sqlframe.spark import types as SparkTypes


def test_replace_pyspark_spark(check_pyspark_imports):
    check_pyspark_imports(
        "spark",
        sqlf_session=SparkSession,
        sqlf_catalog=SparkCatalog,
        sqlf_column=SparkColumn,
        sqlf_dataframe=SparkDataFrame,
        sqlf_grouped_data=SparkGroupedData,
        sqlf_window=SparkWindow,
        sqlf_window_spec=SparkWindowSpec,
        sqlf_functions=SparkF,
        sqlf_types=SparkTypes,
        sqlf_udf_registration=SparkUDFRegistration,
        sqlf_dataframe_reader=SparkDataFrameReader,
        sqlf_dataframe_writer=SparkDataFrameWriter,
        sqlf_dataframe_na_functions=SparkDataFrameNaFunctions,
        sqlf_dataframe_stat_functions=SparkDataFrameStatFunctions,
        sqlf_row=SparkRow,
    )


@pytest.mark.local
def test_activate_with_existing_pyspark_session_reuses_connection(monkeypatch):
    from pyspark.sql import SparkSession as PySparkSession

    spark = (
        PySparkSession.builder.master("local[1]")
        .appName("sqlframe-activate-existing-session")
        .getOrCreate()
    )
    # The shared test fixture replaces classproperty with a cached Builder.
    monkeypatch.setattr(SparkSession, "builder", classproperty(lambda cls: cls.Builder()))

    try:
        activate(engine="spark", conn=spark)
        from pyspark.sql import SparkSession as ActivatedSession

        sqlframe_session = ActivatedSession.builder.getOrCreate()
        assert sqlframe_session.spark_session is spark
    finally:
        deactivate()
        try:
            assert PySparkSession.builder.getOrCreate() is spark
            assert spark.sql("SELECT 1 AS value").collect()[0].value == 1
        finally:
            spark.stop()
