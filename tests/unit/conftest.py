import findspark
import pytest

from sqlframe import activate, deactivate


@pytest.fixture()
def check_pyspark_imports():
    def _check_pyspark_imports(
        engine_name,
        sqlf_session,
        sqlf_catalog,
        sqlf_column,
        sqlf_dataframe,
        sqlf_grouped_data,
        sqlf_window,
        sqlf_window_spec,
        sqlf_functions,
        sqlf_types,
        sqlf_udf_registration,
        sqlf_dataframe_reader,
        sqlf_dataframe_writer,
        sqlf_dataframe_na_functions,
        sqlf_dataframe_stat_functions,
        sqlf_row,
    ):
        activate(engine=engine_name)
        findspark.init()
        from pyspark.sql import (
            Catalog,
            Column,
            DataFrame,
            DataFrameNaFunctions,
            DataFrameReader,
            DataFrameStatFunctions,
            DataFrameWriter,
            GroupedData,
            Row,
            SparkSession,
            UDFRegistration,
            Window,
            WindowSpec,
            types,
        )
        from pyspark.sql import functions as F

        assert SparkSession == sqlf_session
        assert Catalog == sqlf_catalog
        assert Column == sqlf_column
        assert GroupedData == sqlf_grouped_data
        assert DataFrame == sqlf_dataframe
        assert Window == sqlf_window
        assert WindowSpec == sqlf_window_spec
        assert F == sqlf_functions
        assert types == sqlf_types
        assert UDFRegistration == sqlf_udf_registration
        assert DataFrameNaFunctions == sqlf_dataframe_na_functions
        assert DataFrameStatFunctions == sqlf_dataframe_stat_functions
        assert DataFrameReader == sqlf_dataframe_reader
        assert DataFrameWriter == sqlf_dataframe_writer
        assert Row == sqlf_row

        from pyspark.sql.session import SparkSession as SparkSession2

        assert SparkSession2 == sqlf_session

        from pyspark.sql.catalog import Catalog as Catalog2

        assert Catalog2 == sqlf_catalog

        from pyspark.sql.column import Column as Column2

        assert Column2 == sqlf_column

        from pyspark.sql.dataframe import DataFrame as DataFrame2
        from pyspark.sql.dataframe import DataFrameNaFunctions as DataFrameNaFunctions2
        from pyspark.sql.dataframe import (
            DataFrameStatFunctions as DataFrameStatFunctions2,
        )

        assert DataFrame2 == sqlf_dataframe
        assert DataFrameNaFunctions2 == sqlf_dataframe_na_functions
        assert DataFrameStatFunctions2 == sqlf_dataframe_stat_functions

        from pyspark.sql.group import GroupedData as GroupedData2

        assert GroupedData2 == sqlf_grouped_data

        from pyspark.sql.window import WindowSpec as WindowSpec2

        assert WindowSpec2 == sqlf_window_spec

        from pyspark.sql.readwriter import DataFrameReader as DataFrameReader2
        from pyspark.sql.readwriter import DataFrameWriter as DataFrameWriter2

        assert DataFrameReader2 == sqlf_dataframe_reader
        assert DataFrameWriter2 == sqlf_dataframe_writer

        from pyspark.sql.window import Window, WindowSpec

        assert Window == sqlf_window
        assert WindowSpec == sqlf_window_spec

        from pyspark.sql import functions as F
        from pyspark.sql import types

        assert F == sqlf_functions
        assert types == sqlf_types
        assert types.Row == sqlf_row

        from pyspark.sql import UDFRegistration

        assert UDFRegistration == sqlf_udf_registration

        import pyspark.sql.functions as F
        import pyspark.sql.types as types

        assert F == sqlf_functions
        assert types == sqlf_types

        deactivate()
        findspark.init()
        from pyspark.sql import (
            Catalog,
            Column,
            DataFrame,
            DataFrameNaFunctions,
            DataFrameReader,
            DataFrameStatFunctions,
            DataFrameWriter,
            GroupedData,
            Row,
            SparkSession,
            UDFRegistration,
            Window,
            WindowSpec,
            types,
        )
        from pyspark.sql import functions as F

        assert SparkSession != sqlf_session
        assert Catalog != sqlf_catalog
        assert Column != sqlf_column
        assert GroupedData != sqlf_grouped_data
        assert DataFrame != sqlf_dataframe
        assert Window != sqlf_window
        assert WindowSpec != sqlf_window_spec
        assert F != sqlf_functions
        assert types != sqlf_types
        assert UDFRegistration != sqlf_udf_registration
        assert DataFrameNaFunctions != sqlf_dataframe_na_functions
        assert DataFrameStatFunctions != sqlf_dataframe_stat_functions
        assert DataFrameReader != sqlf_dataframe_reader
        assert DataFrameWriter != sqlf_dataframe_writer
        assert Row != sqlf_row

        assert F.JVMView is not None
        assert SparkSession.readStream is not None

        from pyspark.sql.session import SparkSession as SparkSession2

        assert SparkSession2 != sqlf_session

        from pyspark.sql.catalog import Catalog as Catalog2

        assert Catalog2 != sqlf_catalog

        from pyspark.sql.column import Column as Column2

        assert Column2 != sqlf_column

        from pyspark.sql.dataframe import DataFrame as DataFrame2
        from pyspark.sql.dataframe import DataFrameNaFunctions as DataFrameNaFunctions2
        from pyspark.sql.dataframe import (
            DataFrameStatFunctions as DataFrameStatFunctions2,
        )

        assert DataFrame2 != sqlf_dataframe
        assert DataFrameNaFunctions2 != sqlf_dataframe_na_functions
        assert DataFrameStatFunctions2 != sqlf_dataframe_stat_functions

        from pyspark.sql.group import GroupedData as GroupedData2

        assert GroupedData2 != sqlf_grouped_data

        from pyspark.sql.window import WindowSpec as WindowSpec2

        assert WindowSpec2 != sqlf_window_spec

        from pyspark.sql.readwriter import DataFrameReader as DataFrameReader2
        from pyspark.sql.readwriter import DataFrameWriter as DataFrameWriter2

        assert DataFrameReader2 != sqlf_dataframe_reader
        assert DataFrameWriter2 != sqlf_dataframe_writer

        from pyspark.sql.window import Window, WindowSpec

        assert Window != sqlf_window
        assert WindowSpec != sqlf_window_spec

        from pyspark.sql import functions as F
        from pyspark.sql import types

        assert F != sqlf_functions
        assert types != sqlf_types
        assert types.Row != sqlf_row

        from pyspark.sql import UDFRegistration

        assert UDFRegistration != sqlf_udf_registration

        import pyspark.sql.functions as F
        import pyspark.sql.types as types

        assert F != sqlf_functions
        assert types != sqlf_types

    return _check_pyspark_imports
