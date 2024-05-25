import typing as t

from sqlglot import exp

from sqlframe.base.catalog import Column
from sqlframe.base.dataframe import (
    GROUP_DATA,
    NA,
    SESSION,
    STAT,
    WRITER,
    _BaseDataFrame,
)


class PrintSchemaFromTempObjectsMixin(
    _BaseDataFrame, t.Generic[SESSION, WRITER, NA, STAT, GROUP_DATA]
):
    def _get_columns_from_temp_object(self) -> t.List[Column]:
        table = exp.to_table(self.session._random_id)
        self.session._execute(
            exp.Create(
                this=table,
                kind="VIEW",
                replace=True,
                properties=exp.Properties(expressions=[exp.TemporaryProperty()]),
                expression=self.expression,
            )
        )
        return self.session.catalog.listColumns(
            table.sql(dialect=self.session.input_dialect), include_temp=True
        )

    def printSchema(self, level: t.Optional[int] = None) -> None:
        def print_schema(
            column_name: str, column_type: exp.DataType, nullable: bool, current_level: int
        ):
            if level and current_level >= level:
                return
            if current_level > 0:
                print(" |   " * current_level, end="")
            print(
                f" |-- {column_name}: {column_type.sql(self.session.output_dialect).lower()} (nullable = {str(nullable).lower()})"
            )
            if column_type.this == exp.DataType.Type.STRUCT:
                for column_def in column_type.expressions:
                    print_schema(column_def.name, column_def.args["kind"], True, current_level + 1)
            if column_type.this == exp.DataType.Type.ARRAY:
                for data_type in column_type.expressions:
                    print_schema("element", data_type, True, current_level + 1)
            if column_type.this == exp.DataType.Type.MAP:
                print_schema("key", column_type.expressions[0], True, current_level + 1)
                print_schema("value", column_type.expressions[1], True, current_level + 1)

        columns = self._get_columns_from_temp_object()
        print("root")
        for column in columns:
            print_schema(
                column.name,
                exp.DataType.build(column.dataType, dialect=self.session.output_dialect),
                column.nullable,
                0,
            )
