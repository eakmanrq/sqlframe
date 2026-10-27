import pytest

from sqlframe.base.exceptions import UnsupportedOperationError
from sqlframe.trino import TrinoSession


class Cursor:
    def __init__(self):
        self.statements = []
        self.target = [("id", "integer"), ("label", "varchar(10)")]
        self.source = [("id", "integer"), ("label", "varchar(1)")]
        self.exists = True
        self.table_type = "BASE TABLE"
        self.fail_on = None
        self.description = None

    def execute(self, statement):
        self.statements.append(statement)
        if self.fail_on and statement.startswith(self.fail_on):
            raise RuntimeError("metadata unavailable")
        if statement == "SELECT current_catalog, current_schema":
            names, self.rows = ["catalog", "schema"], [("memory", "default")]
        elif ".information_schema.tables" in statement:
            names = ["table_type"]
            self.rows = [(self.table_type,)] if self.exists else []
        elif statement.startswith("SHOW COLUMNS FROM "):
            names = ["Column", "Type", "Extra", "Comment"]
            self.rows = [(name, dtype, "", "") for name, dtype in self.target]
        elif statement.startswith("DESCRIBE OUTPUT ("):
            names = ["Column Name", "Catalog", "Schema", "Table", "Type", "Type Size", "Aliased"]
            self.rows = [(name, "", "", "", dtype, 0, True) for name, dtype in self.source]
        else:
            names, self.rows = [], []
        self.description = [(name,) for name in names]

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self):
        self.cur = Cursor()

    def cursor(self):
        return self.cur


@pytest.fixture
def writer():
    conn = Connection()
    session = TrinoSession(conn=conn)
    return session.createDataFrame([(1, "a")], ["id", "label"]).write, conn.cur


def writes(cursor):
    return [sql for sql in cursor.statements if sql.startswith(("INSERT", "CREATE", "DROP"))]


@pytest.mark.parametrize("method", ["insertInto", "saveAsTable"])
@pytest.mark.parametrize(
    "name", ["", " ", "a; DROP TABLE b", "a;", "a b", "a.*", "a.b.c.d", "a JOIN b", "a(", "`a"]
)
def test_invalid_table_names_never_reach_connection(writer, method, name):
    df_writer, cursor = writer
    with pytest.raises(UnsupportedOperationError, match="table"):
        getattr(df_writer, method)(name)
    assert cursor.statements == []


@pytest.mark.parametrize("method", ["insertInto", "saveAsTable"])
@pytest.mark.parametrize("mode", ["overwrite", "ignore", "invalid", ""])
def test_unsupported_modes_never_reach_connection(writer, method, mode):
    df_writer, cursor = writer
    with pytest.raises(UnsupportedOperationError, match="mode|overwrite"):
        getattr(df_writer.mode(mode), method)("memory.default.target")
    assert cursor.statements == []


@pytest.mark.parametrize("method", ["insertInto", "saveAsTable"])
@pytest.mark.parametrize("setting", ["format", "option"])
def test_unsupported_settings_never_reach_connection(writer, method, setting):
    df_writer, cursor = writer
    configured = df_writer.format("parquet") if setting == "format" else df_writer.option("x", None)
    with pytest.raises(UnsupportedOperationError, match="option|format"):
        getattr(configured, method)("target")
    assert cursor.statements == []


def test_missing_insert_target_does_not_create(writer):
    df_writer, cursor = writer
    cursor.exists = False
    with pytest.raises(UnsupportedOperationError, match="existing table"):
        df_writer.insertInto("target")
    assert writes(cursor) == []


@pytest.mark.parametrize("mode", [None, "error", "errorifexists"])
def test_existing_save_target_errors_without_writing(writer, mode):
    df_writer, cursor = writer
    with pytest.raises(FileExistsError, match="already exists"):
        df_writer.saveAsTable("target", mode=mode)
    assert writes(cursor) == []


@pytest.mark.parametrize("mode", [None, "append", "error", "errorifexists"])
def test_missing_save_target_uses_single_ctas(writer, mode):
    df_writer, cursor = writer
    cursor.exists = False
    df_writer.saveAsTable("memory.default.target", mode=mode)
    assert len(writes(cursor)) == 1
    assert writes(cursor)[0].startswith('CREATE TABLE "memory"."default"."target" AS SELECT ')
    assert any(sql.startswith("DESCRIBE OUTPUT (") for sql in cursor.statements)


def test_insert_maps_columns_by_position(writer):
    df_writer, cursor = writer
    cursor.source = [("other_id", "smallint"), ("other_label", "varchar(1)")]
    df_writer.insertInto("MEMORY.DEFAULT.TARGET", overwrite=False)
    assert writes(cursor)[0].startswith(
        'INSERT INTO "memory"."default"."target" ("id", "label") SELECT '
    )


@pytest.mark.parametrize("method", ["save", "by_name"])
def test_name_mapping_uses_source_order_and_target_names(writer, method):
    df_writer, cursor = writer
    cursor.source = [("LABEL", "varchar(1)"), ("ID", "integer")]
    if method == "save":
        df_writer.mode("overwrite").saveAsTable("target", mode="append")
    else:
        df_writer.byName.insertInto("target")
    assert writes(cursor)[0].startswith(
        'INSERT INTO "memory"."default"."target" ("label", "id") SELECT '
    )


@pytest.mark.parametrize(
    "source,match",
    [
        ([("id", "integer")], "count"),
        ([("id", "bigint"), ("label", "varchar(1)")], "type mismatch"),
        ([("id", "integer"), ("label", "varchar(11)")], "type mismatch"),
        ([("id", "integer"), ("label", "unknown")], "unknown"),
        ([("id", "integer"), ("ID", "integer")], "unique"),
        ([("", "integer"), ("label", "varchar(1)")], "nonempty"),
    ],
)
def test_invalid_source_does_not_insert(writer, source, match):
    df_writer, cursor = writer
    cursor.source = source
    with pytest.raises(UnsupportedOperationError, match=match):
        df_writer.insertInto("target")
    assert writes(cursor) == []


def test_name_mismatch_does_not_insert(writer):
    df_writer, cursor = writer
    cursor.source[0] = ("other", "integer")
    with pytest.raises(UnsupportedOperationError, match="column names"):
        df_writer.saveAsTable("target", mode="append")
    assert writes(cursor) == []


@pytest.mark.parametrize("failure", ["SELECT", "SHOW COLUMNS", "DESCRIBE OUTPUT"])
def test_metadata_failure_does_not_write(writer, failure):
    df_writer, cursor = writer
    cursor.fail_on = failure
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        df_writer.insertInto("target")
    assert writes(cursor) == []


def test_views_are_not_write_targets(writer):
    df_writer, cursor = writer
    cursor.table_type = "VIEW"
    with pytest.raises(UnsupportedOperationError, match="base table"):
        df_writer.insertInto("target")
    assert writes(cursor) == []


def test_writer_requires_connection():
    session = TrinoSession()
    with pytest.raises(ValueError, match="Trino table writes require a DB-API connection"):
        session.createDataFrame([(1,)], ["id"]).write.saveAsTable("target")


@pytest.mark.parametrize(
    "source,target,allowed",
    [
        ("bigint", "bigint", True),
        ("smallint", "bigint", True),
        ("bigint", "integer", False),
        ("integer", "double", False),
        ("varchar(2)", "varchar", True),
        ("varchar", "varchar(2)", False),
        ("varchar(2)", "varchar(3)", True),
        ("varchar(3)", "varchar(2)", False),
        ("decimal(10,2)", "decimal(10,2)", True),
        ("decimal(10,2)", "decimal(10,1)", False),
        ("timestamp(6)", "timestamp(3)", False),
        ("timestamp(3) with time zone", "timestamp(3)", False),
        ("array(integer)", "array(integer)", True),
        ("array(bigint)", "array(integer)", False),
    ],
)
def test_type_compatibility_at_insert_boundary(writer, source, target, allowed):
    df_writer, cursor = writer
    cursor.source = [("value", source)]
    cursor.target = [("value", target)]
    if allowed:
        df_writer.insertInto("target")
        assert len(writes(cursor)) == 1
    else:
        with pytest.raises(UnsupportedOperationError, match="type mismatch"):
            df_writer.insertInto("target")
        assert writes(cursor) == []


@pytest.mark.parametrize("method", ["insertInto", "saveAsTable"])
@pytest.mark.parametrize("dtype", ['row("unknown" integer)', 'row("a""unknown" integer)'])
def test_quoted_unknown_field_names_allow_writes(writer, method, dtype):
    df_writer, cursor = writer
    cursor.source = cursor.target = [("value", dtype)]
    cursor.exists = method == "insertInto"
    getattr(df_writer, method)("target")
    assert len(writes(cursor)) == 1


@pytest.mark.parametrize("metadata", ["source", "target"])
@pytest.mark.parametrize(
    "dtype", ["unknown", "array(unknown)", 'row("unknown" unknown)', 'row("a" array(UNKNOWN))']
)
def test_unknown_types_still_reject_writes(writer, metadata, dtype):
    df_writer, cursor = writer
    setattr(cursor, metadata, [("value", dtype)])
    with pytest.raises(UnsupportedOperationError, match="unknown column types"):
        df_writer.insertInto("target")
    assert writes(cursor) == []


@pytest.mark.parametrize("target", [[], [("id", "integer"), ("ID", "integer")]])
def test_invalid_target_metadata_does_not_write(writer, target):
    df_writer, cursor = writer
    cursor.target = target
    with pytest.raises(UnsupportedOperationError, match="unique"):
        df_writer.insertInto("target")
    assert writes(cursor) == []


@pytest.mark.parametrize(
    "source", [[], [("id", "unknown")], [("id", "integer"), ("ID", "integer")]]
)
def test_invalid_creation_source_does_not_create(writer, source):
    df_writer, cursor = writer
    cursor.exists = False
    cursor.source = source
    with pytest.raises(UnsupportedOperationError, match="unique|unknown"):
        df_writer.saveAsTable("target")
    assert writes(cursor) == []


@pytest.mark.parametrize("queries", [["SELECT 1", "SELECT 2"], ["DELETE FROM target"]])
def test_non_query_sources_do_not_write(writer, monkeypatch, queries):
    df_writer, cursor = writer
    cursor.exists = False
    monkeypatch.setattr(df_writer._df, "sql", lambda **kwargs: queries)
    with pytest.raises(UnsupportedOperationError, match="one SELECT"):
        df_writer.saveAsTable("target")
    assert writes(cursor) == []


@pytest.mark.parametrize("name", ["target", "default.target", "memory.default.target"])
def test_identifier_resolution_uses_fully_qualified_target(writer, name):
    df_writer, cursor = writer
    df_writer.insertInto(name)
    assert writes(cursor)[0].startswith('INSERT INTO "memory"."default"."target" ')
    assert ("SELECT current_catalog, current_schema" in cursor.statements) == (name.count(".") < 2)


def test_explicit_format_argument_is_rejected(writer):
    df_writer, cursor = writer
    with pytest.raises(UnsupportedOperationError, match="format"):
        df_writer.saveAsTable("target", format="parquet")
    assert cursor.statements == []


def test_overwrite_argument_is_rejected(writer):
    df_writer, cursor = writer
    with pytest.raises(UnsupportedOperationError, match="overwrite"):
        df_writer.insertInto("target", overwrite=True)
    assert cursor.statements == []


def test_duplicate_dataframe_columns_are_rejected_before_analysis(writer):
    df_writer, cursor = writer
    with pytest.raises(UnsupportedOperationError, match="unique"):
        df_writer._df.select("id", "id").write.insertInto("target")
    assert not any(sql.startswith("DESCRIBE OUTPUT") for sql in cursor.statements)
    assert writes(cursor) == []


@pytest.mark.parametrize("method", ["insertInto", "saveAsTable"])
@pytest.mark.parametrize(
    "name", ["target", "default.target", "memory.default.target", '"Memory"."Default"."Target"']
)
def test_snowflake_input_normalizes_metadata_and_insert_targets(method, name):
    conn = Connection()
    session = (
        TrinoSession.builder.config("sqlframe.conn", conn)
        .config("sqlframe.input.dialect", "snowflake")
        .getOrCreate()
    )
    writer = session.createDataFrame([(1, "a")], ["id", "label"]).write
    if method == "saveAsTable":
        writer.saveAsTable(name, mode="append")
    else:
        writer.insertInto(name)

    assert [sql for sql in conn.cur.statements if ".information_schema.tables" in sql] == [
        'SELECT table_type FROM "memory".information_schema.tables '
        "WHERE table_schema = 'default' AND table_name = 'target'"
    ]
    assert writes(conn.cur)[0].startswith('INSERT INTO "memory"."default"."target" ')
