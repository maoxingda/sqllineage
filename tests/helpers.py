import os.path
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    Column as SQLAlchemyColumn,
    Integer,
    MetaData,
    Table as SQLAlchemyTable,
    inspect,
    text,
)

from sqllineage import SQLPARSE_DIALECT
from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.core.metadata.sqlalchemy import SQLAlchemyMetaDataProvider
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Table
from sqllineage.runner import LineageRunner
from sqllineage.utils.entities import ColumnQualifierTuple

def _assert_table_lineage(lr: LineageRunner, source_tables=None, target_tables=None):
    for _type, actual, expected in zip(
        ["Source", "Target"],
        [lr.source_tables, lr.target_tables],
        [source_tables, target_tables],
    ):
        actual = set(actual)
        expected = (
            set()
            if expected is None
            else {Table(t) if isinstance(t, str) else t for t in expected}
        )
        assert (
            actual == expected
        ), f"\n\tExpected {_type} Table: {expected}\n\tActual {_type} Table: {actual}"


def _assert_column_lineage(lr: LineageRunner, column_lineages=None):
    expected_max_len = actual_max_len = 0
    expected = set()
    if column_lineages:
        for src, tgt in column_lineages:
            src_col: Column = Column(src.column)
            if src.qualifier is not None:
                src_col.parent = Table(src.qualifier)
            tgt_col: Column = Column(tgt.column)
            tgt_col.parent = Table(tgt.qualifier)
            expected.add((src_col, tgt_col))
            if len(str(tgt_col)) > expected_max_len:
                expected_max_len = len(str(tgt_col))
    actual = {(lineage[0], lineage[-1]) for lineage in set(lr.get_column_lineage())}

    for _, tgt in actual:
        if len(str(tgt)) > actual_max_len:
            actual_max_len = len(str(tgt))

    assert set(actual) == expected, (
        f"\n{' ' * 4}Expected Lineage:\n{' ' * 8}{(chr(10) + ' ' * 8).join(list(map(lambda t: str(t[1]).ljust(expected_max_len) + ' <- ' + str(t[0]), expected - actual)))}"
        f"\n{' ' * 4}Actual Lineage:\n{' ' * 8}{(chr(10) + ' ' * 8).join(list(map(lambda t: str(t[1]).ljust(actual_max_len) + ' <- ' + str(t[0]), actual - expected)))}"
    )


def assert_table_lineage_equal(
    sql: str,
    source_tables=None,
    target_tables=None,
    dialect: str = "ansi",
    test_sqlfluff: bool = True,
    test_sqlparse: bool = True,
):
    lr = LineageRunner(sql, dialect=SQLPARSE_DIALECT)
    lr_sqlfluff = LineageRunner(sql, dialect=dialect)
    if test_sqlparse:
        _assert_table_lineage(lr, source_tables, target_tables)
    if test_sqlfluff:
        _assert_table_lineage(lr_sqlfluff, source_tables, target_tables)


def assert_column_lineage_equal(
    sql: str,
    column_lineages=None,
    dialect: str = "ansi",
    metadata_provider: Optional[MetaDataProvider] = None,
    test_sqlfluff: bool = True,
    test_sqlparse: bool = True,
):
    metadata_provider = (
        DummyMetaDataProvider() if metadata_provider is None else metadata_provider
    )
    lr = LineageRunner(
        sql, dialect=SQLPARSE_DIALECT, metadata_provider=metadata_provider
    )
    lr_sqlfluff = LineageRunner(
        sql, dialect=dialect, metadata_provider=metadata_provider
    )
    if column_lineages is not None and all(
        isinstance(src_col, str)
        and isinstance(tgt_col, str)
        for src_col, tgt_col in column_lineages
    ):
        column_lineages_with_model = []
        for tgt_col, src_col in column_lineages:
            tgt_col_segs = tgt_col.split(".")
            src_col_segs = src_col.split(".")
            if len(tgt_col_segs) == 3 and len(src_col_segs) == 3:
                column_lineages_with_model.append(
                    (
                        ColumnQualifierTuple(src_col_segs[-1], '.'.join(src_col_segs[:2])),
                        ColumnQualifierTuple(tgt_col_segs[-1], '.'.join(tgt_col_segs[:2])),
                    )
                )
            elif len(tgt_col_segs) == 2 and len(src_col_segs) == 2:
                column_lineages_with_model.append(
                    (
                        ColumnQualifierTuple(src_col_segs[-1], src_col_segs[0]),
                        ColumnQualifierTuple(tgt_col_segs[-1], tgt_col_segs[0]),
                    )
                )
        column_lineages = column_lineages_with_model
    if test_sqlparse:
        _assert_column_lineage(lr, column_lineages)
    if test_sqlfluff:
        _assert_column_lineage(lr_sqlfluff, column_lineages)


def generate_metadata_providers(test_schemas):
    dummy_provider = DummyMetaDataProvider(test_schemas)

    sqlite3_sqlalchemy_provider = SQLAlchemyMetaDataProvider("sqlite:///:memory:")
    metadata = MetaData()
    for full_table_name, columns_names in test_schemas.items():
        schema, table = full_table_name.split(".")
        if schema not in ("main", "temp") and not inspect(
            sqlite3_sqlalchemy_provider.engine
        ).has_schema(schema):
            db_file_path = Path(os.path.dirname(__file__)).parent.joinpath(
                f"{schema}.db"
            )
            with sqlite3_sqlalchemy_provider.engine.connect() as conn:
                conn.execute(text(f"ATTACH DATABASE '{db_file_path}' AS '{schema}'"))
        SQLAlchemyTable(
            table,
            metadata,
            *[SQLAlchemyColumn(c, Integer) for c in columns_names],
            schema=schema,
        )
    metadata.create_all(bind=sqlite3_sqlalchemy_provider.engine)

    return [dummy_provider, sqlite3_sqlalchemy_provider]
