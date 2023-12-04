import logging
import warnings
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from sqllineage import DEFAULT_DIALECT, SQLPARSE_DIALECT
from sqllineage.config import SQLLineageConfig
from sqllineage.core.holders import SQLLineageHolder
from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Table
from sqllineage.core.parser.sqlfluff.analyzer import SqlFluffLineageAnalyzer
from sqllineage.core.parser.sqlparse.analyzer import SqlParseLineageAnalyzer
from sqllineage.drawing import draw_lineage_graph
from sqllineage.io import to_cytoscape
from sqllineage.utils.constant import LineageLevel
from sqllineage.utils.helpers import split, trim_comment

logger = logging.getLogger(__name__)


def lazy_method(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self._evaluated:
            self._eval()
        return func(*args, **kwargs)

    return wrapper


def lazy_property(func):
    return property(lazy_method(func))


class LineageRunner(object):
    def __init__(
        self,
        sql: str,
        dialect: str = DEFAULT_DIALECT,
        metadata_provider: MetaDataProvider = DummyMetaDataProvider(),
        encoding: Optional[str] = None,
        verbose: bool = False,
        draw_options: Optional[Dict[str, str]] = None,
    ):
        """
        The entry point of SQLLineage after command line options are parsed.

        :param sql: a string representation of SQL statements.
        :param dialect: sql dialect
        :param metadata_provider: metadata service object providing table schema
        :param encoding: the encoding for sql string
        :param verbose: verbose flag indicate whether statement-wise lineage result will be shown
        """
        if dialect == SQLPARSE_DIALECT:
            warnings.warn(
                "dialect `non-validating` is deprecated, use `ansi` or dialect of your SQL instead. "
                "`non-validating` will stop being the default dialect in v1.5.x release "
                "and be completely removed in v1.6.x",
                DeprecationWarning,
                stacklevel=2,
            )
        self._encoding = encoding
        self._sql = sql
        self._verbose = verbose
        self._draw_options = draw_options if draw_options else {}
        self._evaluated = False
        self._stmt: List[str] = []
        self._dialect = dialect
        self._metadata_provider = metadata_provider

    @lazy_method
    def __str__(self):
        """
        print out the Lineage Summary.
        """
        statements = self.statements()
        source_tables = "\n    ".join(str(t) for t in self.source_tables)
        target_tables = "\n    ".join(str(t) for t in self.target_tables)
        combined = f"""Statements(#): {len(statements)}
Source Tables:
    {source_tables}
Target Tables:
    {target_tables}
"""
        if self.intermediate_tables:
            intermediate_tables = "\n    ".join(
                str(t) for t in self.intermediate_tables
            )
            combined += f"""Intermediate Tables:
    {intermediate_tables}"""
        if self._verbose:
            result = ""
            for i, holder in enumerate(self._stmt_holders):
                stmt_short = statements[i].replace("\n", "")
                if len(stmt_short) > 50:
                    stmt_short = stmt_short[:50] + "..."
                content = str(holder).replace("\n", "\n    ")
                result += f"""Statement #{i + 1}: {stmt_short}
    {content}
"""
            combined = result + "==========\nSummary:\n" + combined
        return combined

    @lazy_method
    def to_cytoscape(self, level=LineageLevel.TABLE) -> List[Dict[str, Dict[str, str]]]:
        """
        to turn the DAG into cytoscape format.
        """
        if level == LineageLevel.COLUMN:
            import networkx as nx

            dw_clg = self._sql_holder.column_lineage_graph
            for n in dw_clg:
                if "dwd.main_transactions_stg.id" == str(n):  # TODO 通过前端传过来
                    return to_cytoscape(
                        dw_clg.subgraph(list(nx.ancestors(dw_clg, n)) + [n]),
                        compound=True,
                    )

            return to_cytoscape(self._sql_holder.column_lineage_graph, compound=True)
        else:
            return to_cytoscape(self._sql_holder.table_lineage_graph)

    def draw(self, dialect: str) -> None:
        """
        to draw the lineage directed graph
        """
        draw_options = self._draw_options
        if draw_options.get("f") is None:
            draw_options.pop("f", None)
            draw_options["e"] = self._sql
            draw_options["dialect"] = dialect
        return draw_lineage_graph(**draw_options)

    @lazy_method
    def statements(self) -> List[str]:
        """
        a list of SQL statements.
        """
        return [trim_comment(s) for s in self._stmt]

    @lazy_property
    def source_tables(self) -> List[Table]:
        """
        a list of source :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.source_tables, key=lambda x: str(x))

    @lazy_property
    def target_tables(self) -> List[Table]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.target_tables, key=lambda x: str(x))

    @lazy_property
    def intermediate_tables(self) -> List[Table]:
        """
        a list of intermediate :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.intermediate_tables, key=lambda x: str(x))

    @lazy_method
    def get_column_lineage(self, exclude_subquery=True) -> List[Tuple[Column, Column]]:
        """
        a list of column tuple :class:`sqllineage.models.Column`
        """
        # sort by target column, and then source column
        return sorted(
            self._sql_holder.get_column_lineage(exclude_subquery),
            key=lambda x: (str(x[-1]), str(x[0])),
        )

    def print_column_lineage(self) -> None:
        """
        print column level lineage to stdout
        """
        for path in self.get_column_lineage():
            print(" <- ".join(str(col) for col in reversed(path)))

    def print_table_lineage(self) -> None:
        """
        print table level lineage to stdout
        """
        print(str(self))

    def _eval(self):
        analyzer = (
            SqlParseLineageAnalyzer()
            if self._dialect == SQLPARSE_DIALECT
            else SqlFluffLineageAnalyzer(self._dialect)
        )
        if SQLLineageConfig.TSQL_NO_SEMICOLON and self._dialect == "tsql":
            self._stmt = analyzer.split_tsql(self._sql.strip())
        else:
            if SQLLineageConfig.TSQL_NO_SEMICOLON and self._dialect != "tsql":
                warnings.warn(
                    f"Dialect={self._dialect}, TSQL_NO_SEMICOLON will be ignored unless dialect is tsql"
                )
            self._stmt = split(self._sql.strip())

        self._stmt_holders = [analyzer.analyze(stmt) for stmt in self._stmt]
        self._sql_holder = SQLLineageHolder.of(
            self._metadata_provider, *self._stmt_holders
        )
        self._evaluated = True

    @staticmethod
    def supported_dialects() -> Dict[str, List[str]]:
        """
        an ordered dict (so we can make sure the default parser implementation comes first)
        with kv as parser_name: dialect list
        """
        dialects = OrderedDict(
            [
                (
                    SqlParseLineageAnalyzer.PARSER_NAME,
                    SqlParseLineageAnalyzer.SUPPORTED_DIALECTS,
                ),
                (
                    SqlFluffLineageAnalyzer.PARSER_NAME,
                    SqlFluffLineageAnalyzer.SUPPORTED_DIALECTS,
                ),
            ]
        )
        return dialects


def draw_graph(*graphs):
    import random
    import networkx as nx
    from matplotlib import pyplot as plt
    from matplotlib.font_manager import FontProperties

    def generate_hex_color():
        # 生成一个随机的RGB颜色值
        rgb_color = (
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
        )

        # 将RGB转换为十六进制颜色值
        hex_color = "#{:02x}{:02x}{:02x}".format(
            rgb_color[0], rgb_color[1], rgb_color[2]
        )

        return hex_color

    edge_type_colors = {
        "lineage": generate_hex_color(),
        "has_column": generate_hex_color(),
        "has_alias": generate_hex_color(),
        "deafult": generate_hex_color(),
    }

    edge_type_colors["lineage"] = "red"
    edge_type_colors["has_column"] = "blue"
    edge_type_colors["has_alias"] = "green"

    title_color = generate_hex_color()
    font_properties = FontProperties(
        fname="/Users/maoxd/open-source/scp_zh/fonts/SimHei.ttf"
    )

    if len(graphs) > 1:
        _, axes = plt.subplots(nrows=len(graphs), ncols=1, figsize=(100, 8))
        for i, graph in enumerate(graphs):
            edge_colors = [
                edge_type_colors[attr.get("type", "deafult")]
                for _, _, attr in graph.edges(data=True)
            ]

            pos = nx.drawing.nx_pydot.pydot_layout(graph, prog="dot")

            title = graph.title if hasattr(graph, "title") else ""
            axes[i].set_title(
                title, color=title_color, fontproperties=font_properties, fontsize=20
            )
            nx.draw_networkx_labels(graph, pos, ax=axes[i])
            nx.draw_networkx_edges(
                graph, pos, edge_color=edge_colors, arrows=True, ax=axes[i]
            )
    else:
        graph = graphs[0]
        plt.figure(figsize=(100, 10))
        edge_colors = [
            edge_type_colors[attr.get("type", "deafult")]
            for _, _, attr in graph.edges(data=True)
        ]
        edge_labels = {(u, v): attr["type"] for u, v, attr in graph.edges(data=True)}

        pos = nx.drawing.nx_pydot.pydot_layout(graph, prog="dot")

        title = graph.title if hasattr(graph, "title") else ""
        plt.title(title, color=title_color, fontproperties=font_properties, fontsize=20)
        nx.draw_networkx_labels(graph, pos)
        nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels)
        nx.draw_networkx_edges(graph, pos, edge_color=edge_colors, arrows=True)

    plt.show()
