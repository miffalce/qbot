# 查询消息

import datetime
import operator

from sqlalchemy import (
    ARRAY,
    FLOAT,
    INTEGER,
    TIMESTAMP,
    VARCHAR,
    Boolean,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    case,
    create_engine,
    inspect,
    not_,
    or_,
    null,
    select,
    Subquery,
    delete,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class Operators(object):
    cmp_operators = {
        "=": operator.eq,
        "==": operator.eq,
        "equal": operator.eq,
        "<": operator.lt,
        "lt": operator.lt,
        "<=": operator.le,
        "le": operator.le,
        ">": operator.gt,
        "gt": operator.gt,
        ">=": operator.ge,
        "ge": operator.ge,
        "!=": operator.ne,
        "<>": operator.ne,
        "ne": operator.ne,
    }


class MeteTypeDef(object):
    def __init__(self) -> None:
        self.user_tb: list = [
            Column("mid", Integer, primary_key=True),
            Column("author_id", String, index=True),
            Column("author_username", String),
            Column("author_bot", Boolean),
            Column("author_avatar", String),
            Column("channel_id", String, index=True),
            Column("guild_id", String, index=True),
            Column("member_nick", String),
            Column("member_roles", ARRAY(Integer)),
            Column("member_joined_at", TIMESTAMP),
            Column("spam", FLOAT),
            Column("color", Integer),
        ]
        self.message_tb: list = [
            Column("mid", Integer, primary_key=True),
            Column("id", String, index=True),
            Column("channel_id", String, index=True),
            Column("author_id", String, index=True),
            Column("content", String),
            Column("timestamp", TIMESTAMP),
            Column("spam", FLOAT),
            Column("color", Integer),
        ]

    @property
    def dicts(self):
        return vars(self)


class DBResultParser(object):
    def __init__(self, raw, qq_guild) -> None:
        self.raw = raw
        self.ans = []
        self.qq_guild = qq_guild

    def fetchall(self):
        self.ans = self.raw.fetchall()
        return self

    def fetchone(self):
        self.ans = self.raw.fetchone()
        return self

    def filter_by(self, filter_=None):
        if filter_ is None:
            filter_ = self.qq_guild.no_filter_names
        idx_ = [self.qq_guild.no_filter_names.index(i) for i in filter_]
        if len(idx_) == 1:
            ans = [rol[idx_[0]] for rol in self.ans]
        else:
            ans = [[rol[i] for i in idx_] for rol in self.ans]
        return ans


class TableModel(object):
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        self.init_engine()

    def init_engine(self):
        self.engine = create_engine(self.pg_conn)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.session = sessionmaker(bind=self.engine)()


class TableStmt(object):
    def __init__(self, table_meta: MetaData = None):
        # sourcery skip: default-mutable-arg
        self.table_meta = table_meta
        self.stmt = self.table_meta

    @property
    def ignore_col(self):
        return []

    @property
    def ignore_type_col(self):
        return [datetime.datetime]

    @property
    def typedef(self):
        return MeteTypeDef().dicts

    def create(self, tb_name, shema_meta, engine):
        """Create a table in the database with the provided schema metadata."""
        try:
            if tb_name not in shema_meta.tables.keys():
                data_type = [
                    v for k, v in self.typedef.items() if tb_name.startswith(k)
                ][0]
                table_meta = Table(tb_name, shema_meta, *data_type)
                table_meta.create(engine)
            self.table_meta = shema_meta.tables[tb_name]
            self.stmt = self.table_meta
            return self
        except Exception as err:
            print(f"[ ERROR ] {err}")
            if tb_name in shema_meta.tables.keys():
                self.table_meta.tables.pop(tb_name)

    def delete(self):
        self.stmt = self.table_meta.delete()
        return self

    def insert(self, **kwargs):
        """
        Create an insert statement.

        Args:
            kwargs: The column name and value pairs to insert.

        Returns:
            The insert statement.
        """
        kwargs = {
            k: self.types[k](v)
            for k, v in kwargs.items()
            if k in self.names and v != "NULL"
        }
        self.stmt = self.table_meta.insert().values(**kwargs)
        return self

    def case(self, update_col="", judge_col="", case_items=[]):
        """
        Create a case expression.

        Args:
            update_col (str): The column to update.
            judge_col (str): The column to compare.
            case_items (list): A list of case items, each of which is a tuple of (operator, value, result).

        Returns:
            The case expression.
        """
        conds = []
        for stmt_ in case_items:
            op = stmt_[0]
            if op not in Operators.cmp_operators:
                op = "="
            conds.append(
                (
                    Operators.cmp_operators[op](
                        getattr(self.table_meta.c, judge_col), stmt_[-2]
                    ),
                    stmt_[-1],
                )
            )

        return case(*conds, else_=getattr(self.table_meta.c, update_col))

    def update_case(self, update_col="", judge_col="", case_items=[], **kwargs):
        """
        Update the value of a column based on a case expression.

        Args:
            update_col (str): The column to update.
            judge_col (str): The column to compare.
            case_items (list): A list of case items, each of which is a tuple of (operator, value, result).
            kwargs: Keyword arguments to be passed to the `update()` function.

        Returns:
            The updated statement.
        """

        if kwargs:
            self.stmt = self.stmt.update().values(kwargs)
            return self

        self.stmt = self.stmt.update().values(
            {update_col: self.case(update_col, judge_col, case_items)}
        )
        return self

    def select(self):
        self.stmt = self.stmt.select()
        return self

    def where(self, col, value, op="="):
        if self.exist_stmt:
            bool_ = Operators.cmp_operators[op](getattr(self.table_meta.c, col), value)
            if value is not None and Operators.cmp_operators[op] == operator.ne:
                bool_ = or_(bool_, getattr(self.table_meta.c, col) == None)
            self.stmt = self.stmt.where(bool_)
        return self

    def limit(self, num: int):
        self.stmt = self.stmt.limit(num)
        return self

    def subquery(self, *col):
        col = [getattr(self.table_meta.c, c) for c in col]
        self.stmt = select(*col)
        return self

    def where_in_(self, col, stmt):
        col = getattr(self.table_meta.c, col)
        self.stmt = self.stmt.where(col.in_(stmt))
        return self

    def in_(self, stmt: int):
        self.stmt = self.stmt._in(stmt)
        return self

    def execute(self, session):
        """
        Execute the statement.

            Args:
                session: The database session.

            Returns:
                The `TableStmt` instance.
        """
        if self.exist_stmt:
            ans = session.execute(self.stmt)
            session.commit()
        return DBResultParser(ans, self)

    @property
    def exist_stmt(self):
        return str(self.stmt) != "None" or not str(self.stmt)

    @property
    def names(self):
        return [
            col.name
            for col in self.table_meta.columns
            if col.name not in self.ignore_col
        ]

    @property
    def no_filter_names(self):
        return [col.name for col in self.table_meta.columns]

    @property
    def types(self):
        return {
            k: v
            for k, v in zip(
                self.names,
                [
                    col.type.python_type
                    if col.type.python_type not in self.ignore_type_col
                    else lambda x: x
                    for col in self.table_meta.columns
                    if col.name not in self.ignore_col
                ],
            )
        }


class Reflect(object):
    def __init__(self, key, record) -> None:
        self.key = key
        self.record = record

    def dot_split_name(self, name):
        res = [name]

        def warpper(name, idx=1):
            if len(name) == idx:
                return ""
            if name[idx] == "_":
                for i in ["_", "."]:
                    name = str(name[:idx]) + i + str(name[idx + 1 :])
                    res.append(name)
            warpper(name, idx + 1)

        warpper(name)
        return list(sorted(set(res), key=res.index))

    @property
    def col_value(self):
        for value in self.dot_split_name(self.key):
            try:
                return operator.attrgetter(value)(self.record)
            except:
                pass
        return "NULL"


class QQGulidStmt(TableStmt):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @property
    def ignore_col(self):
        return ["mid"]

    def insert(self, record):
        kwargs = {col: Reflect(col, record).col_value for col in self.names}
        return super().insert(**kwargs)
