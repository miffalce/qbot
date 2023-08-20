# 查询消息

import datetime
import operator

from nonebot.log import logger
import box
from pydantic import BaseModel
from sqlalchemy import (
    ARRAY,
    select,
    VARCHAR,
    FLOAT,
    INTEGER,
    TIMESTAMP,
    Boolean,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    create_engine,
    inspect,
    null,
    not_,
    case,
    select,
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


class Struct:
    pass


class GuildTableModel(BaseModel):
    user_tb: list = [
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

    message_tb: list = [
        Column("mid", Integer, primary_key=True),
        Column("id", String, index=True),
        Column("channel_id", String, index=True),
        Column("author_id", String, index=True),
        Column("content", String),
        Column("timestamp", TIMESTAMP),
        Column("spam", FLOAT),
        Column("color", Integer),
    ]


class GuildTableAdaptor(object):
    def __init__(self, metadata, engine) -> None:
        self.metadata = metadata
        self.engine = engine

    def get_col_names(self, tb_name):
        # res = []
        # for col in inspect(self.engine).get_columns(tb_name):
        #     if col not in Constance.IGNORE_COLUMNS:
        #         res.append(col["name"])
        # return res
        res = []
        for key, value in vars(GuildTableModel()).items():
            if tb_name.startswith(key):
                for v in value:
                    if v.name not in Constance.IGNORE_COLUMNS:
                        res.append(v.name)
        return res

    def get_col_type(self, tb_name, col_name):
        convertor = {Integer: int, String: str, FLOAT: float}
        for tb, cols in vars(GuildTableModel()).items():
            if not tb_name.startswith(tb):
                continue
            for col in cols:
                if col.name != col_name:
                    continue
                return convertor.get(type(col.type), lambda x: x)

        return lambda x: x

    def get_col_def(self, tb_name: str) -> list:
        for key, value in vars(GuildTableModel()).items():
            if tb_name.startswith(key):
                return value
        return []

    def get_rule_set(self, col_name):
        res = [col_name]

        def inner_(col_name, index=1):
            if len(col_name) == index:
                return ""
            if col_name[index] == "_":
                for i in ["_", "."]:
                    col_name = str(col_name[:index]) + i + str(col_name[index + 1 :])
                    res.append(col_name)
            inner_(col_name, index + 1)

        inner_(col_name)
        return list(sorted(set(res), key=res.index))

    def rule_(self, tb_name, col_name, record):
        attr_type = self.get_col_type(tb_name, col_name)
        for match in self.get_rule_set(col_name):
            try:
                operator.attrgetter(match)(record)
                return attr_type(operator.attrgetter(match)(record))
            except Exception as ste:
                logger.error(ste)

    def insert_values(self, tb_name: list, record: Struct):
        insert = {}
        for col in self.get_col_names(tb_name):
            rv = self.rule_(tb_name, col, record)
            if rv:
                insert[col] = rv
        return insert

    def select_where_(self, tb_name: str, record: Struct):
        where_ = []
        for col in self.get_col_names(tb_name):
            table_meta = self.metadata.tables[tb_name]
            attr_name = getattr(table_meta.c, col)
            rv = self.rule_(tb_name, col, record)
            if rv:
                where_.append(attr_name == rv)
        return and_(*where_)

    def select_(self, tb_name: str, record: Struct):
        return (
            self.metadata.tables[tb_name]
            .select()
            .where(self.select_where_(tb_name, record))
        )


class Constance(object):
    IGNORE_COLUMNS: list = ["mid"]


class TableModel(object):
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        self.init_engine()

    def init_engine(self):
        self.engine = create_engine(self.pg_conn)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.session = sessionmaker(bind=self.engine)()
        self.controller = GuildTableAdaptor(self.metadata, self.engine)

    def where_filter_by_col(self, tb_name, record):
        return self.controller.where_and_driver(
            self.metadata, self.get_cols_name(tb_name), tb_name, record
        )

    def create_table(self, tb_name):
        try:
            if tb_name not in self.metadata.tables.keys():
                table_meta = Table(
                    tb_name, self.metadata, *self.controller.get_col_def(tb_name)
                )
                table_meta.create(self.engine)
        except Exception as ste:
            if tb_name in self.metadata.tables.keys():
                self.metadata.tables.pop(tb_name)

    def insert_table(self, tb_name, record, igore_dup=True):
        self.create_table(tb_name)
        table_meta = self.metadata.tables[tb_name]
        query_sql = table_meta.select().where(
            self.controller.select_where_(tb_name, record)
        )
        if igore_dup or not self.session.execute(query_sql).fetchone():
            insert_sql = table_meta.insert().values(
                self.controller.insert_values(tb_name, record)
            )
            self.session.execute(insert_sql)
            self.session.commit()


class TableStmt(object):
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

    def __init__(self, meta: MetaData):
        self.meta = meta
        self.stmt = self.meta
        self.ignore_col = []
        self.ignore_type_col = [datetime.datetime]

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
        self.stmt = self.meta.insert().values(**kwargs)
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
        for op, r_, v in case_items:
            conds.append(
                (TableStmt.cmp_operators[op](getattr(self.meta.c, judge_col), r_), v)
            )

        return case(*conds, else_=getattr(self.meta.c, update_col))

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

    def select(self, *cols):
        if cols:
            cols = [operator.attrgetter(f"c.{col}")(self.meta) for col in cols]
            self.stmt = select(*cols)
        self.stmt = self.stmt.select()
        return self

    @property
    def names(self):
        return [
            col.name for col in self.meta.columns if col.name not in self.ignore_col
        ]

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
                    for col in self.meta.columns
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
        self.ignore_col = ["mid"]

    def insert(self, record):
        kwargs = {col: Reflect(col, record).col_value for col in self.names}
        return super().insert(**kwargs)


def send_post(text):
    import json

    import requests

    url = "url"
    headers = {"Content-Type": "application/json"}

    data = {"data": {"text": text}}
    r = requests.post(url=url, headers=headers, data=json.dumps(data))
    result_json = json.loads(r.text)


if __name__ == "__main__":

    class TestConst:
        record = {
            "id": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "channel_id": 111111111,
            "guild_id": 33333333333333333333,
            "content": "asdgklasdgklsadg",
            "timestamp": "2023-08-06T20:16:05+08:00",
            "edited_timestamp": None,
            "mention_everyone": None,
            "author": {
                "id": 11111111111111111111,
                "username": "username",
                "avatar": "https://qqchannel-profile/",
                "bot": False,
                "union_openid": None,
                "union_user_account": None,
            },
            "attachments": None,
            "embeds": None,
            "mentions": None,
            "member": {
                "user": None,
                "nick": "username",
                "roles": [4, 18],
                "joined_at": datetime.datetime(2023, 6, 16, 23, 58, 31),
                # "joined_at": "2023-06-16T23:58:31+08:00",
            },
            "ark": None,
            "seq": 1498,
            "seq_in_channel": "1498",
            "message_reference": None,
            "src_guild_id": None,
            "to_me": False,
            "reply": None,
            "_message": [{"type": "text", "data": {"text": "asdgklasdgklsadg"}}],
        }

    tb_mod = TableModel("postgresql://user:pass..@ip:port/db_name")
    tb_mod.init_engine()
    tb_mod.create_table("user_tb")
    tb_mod.create_table("message_tb_123")
    user_meta = tb_mod.metadata.tables["user_tb"]
    stmt = (
        TableStmt(user_meta)
        .update_case(
            "spam",
            "mid",
            [
                [">=", 4, 0.3],
                ["<", 6, 0.2],
            ],
        )
        .stmt
    )
    stmt = QQGulidStmt(user_meta).insert(box.Box(TestConst.record)).stmt
    tb_mod.session.execute(stmt)
    tb_mod.session.commit()