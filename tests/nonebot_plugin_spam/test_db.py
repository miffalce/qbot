import datetime

import box
import os
import spam_db as spam_db


class T:
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
    model = spam_db.TableModel(os.getenv("pg_conn", ""))
    model.init_engine()


class TestTableStmt:
    def test_create_table(func):
        spam_db.QQGulidStmt().create(
            "user_tb_test", T.model.metadata, T.model.engine
        ).insert(box.Box(T.record)).execute(T.model.session)
        spam_db.QQGulidStmt().create(
            "message_tb_test", T.model.metadata, T.model.engine
        ).insert(box.Box(T.record)).execute(T.model.session)

    def test_select_where_double_list_message(func):
        meta = T.model.metadata.tables["message_tb_test"]
        data = (
            spam_db.QQGulidStmt(meta)
            .select()
            .where([["spam", None]])
            .execute(T.model.session)
            .fetchall()
            .filter_by(["content"])
        )
        assert data == ["asdgklasdgklsadg"]

    def test_select_where_one_list_message(func):
        meta = T.model.metadata.tables["message_tb_test"]
        data = (
            spam_db.QQGulidStmt(meta)
            .select()
            .where(["spam", None])
            .execute(T.model.session)
            .fetchall()
            .filter_by(["content"])
        )
        assert data == ["asdgklasdgklsadg"]

    def test_select_where_kwargs_message(func):
        meta = T.model.metadata.tables["message_tb_test"]
        data = (
            spam_db.QQGulidStmt(meta)
            .select()
            .where(spam=None)
            .execute(T.model.session)
            .fetchall()
            .filter_by(["content"])
        )
        assert data == ["asdgklasdgklsadg"]

    def test_update_case_message_3_para(func):
        meta = T.model.metadata.tables["message_tb_test"]
        dq = spam_db.QQGulidStmt(meta).update_case(
            "spam",
            "mid",
            [
                [">=", 4, 0.3],
                ["<", 6, 0.2],
            ],
        )
        dq.execute(T.model.session)
        ans = (
            spam_db.QQGulidStmt(meta)
            .select()
            .execute(T.model.session)
            .fetchall()
            .filter_by(["spam"])
        )
        assert ans == [0.2]

    def test_sub_query(self):
        user_meta = T.model.metadata.tables["user_tb_test"]
        meta = T.model.metadata.tables["message_tb_test"]
        sub_query_stmt = (
            spam_db.QQGulidStmt(user_meta)
            .subquery("author_id")
            .where(member_roles=[4, 18])
            .stmt
        )
        data = (
            spam_db.QQGulidStmt(meta)
            .select()
            .where(
                [
                    ["author_id", "in", sub_query_stmt],
                    ["color", "!=", 1],
                ]
            )
            .execute(T.model.session)
            .fetchall()
            .filter_by(["channel_id", "id", "color"])
        )
        assert data == [
            ["111111111", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", None]
        ]

    def test_unqiue(self):
        T.record["content"] = "ppp"
        meta = T.model.metadata.tables["message_tb_test"]
        spam_db.QQGulidStmt(meta).insert(box.Box(T.record)).execute(T.model.session)
        data = (
            spam_db.QQGulidStmt(meta)
            .select()
            .execute(T.model.session)
            .fetchall()
            .filter_by(["content"])
        )

        assert data == ["ppp"]

    def test_clean_db(self):
        meta = T.model.metadata.tables["user_tb_test"]
        spam_db.QQGulidStmt(meta).drop(T.model.engine).execute(T.model.session)
        meta = T.model.metadata.tables["message_tb_test"]
        spam_db.QQGulidStmt(meta).drop(T.model.engine).execute(T.model.session)


class TestGobalConfig:
    def test_global_insert(self):
        spam_db.QQGulidStmt().create(
            "guild_config_test", T.model.metadata, T.model.engine
        ).insert(
            box.Box(
                value={"max_spam_value": 0.3, "ppp": "998", "xx": ["tttt"]},
                guild_id="10000000",
            )
        ).execute(
            T.model.session
        )

    def test_guild_insert(self):
        spam_db.QQGulidStmt().create(
            "guild_config_test", T.model.metadata, T.model.engine
        ).insert(
            box.Box(
                value={"max_spam_value": 0.4, "xx": ["tttt"]},
                guild_id=T.record["guild_id"],
            )
        ).execute(
            T.model.session
        )

    def test_2th_guild_insert(self):
        spam_db.QQGulidStmt().create(
            "guild_config_test", T.model.metadata, T.model.engine
        ).insert(
            box.Box(
                value={"max_spam_value": 0.3},
                guild_id=T.record["guild_id"],
            )
        ).execute(
            T.model.session
        )

    def test_select(self):
        meta = T.model.metadata.tables["guild_config_test"]
        data = (
            spam_db.QQGulidStmt(meta)
            .select()
            .where(["guild_id", "in", ["10000000", str(T.record["guild_id"])]])
            .execute(T.model.session)
            .fetchall()
            .filter_by(["value"])
        )
        print(data)

    def test_clean(self):
        meta = T.model.metadata.tables["guild_config_test"]
        spam_db.QQGulidStmt(meta).drop(T.model.engine).execute(T.model.session)
