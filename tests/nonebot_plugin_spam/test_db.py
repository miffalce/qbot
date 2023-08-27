import datetime

import box
import os
import db as db


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
    model = db.TableModel(os.getenv("pg_conn", ""))
    model.init_engine()


class TestTableStmt:
    def test_create_user(func):
        db.QQGulidStmt().create("user_tb", T.model.metadata, T.model.engine)

    def test_create_user(func):
        db.QQGulidStmt().create("user_tb", T.model.metadata, T.model.engine).insert(
            box.Box(T.record)
        ).execute(T.model.session)

    def test_create_message_table(func):
        db.QQGulidStmt().create(
            f"message_tb_{box.Box(T.record).guild_id}",
            T.model.metadata,
            T.model.engine,
        )

    def test_insert_user(func):
        for mess in T.model.metadata.tables:
            if mess.startswith("user"):
                meta = T.model.metadata.tables[mess]
                db.QQGulidStmt(meta).insert(box.Box(T.record)).execute(T.model.session)

    def test_insert_message(func):
        for mess in T.model.metadata.tables:
            if mess.startswith("message"):
                meta = T.model.metadata.tables[mess]
                db.QQGulidStmt(meta).insert(box.Box(T.record)).execute(T.model.session)

    def test_select_where_message(func):
        for mess in T.model.metadata.tables:
            if mess.startswith("message"):
                meta = T.model.metadata.tables[mess]
                dq = db.QQGulidStmt(meta).select().where("spam", None)
                dq.execute(T.model.session).fetchall(["content"])

    def test_update_case_message_3(func):
        for mess in T.model.metadata.tables:
            if mess.startswith("message"):
                meta = T.model.metadata.tables[mess]
                dq = db.QQGulidStmt(meta).update_case(
                    "spam",
                    "mid",
                    [
                        [">=", 4, 0.3],
                        ["<", 6, 0.2],
                    ],
                )
                dq.execute(T.model.session)
                ans = db.QQGulidStmt(meta).select().execute(T.model.session).fetchall()
                assert ans[-1][-2]
