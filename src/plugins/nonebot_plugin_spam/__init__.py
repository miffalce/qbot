from nonebot import get_driver, on_message
from nonebot.internal.adapter import Event as Event
from nonebot.plugin import PluginMetadata
from nonebot_plugin_apscheduler import scheduler

from .common import error_singal, ScoreRequest
from .config import Config
from .db import QQGulidStmt, TableModel

__plugin_meta__ = PluginMetadata(
    name="guild_spam",
    description="a spam bot on qq guild channel",
    usage="*",
    type="application",
    config=Config,
    extra={},
)

import nonebot

global_config = get_driver().config
guild_config = Config.parse_obj(global_config)
T = TableModel(guild_config.pg_conn)

guild_message_handler = on_message(priority=11)


async def store_handler_(event):
    QQGulidStmt().create(f"message_tb_{event.guild_id}", T.metadata, T.engine).insert(
        event
    ).execute(T.session)
    QQGulidStmt().create("user_tb", T.metadata, T.engine).insert(event).execute(
        T.session
    )


@guild_message_handler.handle()
async def store_handler(event: Event) -> None:
    await store_handler_(event)


@scheduler.scheduled_job("cron", second="*/1", id="update_score")
async def update_score() -> None:
    for name, table_meta in T.metadata.tables.items():
        if name.startswith("message_tb"):
            QQGulidStmt(table_meta).delete().where("content", None).execute(T.session)
            data = (
                QQGulidStmt(table_meta)
                .select()
                .where("spam", None)
                .limit(5)
                .execute(T.session)
                .fetchall()
                .filter_by(["content"])
            )
            print(data)
            if data:
                sr = ScoreRequest(data)
                sr.init_request(guild_config.bert_url)
                dq = QQGulidStmt(table_meta).update_case("spam", "content", sr.list)
                dq.execute(T.session)


@scheduler.scheduled_job("cron", second="*/1", id="event_message")
async def recall_message() -> None:
    bot = nonebot.get_bot()
    user_meta = T.metadata.tables["user_tb"]
    for name, table_meta in T.metadata.tables.items():
        if name.startswith("message_tb"):
            sub_query_stmt = (
                QQGulidStmt(user_meta)
                .subquery("author_id")
                .where("member_roles", [4, 19], op="!=")
                .stmt
            )
            data = (
                QQGulidStmt(table_meta)
                .select()
                .where_in_("author_id", sub_query_stmt)
                .where("color", 1, "!=")
                .where("spam", 0, "<")
                .execute(T.session)
                .fetchall()
                .filter_by(["channel_id", "id"])
            )
            print(data)
            for ent in data:
                try:
                    await bot.delete_message(channel_id=ent[0], message_id=ent[1])
                except Exception as err:
                    print(f"ERROR: {err}")
                finally:
                    QQGulidStmt(table_meta).update_case(color=1).where(
                        "id", ent[1]
                    ).execute(T.session)
