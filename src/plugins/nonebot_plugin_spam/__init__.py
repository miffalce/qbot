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
                .select("content", "spam")
                .where("spam", None)
                .limit(5)
                .execute(T.session)
                .fetchall()
            )
            data = [i[0] for i in data if i[0]]
            print(data)
            if data:
                sr = ScoreRequest(data)
                sr.init_request(guild_config.bert_url)
                sr.cacluate()
                dq = QQGulidStmt(table_meta).update_case("spam", "content", sr.list)
                dq.execute(T.session)


@scheduler.scheduled_job("cron", second="*/5", id="event_message")
async def recall_message() -> None:
    bot = nonebot.get_bot()
    pass
    # guild = QQGulidApi(bot.bot_info.id, bot.bot_info.token)
    # user_meta = t_mod.metadata.tables["user_tb"]
    # u_stmt = select(user_meta.c.author_id).where(user_meta.c.member_roles == [5, 14])
    # for t_name, t_meta in t_mod.metadata.tables.items():
    #     if t_name.startswith("message_tb"):
    #         stmt = select(t_meta.c.channel_id, t_meta.c.id).where(
    #             t_meta.c.author_id.in_(u_stmt)
    #         )
    #         datas = t_mod.session.execute(stmt).fetchall()
    #         logger.debug(datas)
    #         for i in datas:
    #             await bot.delete_message(channel_id=i[0], message_id=i[1])
