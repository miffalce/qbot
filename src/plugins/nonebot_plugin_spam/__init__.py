from nonebot import get_driver, on_message
from nonebot_plugin_apscheduler import scheduler

from nonebot.internal.adapter import Event as Event

# from botpy.types.message import Message
from nonebot.plugin import PluginMetadata
from nonebot.log import logger

from .config import Config
from .db import TableModel
from .common import error_singal, get_classifer_result
from sqlalchemy import null, case, and_, select

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

t_mod = TableModel(guild_config.pg_conn)

guild_message_handler = on_message(priority=11)


# @error_singal(timeout=3)
@error_singal(timeout=3)
async def store_handler_(event):
    t_mod.insert_table(f"message_tb_{event.guild_id}", event, True)
    t_mod.insert_table("user_tb", event, False)


@guild_message_handler.handle()
async def store_handler(event: Event) -> None:
    await store_handler_(event)


@scheduler.scheduled_job("cron", second="*", id="check_context")
# @error_singal(timeout=3)
async def check_context() -> None:
    return
    for t_name, t_meta in t_mod.metadata.tables.items():
        if t_name.startswith("message_tb"):
            stmt = t_meta.select().where(t_meta.c.spam == None).limit(5)
            datas = t_mod.session.execute(stmt).fetchall()
            contents = [i[4] for i in datas]
            if not contents:
                continue
            logger.debug(contents)
            r_post = get_classifer_result(contents)
            logger.debug(r_post)
            conds = [
                (t_meta.c.content == text, score) for text, score in r_post.items()
            ]
            update_stmt = (
                t_meta.update()
                .values(spam=case(conds))
                .where(t_meta.c.content.in_(contents))
            )
            t_mod.session.execute(update_stmt)
            t_mod.session.commit()


@scheduler.scheduled_job("cron", second="*/5", id="event_message")
async def recall_message() -> None:
    bot = nonebot.get_bot()
    guild = QQGulidApi(bot.bot_info.id, bot.bot_info.token)
    user_meta = t_mod.metadata.tables["user_tb"]
    u_stmt = select(user_meta.c.author_id).where(user_meta.c.member_roles == [5, 14])
    for t_name, t_meta in t_mod.metadata.tables.items():
        if t_name.startswith("message_tb"):
            stmt = select(t_meta.c.channel_id, t_meta.c.id).where(
                t_meta.c.author_id.in_(u_stmt)
            )
            datas = t_mod.session.execute(stmt).fetchall()
            logger.debug(datas)
            for i in datas:
                await bot.delete_message(channel_id=i[0], message_id=i[1])
