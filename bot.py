import nonebot
from nonebot.adapters.qqguild import Adapter as GuildAdaptor


# 初始化 NoneBot
nonebot.init()
nonebot.load_from_toml("pyproject.toml")


# 注册适配器
driver = nonebot.get_driver()
driver.register_adapter(GuildAdaptor)

guild_bot = nonebot.get_adapter(GuildAdaptor).bots


if __name__ == "__main__":
    nonebot.run()
