import functools
import json
import signal

import requests
from nonebot.internal.adapter import Event as Event
from nonebot.log import logger


def error_singal(timeout=10, ignore_err=True):
    """
    超时处理
    :param timeout: 超时时间
    :param ignore_err: 是否抛出异常
    :return:
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kw: Event):
            try:
                signal.alarm(timeout)
                rt = await func(*args, **kw)
                signal.alarm(0)
                return rt
            except Exception as err:
                logger.error(err)
                if not ignore_err:
                    raise Exception(err)

        return wrapper

    return decorator


class ScoreRequest(object):
    def __init__(self, text: list) -> None:
        self.text = text
        self.encode = {}
        self.result = {}

    def init_request(self, url):
        data = {"data": {"text": self.text}}
        headers = {"Content-Type": "application/json"}
        ans = requests.post(url=url, headers=headers, data=json.dumps(data))
        self.encode = json.loads(ans.text)

    def cacluate(self):
        for block in self.encode["result"]:
            score = block["predictions"][0]["score"] * (
                1 if block["predictions"][0]["label"] == "normal" else -1
            )
            self.result[block["text"]] = score

    @property
    def dicts(self):
        return self.result

    @property
    def list(self):
        return [[k, v] for k, v in self.dicts.items()]
