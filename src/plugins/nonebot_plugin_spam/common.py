import functools
import json
import signal

import requests
from box import Box
from nonebot.internal.adapter import Event as Event
from nonebot.log import logger


def error_singal(timeout=10, ignore_err=True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw: Event):
            try:
                signal.alarm(timeout)
                rt = func(*args, **kw)
                signal.alarm(0)
                return rt
            except Exception as se:
                logger.error(se)
                if not ignore_err:
                    raise Exception(se)

        return wrapper

    return decorator


def get_classifer_post(text, url=""):
    headers = {"Content-Type": "application/json"}

    data = {"data": {"text": text}}
    r = requests.post(url=url, headers=headers, data=json.dumps(data))
    result_json = json.loads(r.text)
    return result_json


def get_value_by_key(box, keys=[]):
    stack = [box]
    res = []
    while stack:
        sp = stack.pop()
        if isinstance(sp, list):
            for v in sp:
                stack.append(v)
        elif isinstance(sp, Box):
            for k, v in sp.items():
                if k in keys:
                    res.append(v)
                else:
                    stack.append(v)
    return res


def get_classifer_result(data):
    r_post = get_classifer_post(data, url='')
    lables = get_value_by_key(Box(r_post), ["label"])
    scores = get_value_by_key(Box(r_post), ["score"])
    scores = [s if l == "normal" else s * -1 for l, s in zip(lables, scores)]
    text = get_value_by_key(Box(r_post), ["text"])
    return {k: v for k, v in zip(text, scores)}


if __name__ == "__main__":
    text = ["有啥要qq号码嘛？", "出售qq,13"]

    print(get_classifer_post(text))
    result = {
        "result": [
            {
                "predictions": [{"label": "normal", "score": 0.9938834686938403}],
                "text": "有啥要qq号码嘛？",
            },
            {
                "predictions": [{"label": "spam", "score": 0.9407028785858631}],
                "text": "出售qq,13",
            },
        ]
    }
    print(get_classifer_result(result))
