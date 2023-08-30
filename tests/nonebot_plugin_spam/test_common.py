import spam_common as q
import os


class T:
    text = ["有啥要qq号码嘛？", "出售qq,13"]
    bert_url = os.getenv("bert_url", "")
    result = {
        "result": [
            {
                "predictions": [{"label": "normal", "score": 0.9937738530780085}],
                "text": "有啥要qq号码嘛？",
            },
            {
                "predictions": [{"label": "spam", "score": 0.57104671899086}],
                "text": "出售qq,13",
            },
        ]
    }


class TestTableStmt:
    def test_request_result(self):
        sq = q.ScoreRequest(T.text)
        sq.init_request(T.bert_url)

    def test_cacl(self):
        sq = q.ScoreRequest(T.text)
        sq.encode = T.result
        assert sq.dicts == {
            "有啥要qq号码嘛？": 0.9937738530780085,
            "出售qq,13": -0.57104671899086,
        }
