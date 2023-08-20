from pydantic import BaseModel, Extra, Field
from typing import List


class BotInfo(BaseModel):
    id: str = Field(alias="id")
    token: str = Field(alias="token")
    secret: str = Field(alias="secret")


class Config(BaseModel, extra=Extra.ignore):
    """Plugin Config Here"""

    pg_conn: str = ""
    qguild_bots: List[BotInfo] = Field(default_factory=list)
