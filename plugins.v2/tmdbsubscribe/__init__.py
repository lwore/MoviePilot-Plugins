from datetime import datetime, timedelta
import re
import pytz

from app import schemas
from app.chain.download import DownloadChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfo
from app.modules.themoviedb import TmdbApi
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.schemas import MediaType
from app.schemas.types import EventType
from app.utils.string import StringUtils


class TMDBSubscribe(_PluginBase):
    plugin_name = "批量订阅"
    plugin_desc = "通过TMDB ID批量订阅添加订阅。"
    plugin_icon = "https://raw.githubusercontent.com/thsrite/MoviePilot-Plugins/main/icons/mediarelease.png"
    plugin_version = "1.0"
    plugin_author = "Lwore"
    author_url = "https://github.com/Lwore"
    plugin_config_prefix = "mediarelease_"
    plugin_order = 26
    auth_level = 2

    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    subscribechain = None
    downloadchain = None
    tmdb = None
    _scheduler: Optional[BackgroundScheduler] = None
    _clear = False
    _movies = None
    _tvs = None

    # 允许写法：tmdb=123 / tmdb:123 / tmdbid=123 / TMDB=123
    _re_tmdb = re.compile(r"(?:tmdbid|tmdb)\s*[:=]\s*(\d+)", re.IGNORECASE)

    def init