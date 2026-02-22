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

    # 支持：tmdb=123 / tmdb:123 / tmdbid=123 / TMDB=123
    _re_tmdb = re.compile(r"(?:tmdbid|tmdb)\s*[:=]\s*(\d+)", re.IGNORECASE)

    def init_plugin(self, config: dict = None):
        self.downloadchain = DownloadChain()
        self.subscribechain = SubscribeChain()
        self.tmdb = TmdbApi()
        self.stop_service()

        if not config:
            return

        self._enabled = config.get("enabled")
        self._onlyonce = config.get("onlyonce")
        self._cron = config.get("cron")
        self._clear = config.get("clear")
        self._movies = config.get("movies")
        self._tvs = config.get("tvs")

        if self._clear:
            self.del_data(key="history")
            self._clear = False
            self.__update_config()
            logger.info("订阅历史清理完成")

        if not (self._enabled or self._onlyonce):
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if self._onlyonce:
            logger.info("影视将映订阅服务启动，立即运行一次")
            self._scheduler.add_job(
                self.__release,
                "date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="影视将映订阅"
            )
            self._onlyonce = False
            self.__update_config()

        if self._cron:
            try:
                self._scheduler.add_job(
                    func=self.__release,
                    trigger=CronTrigger.from_crontab(self._cron),
                    name="影视将映订阅"
                )
            except Exception as err:
                logger.error(f"定时任务配置错误：{err}")
                self.systemmessage.put(f"执行周期配置错误：{err}")

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def __release(self):
        if not self._movies and not self._tvs:
            logger.warn("暂无作品订阅，停止运行")
            return

        history: List[dict] = self.get_data("history") or []

        if self._movies:
            logger.info("开始检查将映电影")
            noexist_medias, history = self.__subscribe(self._movies, MediaType.MOVIE, history)
            self._movies = ",".join(noexist_medias)
            self.__update_config()

        if self._tvs:
            logger.info("开始检查将映电视剧")
            noexist_medias, history = self.__subscribe(self._tvs, MediaType.TV, history)
            self._tvs = ",".join(noexist_medias)
            self.__update_config()

        self.save_data("history", history)
        logger.info("影视将映订阅任务完成")

    @staticmethod
    def _split_items(medias: str) -> List[str]:
        if not medias:
            return []
        medias = medias.replace("\r\n", "\n")
        items: List[str] = []
        for line in medias.split("\n"):
            line = line.strip()
            if not line:
                continue
            for part in line.split(","):
                part = part.strip()
                if part:
                    items.append(part)
        return items

    def _extract_tmdbid(self, text: str) -> Optional[int]:
        m = self._re_tmdb.search(text or "")
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _tv_seasons_from_tmdb_detail(self, tv_detail: dict) -> List[int]:
        """
        从 TMDB TV 详情中提取所有季号，默认跳过 Season 0 specials。
        """
        season_numbers: List[int] = []
        for s in (tv_detail.get("seasons") or []):
            try:
                sn = int(s.get("season_number"))
            except Exception:
                continue
            if sn >= 1:
                season_numbers.append(sn)
        season_numbers = sorted(set(season_numbers))
        if season_numbers:
            return season_numbers

        try:
            nos = int(tv_detail.get("number_of_seasons") or 0)
        except Exception:
            nos = 0
        if nos > 0:
            return list(range(1, nos + 1))

        return [1]

    def __subscribe(self, medias, mtype: MediaType, history):
        noexist_medias = []
        items = self._split_items(medias)

        for media_name in items:
            if not media_name:
                continue

            tmdbid_override = self._extract_tmdbid(media_name)

            # 解析标题/年份/季（用户如果写了 S02，则只订该季）
            _, key_word, season_num, episode_num, year, _content = StringUtils.get_keyword(media_name)

            meta = MetaInfo(key_word)
            meta.type = mtype
            if season_num:
                meta.begin_season = season_num
            if episode_num:
                meta.begin_episode = episode_num
            if year:
                meta.year = year

            # 电影：tmdb=xxx 直接订阅
            if mtype == MediaType.MOVIE and tmdbid_override:
                logger.info(f"开始订阅 电影 {meta.name} (tmdb={tmdbid_override})")
                self.subscribechain.add(
                    title=meta.name,
                    year=meta.year,
                    mtype=MediaType.MOVIE,
                    tmdbid=tmdbid_override,
                    exist_ok=True,
                    username="影视将映订阅"
                )
                history.append({
                    "title": meta.name,
                    "type": mtype.value,
                    "year": meta.year,
                    "poster": "",
                    "overview": "",
                    "tmdbid": tmdbid_override,
                    "doubanid": None,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "unique": f"mediarelease: {meta.name} (TMDB:{tmdbid_override})"
                })
                continue

            # 剧集：tmdb=xxx 直接订阅，并订阅所有季（除非用户显式给了 season_num）
            if mtype == MediaType.TV and tmdbid_override:
                logger.info(f"开始订阅 电视剧 {meta.name} (tmdb={tmdbid_override})")

                tv_detail = {}
                try:
                    tv_detail = self.tmdb.get_info(MediaType.TV, tmdbid_override) or {}
                except Exception as e:
                    logger.warn(f"{meta.name} (tmdb={tmdbid_override}) 获取季信息失败：{e}")
                    tv_detail = {}

                seasons_to_subscribe = [season_num] if season_num else self._tv_seasons_from_tmdb_detail(tv_detail)

                orig_begin_season = getattr(meta, "begin_season", None)
                try:
                    for s in seasons_to_subscribe:
                        meta.begin_season = s

                        # 这里 mediainfo 不好构造完整对象（需要更多详情），因此库存在性检查只能依赖订阅链侧处理。
                        # 但订阅判重可以用 subscribechain.add 内部 exist_ok 兜底；我们仍然尽量按季写入。
                        self.subscribechain.add(
                            title=meta.name,
                            year=meta.year,
                            mtype=MediaType.TV,
                            tmdbid=tmdbid_override,
                            season=s,
                            exist_ok=True,
                            username="影视将映订阅"
                        )

                        history.append({
                            "title": meta.name,
                            "type": mtype.value,
                            "year": meta.year,
                            "season": s,
                            "poster": "",
                            "overview": "",
                            "tmdbid": tmdbid_override,
                            "doubanid": None,
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "unique": f"mediarelease: {meta.name} (TMDB:{tmdbid_override}) S{s}"
                        })
                finally:
                    meta.begin_season = orig_begin_season

                continue

            # 未给 tmdbid：走原搜索逻辑
            if mtype == MediaType.MOVIE:
                search_medias = self.tmdb.search_movies(meta.name, meta.year)
            else:
                search_medias = self.tmdb.search_tvs(meta.name, meta.year)

            search_medias = [MediaInfo(tmdb_info=info) for info in search_medias]
            if not search_medias:
                logger.warn(f"{mtype.value} {media_name} 在TMDB中未找到")
                noexist_medias.append(media_name)
                continue

            for mediainfo in search_medias:
                # 电影（搜索分支）
                if mtype == MediaType.MOVIE:
                    exist_flag, _ = self.downloadchain.get_no_exists_info(meta=meta, mediainfo=mediainfo)
                    if exist_flag:
                        logger.warn(f"{mediainfo.title_year} 媒体库中已存在")
                        continue

                    if self.subscribechain.exists(mediainfo=mediainfo):
                        logger.warn(f"{mediainfo.title_year} 订阅已存在")
                        continue

                    logger.info(f"开始订阅 {mtype.value} {mediainfo.title_year} TMDBID {mediainfo.tmdb_id}")
                    self.subscribechain.add(
                        title=mediainfo.title,
                        year=mediainfo.year,
                        mtype=mediainfo.type,
                        tmdbid=mediainfo.tmdb_id,
                        doubanid=mediainfo.douban_id,
                        exist_ok=True,
                        username="影视将映订阅"
                    )

                    history.append({
                        "title": mediainfo.title,
                        "type": mtype.value,
                        "year": mediainfo.year,
                        "poster": mediainfo.get_poster_image(),
                        "overview": mediainfo.overview,
                        "tmdbid": mediainfo.tmdb_id,
                        "doubanid": mediainfo.douban_id,
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "unique": f"mediarelease: {mediainfo.title} (DB:{mediainfo.tmdb_id})"
                    })
                    continue

                # 剧集（搜索分支）：显式季 -> 单季；否则订阅所有季
                if season_num:
                    seasons_to_subscribe = [season_num]
                else:
                    tv_detail = {}
                    try:
                        tv_detail = self.tmdb.get_info(MediaType.TV, mediainfo.tmdb_id) or {}
                    except Exception as e:
                        logger.warn(f"{mediainfo.title_year} 获取季信息失败：{e}")
                        tv_detail = {}
                    seasons_to_subscribe = self._tv_seasons_from_tmdb_detail(tv_detail)

                orig_begin_season = getattr(meta, "begin_season", None)
                try:
                    for s in seasons_to_subscribe:
                        meta.begin_season = s

                        exist_flag, _ = self.downloadchain.get_no_exists_info(meta=meta, mediainfo=mediainfo)
                        if exist_flag:
                            logger.warn(f"{mediainfo.title_year} S{s:02d} 媒体库中已存在")
                            continue

                        if self.subscribechain.exists(mediainfo=mediainfo, meta=meta):
                            logger.warn(f"{mediainfo.title_year} S{s:02d} 订阅已存在")
                            continue

                        logger.info(f"开始订阅 {mtype.value} {mediainfo.title_year} S{s:02d} TMDBID {mediainfo.tmdb_id}")
                        self.subscribechain.add(
                            title=mediainfo.title,
                            year=mediainfo.year,
                            mtype=mediainfo.type,
                            tmdbid=mediainfo.tmdb_id,
                            doubanid=mediainfo.douban_id,
                            season=s,
                            exist_ok=True,
                            username="影视将映订阅"
                        )

                        history.append({
                            "title": mediainfo.title,
                            "type": mtype.value,
                            "year": mediainfo.year,
                            "season": s,
                            "poster": mediainfo.get_poster_image(),
                            "overview": mediainfo.overview,
                            "tmdbid": mediainfo.tmdb_id,
                            "doubanid": mediainfo.douban_id,
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "unique": f"mediarelease: {mediainfo.title} (DB:{mediainfo.tmdb_id}) S{s}"
                        })
                finally:
                    meta.begin_season = orig_begin_season

        logger.info(f"{mtype.value} 将映订阅任务完成")
        return noexist_medias, history

    @eventmanager.register(EventType.PluginAction)
    def remote_subscribe(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "media_release":
                return
            args = event_data.get("arg_str")
            if not args:
                logger.error(f"缺少参数：{event_data}")
                return
            args = args.split(" ")
            if len(args) < 2:
                logger.error(f"参数错误：{event_data} 电影/电视剧 名称 年份")
                self.post_message(channel=event.event_data.get("channel"),
                                  title="参数错误！格式：电影/电视剧 名称 年份！",
                                  userid=event.event_data.get("user"))
                return

            content = " ".join(args[1:])
            if str(args[0]) == "电影":
                if not self._movies:
                    self._movies = str(content)
                else:
                    movies = [movie for movie in self._movies.split(",")]
                    if str(content) in movies:
                        logger.error(f"{content} 已在电影列表中")
                        if event.event_data.get("user"):
                            self.post_message(channel=event.event_data.get("channel"),
                                              title=f"{content} 已在电影列表中！",
                                              userid=event.event_data.get("user"))
                        return
                    movies.append(str(content))
                    self._movies = ",".join(movies)

                self.__update_config()
                if event.event_data.get("user"):
                    self.post_message(channel=event.event_data.get("channel"),
                                      title=f"{content} 已添加电影将映订阅！",
                                      userid=event.event_data.get("user"))

            elif str(args[0]) == "电视剧":
                if not self._tvs:
                    self._tvs = str(content)
                else:
                    tvs = [tv for tv in self._tvs.split(",")]
                    if str(content) in tvs:
                        logger.error(f"{content} 已在电视剧列表中")
                        if event.event_data.get("user"):
                            self.post_message(channel=event.event_data.get("channel"),
                                              title=f"{content} 已在电视剧列表中！",
                                              userid=event.event_data.get("user"))
                        return
                    tvs.append(str(content))
                    self._tvs = ",".join(tvs)

                self.__update_config()
                if event.event_data.get("user"):
                    self.post_message(channel=event.event_data.get("channel"),
                                      title=f"{content} 已添加电视剧将映订阅！",
                                      userid=event.event_data.get("user"))
            else:
                logger.error(f"参数错误：{event_data} 电影/电视剧 名称 年份")
                self.post_message(channel=event.event_data.get("channel"),
                                  title="参数错误！格式：电影/电视剧 名称 年份！",
                                  userid=event.event_data.get("user"))
                return

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "clear": self._clear,
            "movies": self._movies,
            "tvs": self._tvs,
        })

    def delete_history(self, key: str, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        historys = self.get_data("history")
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")
        historys = [h for h in historys if h.get("unique") != key]
        self.save_data("history", historys)
        return schemas.Response(success=True, message="删除成功")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/mrs",
                "event": EventType.PluginAction,
                "desc": "影视将映订阅",
                "category": "",
                "data": {"action": "media_release"}
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除订阅历史记录"
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VSwitch", "props": {"model": "clear", "label": "清理订阅记录"}}]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {"model": "cron", "label": "执行周期", "placeholder": "5位cron表达式，留空自动"}
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{"component": "VTextarea", "props": {"model": "movies", "label": "电影", "rows": 4, "placeholder": "电影名称(多个英文逗号或换行分隔，可加 tmdb=xxxx)"}}]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{"component": "VTextarea", "props": {"model": "tvs", "label": "电视剧", "rows": 4, "placeholder": "电视剧名称(多个英文逗号或换行分隔，可加 tmdb=xxxx)"}}]
                            }
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "5 1 * * *",
            "clear": False,
            "tvs": "",
            "movies": "",
        }

    def get_page(self) -> List[dict]:
        historys = self.get_data("history")
        if not historys:
            return [{"component": "div", "text": "暂无数据", "props": {"class": "text-center"}}]

        historys = sorted(historys, key=lambda x: x.get("time"), reverse=True)
        contents = []
        for history in historys:
            title = history.get("title")
            poster = history.get("poster")
            mtype = history.get("type")
            time_str = history.get("time")
            tmdbid = history.get("tmdbid")
            doubanid = history.get("doubanid")
            unique_key = history.get("unique") or f"mediarelease: {title} (DB:{tmdbid})"

            contents.append({
                "component": "VCard",
                "content": [
                    {
                        "component": "VDialogCloseBtn",
                        "props": {"innerClass": "absolute top-0 right-0"},
                        "events": {"click": {"api": "plugin/MediaRelease/delete_history", "method": "get",
                                             "params": {"key": unique_key, "apikey": settings.API_TOKEN}}}
                    },
                    {
                        "component": "div",
                        "props": {"class": "d-flex justify-space-start flex-nowrap flex-row"},
                        "content": [
                            {
                                "component": "div",
                                "content": [
                                    {
                                        "component": "VImg",
                                        "props": {
                                            "src": poster,
                                            "height": 120,
                                            "width": 80,
                                            "aspect-ratio": "2/3",
                                            "class": "object-cover shadow ring-gray-500",
                                            "cover": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "div",
                                "content": [
                                    {
                                        "component": "VCardSubtitle",
                                        "props": {"class": "pa-2 font-bold break-words whitespace-break-spaces"},
                                        "content": [
                                            {
                                                "component": "a",
                                                "props": {"href": f"https://movie.douban.com/subject/{doubanid}", "target": "_blank"},
                                                "text": title
                                            }
                                        ]
                                    },
                                    {"component": "VCardText", "props": {"class": "pa-0 px-2"}, "text": f"类型：{mtype}"},
                                    {"component": "VCardText", "props": {"class": "pa-0 px-2"}, "text": f"时间：{time_str}"}
                                ]
                            }
                        ]
                    }
                ]
            })

        return [{"component": "div", "props": {"class": "grid gap-3 grid-info-card"}, "content": contents}]

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))