import asyncio
import json
import pathlib
import re
import sqlite3
import time
from datetime import datetime, timedelta
from dataclasses import dataclass

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register

DIARY_SYSTEM_PROMPT = (
    "你是一个日记作者。请根据下方提供的聊天记录，撰写一篇日记。\n"
    "\n"
    "要求：\n"
    "1. 输出一个严格的 JSON 对象，仅包含两个字段：\n"
    '   - "title"：日记标题，简短概括当天内容，不超过 20 字，不含特殊字符和标点\n'
    '   - "content"：日记正文，用自然流畅的语言总结聊天内容，体现情感与细节\n'
    "2. 只输出 JSON，不要有任何多余文字或 markdown 标记\n"
    "\n"
    "请继承以下人格风格来撰写日记，使日记语气与人格保持一致：\n"
    "$PERSONA_PROMPT$\n"
    "$CUSTOM_PROMPT$"
)

MAX_TITLE_LEN = 20
TITLE_SANITIZE_RE = re.compile(r"[^\w\u4e00-\u9fff\u3400-\u4dbf\s]")

_FALLBACK_WAIT_SEC = 3600.0
_MAX_LOCK_POOL_SIZE = 500
_CLEANUP_INTERVAL_SEC = 86400


@dataclass
class NoteRecord:
    title: str
    content: str
    created_at: int


@register(
    "astrbot_plugin_daily_note",
    "X轩万",
    "总结聊天记录为日记，存入本地 SQLite，支持按编号查询",
    "0.1.0",
    "https://github.com/976230600/astrbot_plugin_daily_note",
)
class DailyNotePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        data_dir = pathlib.Path("data") / "astrbot_plugin_daily_note"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(data_dir / "daily_note.db")

        self._init_db()
        self._schedule_task: asyncio.Task | None = None
        self._session_locks: dict[tuple[str, str], asyncio.Lock] = {}

    def _get_session_lock(self, umo: str, cid: str) -> asyncio.Lock:
        key = (umo, cid)
        if key not in self._session_locks:
            if len(self._session_locks) > _MAX_LOCK_POOL_SIZE:
                idle = [k for k, v in self._session_locks.items() if not v.locked()]
                for k in idle:
                    del self._session_locks[k]
            self._session_locks[key] = asyncio.Lock()
        return self._session_locks[key]

    # ── 数据库初始化与迁移 ────────────────────────────────

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS daily_notes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    unified_msg_origin TEXT NOT NULL,
                    cid TEXT NOT NULL DEFAULT '',
                    history_count INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS schedule_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )"""
            )
            self._migrate_columns(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_notes_umo_cid_time "
                "ON daily_notes (unified_msg_origin, cid, created_at DESC)"
            )

    @staticmethod
    def _migrate_columns(conn: sqlite3.Connection):
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(daily_notes)").fetchall()
        }
        if "cid" not in existing:
            conn.execute("ALTER TABLE daily_notes ADD COLUMN cid TEXT NOT NULL DEFAULT ''")
        if "history_count" not in existing:
            conn.execute(
                "ALTER TABLE daily_notes ADD COLUMN history_count INTEGER NOT NULL DEFAULT 0"
            )

    # ── 状态持久化 ────────────────────────────────────────

    def _get_state(self, key: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT value FROM schedule_state WHERE key = ?", (key,)
            ).fetchone()
        return row[0] if row else None

    def _set_state(self, key: str, value: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO schedule_state (key, value) VALUES (?, ?)",
                (key, value),
            )

    # ── 定时任务 ─────────────────────────────────────────

    @filter.on_astrbot_loaded()
    async def _on_loaded(self):
        if self._schedule_task and not self._schedule_task.done():
            self._schedule_task.cancel()
            try:
                await self._schedule_task
            except asyncio.CancelledError:
                pass
        self._schedule_task = asyncio.create_task(self._schedule_loop())

    def _calc_next_trigger(self) -> float:
        """基于持久化的上次触发时间计算下次触发，重启后不丢失间隔。

        内部异常时回退到安全默认值，确保调度线程不会崩溃。
        """
        try:
            hour = max(0, min(23, self.config.get("generate_hour", 22)))
            interval_days = max(self.config.get("interval_days", 1), 1)

            now = datetime.now()
            today_target = now.replace(hour=hour, minute=0, second=0, microsecond=0)

            last_trigger_str = self._get_state("last_trigger_time")
            if last_trigger_str:
                last_dt = datetime.fromtimestamp(float(last_trigger_str))
                next_target = last_dt.replace(
                    hour=hour, minute=0, second=0, microsecond=0
                ) + timedelta(days=interval_days)
                while next_target <= now:
                    next_target += timedelta(days=interval_days)
            else:
                next_target = (
                    today_target
                    if today_target > now
                    else today_target + timedelta(days=interval_days)
                )

            return max((next_target - now).total_seconds(), 60)
        except Exception as e:
            logger.error(
                f"[daily_note] 计算下次触发时间出错: {e}，"
                f"{_FALLBACK_WAIT_SEC / 3600:.0f} 小时后重试"
            )
            return _FALLBACK_WAIT_SEC

    async def _schedule_loop(self):
        while True:
            wait_sec = self._calc_next_trigger()
            logger.info(
                f"[daily_note] 下次自动生成将在 {wait_sec / 3600:.1f} 小时后触发"
            )
            await asyncio.sleep(wait_sec)

            should_cleanup = True
            last_cleanup_str = self._get_state("last_cleanup_time")
            if last_cleanup_str:
                try:
                    should_cleanup = (
                        time.time() - float(last_cleanup_str) > _CLEANUP_INTERVAL_SEC
                    )
                except (ValueError, TypeError):
                    pass
            if should_cleanup:
                try:
                    await self._cleanup_orphan_notes()
                    self._set_state("last_cleanup_time", str(time.time()))
                except Exception as e:
                    logger.error(f"[daily_note] 孤儿日记清理出错: {e}")

            try:
                gen, skip, fail = await self._generate_all_sessions()
                if gen > 0 or fail == 0:
                    self._set_state("last_trigger_time", str(time.time()))
                if gen > 0:
                    logger.info(
                        f"[daily_note] 本轮完成：生成 {gen}，跳过 {skip}，失败 {fail}"
                    )
                elif fail > 0:
                    logger.warning(
                        f"[daily_note] 本轮生成失败 {fail} 个会话"
                        f"（跳过 {skip}），不推进触发时间"
                    )
                else:
                    logger.info(
                        f"[daily_note] 本轮所有 {skip} 个会话均无新内容"
                    )
            except Exception as e:
                logger.error(f"[daily_note] 定时生成日记出错: {e}")

    async def _generate_all_sessions(self) -> tuple[int, int, int]:
        """遍历所有会话生成日记，返回 (生成数, 跳过数, 失败数)。"""
        conv_mgr = self.context.conversation_manager
        conversations = await conv_mgr.get_conversations(
            platform_id=None, unified_msg_origin=None
        )
        gen_count = 0
        skip_count = 0
        fail_count = 0
        for conv in conversations:
            umo = self._build_umo(conv)
            if not umo:
                logger.warning(
                    f"[daily_note] 会话 cid={conv.cid} 无法构建 unified_msg_origin，跳过"
                )
                skip_count += 1
                continue
            try:
                generated = await self._generate_note_for_conversation(
                    umo=umo,
                    cid=conv.cid,
                    persona_id=conv.persona_id,
                )
                if generated:
                    gen_count += 1
                else:
                    skip_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(
                    f"[daily_note] 会话 cid={conv.cid} umo={umo} 生成失败: {e}"
                )
        return gen_count, skip_count, fail_count

    @staticmethod
    def _build_umo(conv) -> str | None:
        """从 Conversation 对象推导 unified_msg_origin。

        优先尝试 conv 自身的 unified_msg_origin 属性（若存在），
        回退到 platform_id（AstrBot 当前版本中存储的就是完整 umo）。
        """
        umo = getattr(conv, "unified_msg_origin", None)
        if umo:
            return umo
        if conv.platform_id:
            return conv.platform_id
        return None

    # ── 日记生成核心（纯增量模型） ────────────────────────

    async def _generate_note_for_conversation(
        self,
        umo: str,
        cid: str,
        persona_id: str | None,
        force: bool = False,
    ) -> bool:
        """纯增量生成：只处理 history_end 之后的新消息，全量送入 LLM。

        force=True 时忽略增量检查，基于全部历史重新生成。
        """
        async with self._get_session_lock(umo, cid):
            conv_mgr = self.context.conversation_manager
            conversation = await conv_mgr.get_conversation(
                unified_msg_origin=umo, conversation_id=cid
            )
            if not conversation or not conversation.history:
                return False

            history = conversation.history
            if isinstance(history, str):
                try:
                    history = json.loads(history)
                except json.JSONDecodeError:
                    logger.warning(
                        f"[daily_note] cid={cid} history 非有效 JSON，跳过该会话"
                    )
                    return False

            if not isinstance(history, list):
                logger.warning(
                    f"[daily_note] cid={cid} history 格式异常"
                    f"（类型 {type(history).__name__}），跳过该会话"
                )
                return False

            total_count = len(history)
            last_end = 0 if force else self._get_last_history_end(umo, cid)

            if total_count < last_end:
                logger.info(
                    f"[daily_note] umo={umo} cid={cid} 检测到上下文压缩"
                    f"（当前 {total_count} 条 < 上次 {last_end} 条），重置基线"
                )
                last_end = 0

            if total_count <= last_end:
                return False

            min_messages = max(self.config.get("min_messages", 3), 1)
            new_messages = history[last_end:]
            if not force and len(new_messages) < min_messages:
                logger.info(
                    f"[daily_note] umo={umo} cid={cid} 新消息不足"
                    f"（{len(new_messages)} 条 < 最低 {min_messages} 条），跳过"
                )
                return False

            chat_text = self._format_history(new_messages)
            if not chat_text.strip():
                return False

            persona_prompt = "（无特定人格，使用默认自然风格）"
            if persona_id:
                try:
                    persona = await self.context.persona_manager.get_persona(
                        persona_id
                    )
                    if persona and persona.system_prompt:
                        persona_prompt = persona.system_prompt
                except Exception as e:
                    logger.warning(
                        f"[daily_note] 获取人格 persona_id={persona_id} 失败: {e}，"
                        "使用默认风格"
                    )

            custom_prompt = self.config.get("custom_prompt", "")
            custom_section = f"\n额外风格要求：{custom_prompt}" if custom_prompt else ""

            system_prompt = (
                DIARY_SYSTEM_PROMPT
                .replace("$PERSONA_PROMPT$", persona_prompt)
                .replace("$CUSTOM_PROMPT$", custom_section)
            )

            provider_id = self.config.get("provider", "")
            if not provider_id:
                try:
                    provider_id = await self.context.get_current_chat_provider_id(
                        umo=umo
                    )
                except Exception as e:
                    raise RuntimeError(f"无法获取模型提供商: {e}") from e

            full_prompt = (
                f"{system_prompt}\n\n以下是需要总结为日记的聊天记录：\n\n{chat_text}"
            )
            llm_resp = await self.context.llm_generate(
                chat_provider_id=provider_id,
                prompt=full_prompt,
            )

            resp_text = llm_resp.completion_text or ""
            if not resp_text.strip():
                raise RuntimeError("LLM 返回空内容")

            title, content = self._parse_llm_response(resp_text)
            title = self._sanitize_title(title)
            self._save_note(title, content, umo, cid, total_count)
            logger.info(f"[daily_note] 为 umo={umo} cid={cid} 生成日记: {title}")
            return True

    @staticmethod
    def _format_history(messages: list) -> str:
        lines = []
        for msg in messages:
            role = msg.get("role", "unknown")
            parts = msg.get("content", [])
            if isinstance(parts, str):
                text = parts
            elif isinstance(parts, list):
                text_parts = []
                for p in parts:
                    if isinstance(p, dict):
                        text_parts.append(p.get("text", ""))
                    elif isinstance(p, str):
                        text_parts.append(p)
                text = " ".join(text_parts)
            else:
                text = str(parts)
            if text.strip():
                lines.append(f"[{role}]: {text.strip()}")
        return "\n".join(lines)

    @staticmethod
    def _parse_llm_response(text: str) -> tuple[str, str]:
        raw = text.strip()
        json_match = re.search(r"\{[\s\S]*\}", raw)
        if json_match:
            try:
                data = json.loads(json_match.group())
                title = str(data.get("title", "无题日记")).strip()
                content = str(data.get("content", raw)).strip()
                return title, content
            except json.JSONDecodeError:
                pass
        return f"日记_{datetime.now().strftime('%Y%m%d_%H%M%S')}", raw

    @staticmethod
    def _sanitize_title(title: str) -> str:
        title = TITLE_SANITIZE_RE.sub("", title).strip()
        if len(title) > MAX_TITLE_LEN:
            title = title[:MAX_TITLE_LEN]
        if not title:
            title = f"日记_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return title

    # ── 增量追踪 ──────────────────────────────────────────

    def _get_last_history_end(self, umo: str, cid: str) -> int:
        """获取该会话最近一篇日记的 history_end（即上次消费到的位置）。"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT history_count FROM daily_notes "
                "WHERE unified_msg_origin = ? AND cid = ? "
                "ORDER BY created_at DESC LIMIT 1",
                (umo, cid),
            ).fetchone()
        return row[0] if row else 0

    # ── 孤儿日记清理 ──────────────────────────────────────

    async def _cleanup_orphan_notes(self):
        """清理已不存在的会话所对应的日记。"""
        conv_mgr = self.context.conversation_manager

        with sqlite3.connect(self.db_path) as conn:
            pairs = conn.execute(
                "SELECT DISTINCT unified_msg_origin, cid FROM daily_notes"
            ).fetchall()

        for umo, cid in pairs:
            try:
                conv = await conv_mgr.get_conversation(
                    unified_msg_origin=umo, conversation_id=cid
                )
                if not conv:
                    self._delete_notes_by_conversation(umo, cid)
                    logger.info(
                        f"[daily_note] 清理孤儿日记 umo={umo} cid={cid}"
                    )
            except Exception as e:
                logger.debug(
                    f"[daily_note] 检查会话存在性出错 umo={umo} cid={cid}: {e}"
                )

    # ── 数据库操作 ────────────────────────────────────────

    def _save_note(
        self, title: str, content: str, umo: str, cid: str, history_end: int
    ):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO daily_notes "
                "(title, content, unified_msg_origin, cid, history_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (title, content, umo, cid, history_end, int(time.time())),
            )

    def _get_notes_list(self, umo: str, cid: str) -> list[tuple[int, str, int]]:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT id, title, created_at FROM daily_notes "
                "WHERE unified_msg_origin = ? AND cid = ? "
                "ORDER BY created_at DESC",
                (umo, cid),
            ).fetchall()

    def _get_note_by_dynamic_index(
        self, umo: str, cid: str, index: int
    ) -> NoteRecord | None:
        notes = self._get_notes_list(umo, cid)
        if 1 <= index <= len(notes):
            db_id = notes[index - 1][0]
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT title, content, created_at FROM daily_notes WHERE id = ?",
                    (db_id,),
                ).fetchone()
            if row:
                return NoteRecord(title=row[0], content=row[1], created_at=row[2])
        return None

    def _delete_note_by_dynamic_index(
        self, umo: str, cid: str, index: int
    ) -> str | None:
        """按动态编号删除日记，返回被删除日记的标题，不存在返回 None。"""
        notes = self._get_notes_list(umo, cid)
        if 1 <= index <= len(notes):
            db_id, title, _ = notes[index - 1]
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM daily_notes WHERE id = ?", (db_id,))
            return title
        return None

    def _delete_notes_by_conversation(self, umo: str, cid: str) -> int:
        """删除指定会话 (umo+cid) 下的所有日记，返回删除数量。"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM daily_notes "
                "WHERE unified_msg_origin = ? AND cid = ?",
                (umo, cid),
            )
            return cursor.rowcount

    # ── 管理员指令 ────────────────────────────────────────

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("daily_note")
    async def cmd_daily_note(self, event: AstrMessageEvent, index: int = 0):
        '''查看日记列表或按编号查看日记内容。
        不带参数显示列表，带编号显示对应日记全文。'''
        event.stop_event()
        umo = event.unified_msg_origin

        conv_mgr = self.context.conversation_manager
        cid = await conv_mgr.get_curr_conversation_id(umo)

        if index == 0:
            notes = self._get_notes_list(umo, cid)
            if not notes:
                yield event.plain_result("暂无日记记录。")
                return

            lines = ["日记列表：", ""]
            for i, (_, title, created_at) in enumerate(notes, 1):
                dt = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M")
                lines.append(f"  {i}. [{dt}] {title}")
            lines.append("")
            lines.append(f"共 {len(notes)} 篇日记")
            lines.append("使用 /daily_note <编号> 查看完整内容")
            yield event.plain_result("\n".join(lines))
        else:
            note = self._get_note_by_dynamic_index(umo, cid, index)
            if note is None:
                total = len(self._get_notes_list(umo, cid))
                if total == 0:
                    yield event.plain_result("暂无日记记录。")
                else:
                    yield event.plain_result(
                        f"编号 {index} 不存在，当前共 {total} 篇日记（编号范围 1-{total}）。"
                    )
                return
            dt = datetime.fromtimestamp(note.created_at).strftime("%Y-%m-%d %H:%M")
            yield event.plain_result(f"[ {note.title} ]\n{dt}\n\n{note.content}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("daily_note_gen")
    async def cmd_daily_note_gen(self, event: AstrMessageEvent, mode: str = ""):
        '''手动触发为当前会话生成一篇日记。加 force 参数可忽略增量检查强制生成'''
        event.stop_event()
        umo = event.unified_msg_origin
        force = mode.strip().lower() == "force"

        conv_mgr = self.context.conversation_manager
        cid = await conv_mgr.get_curr_conversation_id(umo)
        conversation = await conv_mgr.get_conversation(
            unified_msg_origin=umo, conversation_id=cid
        )
        if not conversation or not conversation.history:
            yield event.plain_result("当前会话无聊天记录，无法生成日记。")
            return

        yield event.plain_result("正在生成日记，请稍候...")

        try:
            generated = await self._generate_note_for_conversation(
                umo=umo,
                cid=cid,
                persona_id=conversation.persona_id,
                force=force,
            )
            if generated:
                notes = self._get_notes_list(umo, cid)
                latest_title = notes[0][1] if notes else "未知"
                yield event.plain_result(
                    f"日记生成成功：{latest_title}\n使用 /daily_note 1 查看内容"
                )
            elif force:
                yield event.plain_result(
                    "当前会话无有效聊天内容，未能生成日记。"
                )
            else:
                yield event.plain_result(
                    "当前会话没有新的聊天记录，无需生成日记。\n"
                    "如需强制重新生成，请使用 /daily_note_gen force"
                )
        except Exception as e:
            logger.error(f"[daily_note] 手动生成失败 umo={umo} cid={cid}: {e}")
            yield event.plain_result(f"生成失败：{e}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("daily_note_del")
    async def cmd_daily_note_del(self, event: AstrMessageEvent, index: int = 0):
        '''删除指定编号的日记'''
        event.stop_event()
        umo = event.unified_msg_origin

        conv_mgr = self.context.conversation_manager
        cid = await conv_mgr.get_curr_conversation_id(umo)

        if index < 1:
            yield event.plain_result("请指定要删除的日记编号，如 /daily_note_del 1")
            return

        title = self._delete_note_by_dynamic_index(umo, cid, index)
        if title is None:
            total = len(self._get_notes_list(umo, cid))
            if total == 0:
                yield event.plain_result("暂无日记记录。")
            else:
                yield event.plain_result(
                    f"编号 {index} 不存在，当前共 {total} 篇日记（编号范围 1-{total}）。"
                )
            return

        yield event.plain_result(f"已删除日记 #{index}：{title}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("daily_note_clear")
    async def cmd_daily_note_clear(self, event: AstrMessageEvent):
        '''清空当前会话的所有日记'''
        event.stop_event()
        umo = event.unified_msg_origin

        conv_mgr = self.context.conversation_manager
        cid = await conv_mgr.get_curr_conversation_id(umo)

        count = self._delete_notes_by_conversation(umo, cid)
        if count == 0:
            yield event.plain_result("暂无日记记录，无需清空。")
        else:
            yield event.plain_result(f"已清空 {count} 篇日记。")

    # ── 插件卸载 ──────────────────────────────────────────

    async def terminate(self):
        if self._schedule_task and not self._schedule_task.done():
            self._schedule_task.cancel()
            try:
                await self._schedule_task
            except asyncio.CancelledError:
                pass
