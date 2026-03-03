import json
import re
import sqlite3
import time
import asyncio
from datetime import datetime
from dataclasses import dataclass

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
import pathlib
from astrbot.api import logger, AstrBotConfig

DIARY_SYSTEM_PROMPT = """你是一个日记作者。请根据下方提供的聊天记录，撰写一篇日记。

要求：
1. 输出一个严格的 JSON 对象，仅包含两个字段：
   - "title"：日记标题，简短概括当天内容，不超过 20 字，不含特殊字符和标点
   - "content"：日记正文，用自然流畅的语言总结聊天内容，体现情感与细节
2. 只输出 JSON，不要有任何多余文字或 markdown 标记

请继承以下人格风格来撰写日记，使日记语气与人格保持一致：
{persona_prompt}
{custom_prompt}"""

MAX_TITLE_LEN = 20
TITLE_SANITIZE_RE = re.compile(r"[^\w\u4e00-\u9fff\u3400-\u4dbf\s]")


@dataclass
class NoteRecord:
    title: str
    content: str
    created_at: int


@register(
    "astrbot_plugin_daily_note",
    "astrbot_user",
    "总结聊天记录为日记，存入本地 SQLite，支持按编号查询",
    "0.1.0",
    "https://github.com/your_name/astrbot_plugin_daily_note",
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

    # ── 数据库初始化 ─────────────────────────────────────

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS daily_notes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    unified_msg_origin TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )"""
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

    async def _schedule_loop(self):
        interval_days = max(self.config.get("interval_days", 1), 1)
        interval_sec = interval_days * 86400
        while True:
            await asyncio.sleep(interval_sec)
            try:
                await self._generate_all_sessions()
            except Exception as e:
                logger.error(f"[daily_note] 定时生成日记出错: {e}")

    async def _generate_all_sessions(self):
        conv_mgr = self.context.conversation_manager
        conversations = await conv_mgr.get_conversations(
            platform_id=None, unified_msg_origin=None
        )
        for conv in conversations:
            umo = self._build_umo(conv)
            if not umo:
                logger.warning(
                    f"[daily_note] 会话 cid={conv.cid} 无法构建 unified_msg_origin，跳过"
                )
                continue
            try:
                await self._generate_note_for_conversation(
                    umo=umo,
                    cid=conv.cid,
                    persona_id=conv.persona_id,
                )
            except Exception as e:
                logger.error(
                    f"[daily_note] 会话 cid={conv.cid} umo={umo} 生成失败: {e}"
                )

    @staticmethod
    def _build_umo(conv) -> str | None:
        """从 Conversation 对象推导 unified_msg_origin。

        Conversation 存有 platform_id 和 user_id，
        unified_msg_origin 的格式通常为 'platform_name:message_type:session_id'。
        如果 conv 本身就带有该字段则直接使用，否则尝试拼接。
        """
        if hasattr(conv, "unified_msg_origin") and conv.unified_msg_origin:
            return conv.unified_msg_origin
        if conv.platform_id and conv.user_id:
            return f"{conv.platform_id}:{conv.user_id}"
        return None

    # ── 日记生成核心 ──────────────────────────────────────

    async def _generate_note_for_conversation(
        self, umo: str, cid: str, persona_id: str | None
    ):
        conv_mgr = self.context.conversation_manager
        conversation = await conv_mgr.get_conversation(
            unified_msg_origin=umo, conversation_id=cid
        )
        if not conversation or not conversation.history:
            return

        history = conversation.history
        if isinstance(history, str):
            try:
                history = json.loads(history)
            except json.JSONDecodeError:
                logger.warning(
                    f"[daily_note] cid={cid} history 非有效 JSON，按原始字符串处理"
                )

        max_messages = max(self.config.get("max_messages", 50), 1)
        if isinstance(history, list):
            recent = history[-max_messages:]
            chat_text = self._format_history(recent)
        else:
            chat_text = str(history)[-6000:]

        if not chat_text.strip():
            return

        if self._has_recent_note(umo, cid, chat_text):
            logger.info(f"[daily_note] cid={cid} 无新增内容，跳过生成")
            return

        persona_prompt = "（无特定人格，使用默认自然风格）"
        if persona_id:
            try:
                persona = await self.context.persona_manager.get_persona(persona_id)
                if persona and persona.system_prompt:
                    persona_prompt = persona.system_prompt
            except Exception as e:
                logger.warning(
                    f"[daily_note] 获取人格 persona_id={persona_id} 失败: {e}，使用默认风格"
                )

        custom_prompt = self.config.get("custom_prompt", "")
        custom_section = f"\n额外风格要求：{custom_prompt}" if custom_prompt else ""

        system_prompt = DIARY_SYSTEM_PROMPT.format(
            persona_prompt=persona_prompt,
            custom_prompt=custom_section,
        )

        provider_id = self.config.get("provider", "")
        if not provider_id:
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo=umo)
            except Exception as e:
                logger.warning(
                    f"[daily_note] umo={umo} 无法获取默认 provider: {e}，跳过"
                )
                return

        llm_resp = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=f"以下是需要总结为日记的聊天记录：\n\n{chat_text}",
            system_prompt=system_prompt,
        )

        title, content = self._parse_llm_response(llm_resp.completion_text)
        title = self._sanitize_title(title)
        self._save_note(title, content, umo)
        logger.info(f"[daily_note] 为 umo={umo} cid={cid} 生成日记: {title}")

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
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        text = text.strip()

        try:
            data = json.loads(text)
            title = str(data.get("title", "无题日记")).strip()
            content = str(data.get("content", text)).strip()
        except json.JSONDecodeError:
            title = f"日记_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            content = text
        return title, content

    @staticmethod
    def _sanitize_title(title: str) -> str:
        title = TITLE_SANITIZE_RE.sub("", title).strip()
        if len(title) > MAX_TITLE_LEN:
            title = title[:MAX_TITLE_LEN]
        if not title:
            title = f"日记_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return title

    # ── 增量去重 ──────────────────────────────────────────

    def _has_recent_note(self, umo: str, cid: str, current_chat_text: str) -> bool:
        """检查最近一篇日记的生成时间之后是否有新消息。

        通过比较最新日记的 created_at 与聊天记录的内容哈希来判断。
        简化方案：如果最近一篇日记距今不到间隔时间的 80%，则视为无需重新生成。
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT created_at FROM daily_notes "
                "WHERE unified_msg_origin = ? ORDER BY created_at DESC LIMIT 1",
                (umo,),
            ).fetchone()

        if row is None:
            return False

        last_created = row[0]
        interval_days = max(self.config.get("interval_days", 1), 1)
        threshold_sec = interval_days * 86400 * 0.8
        if (time.time() - last_created) < threshold_sec:
            return True
        return False

    # ── 数据库操作 ────────────────────────────────────────

    def _save_note(self, title: str, content: str, umo: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO daily_notes (title, content, unified_msg_origin, created_at) "
                "VALUES (?, ?, ?, ?)",
                (title, content, umo, int(time.time())),
            )

    def _get_notes_list(self, umo: str) -> list[tuple[int, str, int]]:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT id, title, created_at FROM daily_notes "
                "WHERE unified_msg_origin = ? ORDER BY created_at DESC",
                (umo,),
            ).fetchall()

    def _get_note_by_dynamic_index(self, umo: str, index: int) -> NoteRecord | None:
        notes = self._get_notes_list(umo)
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

    # ── 管理员指令 ────────────────────────────────────────

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("daily_note")
    async def cmd_daily_note(self, event: AstrMessageEvent, index: int = 0):
        '''查看日记列表或按编号查看日记内容。
        不带参数显示列表，带编号显示对应日记全文。'''
        event.stop_event()
        umo = event.unified_msg_origin

        if index == 0:
            notes = self._get_notes_list(umo)
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
            note = self._get_note_by_dynamic_index(umo, index)
            if note is None:
                total = len(self._get_notes_list(umo))
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
    async def cmd_daily_note_gen(self, event: AstrMessageEvent):
        '''手动触发为当前会话生成一篇日记'''
        event.stop_event()
        umo = event.unified_msg_origin

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
            await self._generate_note_for_conversation(
                umo=umo, cid=cid, persona_id=conversation.persona_id
            )
            notes = self._get_notes_list(umo)
            if notes:
                latest_title = notes[0][1]
                yield event.plain_result(
                    f"日记生成成功：{latest_title}\n使用 /daily_note 1 查看内容"
                )
            else:
                yield event.plain_result("日记生成流程完成，但未产生记录。")
        except Exception as e:
            logger.error(f"[daily_note] 手动生成失败 umo={umo} cid={cid}: {e}")
            yield event.plain_result(f"生成失败：{e}")

    # ── 插件卸载 ──────────────────────────────────────────

    async def terminate(self):
        if self._schedule_task and not self._schedule_task.done():
            self._schedule_task.cancel()
            try:
                await self._schedule_task
            except asyncio.CancelledError:
                pass
