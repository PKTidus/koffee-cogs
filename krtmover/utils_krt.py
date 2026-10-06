"""Helpers for the long-running bulk channel transfer (`[p]krtmove`).

Design goals, in order:
1. Never lose or reorder messages.
2. Use as few webhook calls as possible: Discord limits webhook messages to roughly
   30/minute *per destination channel*, so the number of webhook calls is what
   decides how long a 1.5M-message transfer takes.
3. Survive restarts, network errors and odd messages without stopping the job.
"""
import asyncio
import io
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

import aiohttp
import discord

logger = logging.getLogger("red.krtmover")

MAX_CONTENT = 2000
MAX_FILES_PER_MSG = 10
MAX_EMBEDS_PER_MSG = 10
UPLOAD_LIMIT = 10 * 1024 * 1024  # upload limit for a non-boosted destination server
MERGE_WINDOW = 300  # seconds: consecutive messages by one author within this window may be merged
MERGE_MAX_LEN = 1900  # leave room for the timestamp line
STAMP_GAP = 600  # seconds: add a timestamp line when there's a gap this big (same as msgcopy)
SEND_ATTEMPTS = 5

NO_MENTIONS = discord.AllowedMentions.none()
_BANNED_NAME = re.compile(r"discord|clyde", re.IGNORECASE)


class WebhookGone(Exception):
    """The destination webhook was deleted or is invalid. The job can't continue."""


@dataclass
class FileSpec:
    """Raw attachment bytes. We keep bytes rather than discord.File objects because
    discord.py closes File objects after a send, which would break retries."""
    data: bytes
    filename: str
    url: str
    description: Optional[str] = None

    def to_file(self) -> discord.File:
        kwargs = {"filename": self.filename}
        if self.description:
            kwargs["description"] = self.description
        return discord.File(io.BytesIO(self.data), **kwargs)


@dataclass
class Payload:
    username: str
    avatar_url: Optional[str]
    content: str = ""
    embeds: List[discord.Embed] = field(default_factory=list)
    files: List[FileSpec] = field(default_factory=list)


# ---------- message classification ----------

def is_system(msg: discord.Message) -> bool:
    """Joins, pins, boosts, thread-created notices, etc. These are skipped."""
    return msg.is_system()


def rich_embeds(msg: discord.Message) -> List[discord.Embed]:
    # Only bot-made ("rich") embeds. Link previews regenerate by themselves from the URL.
    return [e for e in msg.embeds if e.type == "rich"]


def is_mergeable(msg: discord.Message) -> bool:
    """Plain text messages that can safely be combined into one webhook message."""
    return (
        not msg.is_system()
        and msg.type == discord.MessageType.default
        and bool(msg.content)
        and not msg.attachments
        and not msg.stickers
        and not rich_embeds(msg)
        and msg.reference is None
        and not getattr(msg, "message_snapshots", None)
        and getattr(msg, "poll", None) is None
    )


def can_join(group: List[discord.Message], msg: discord.Message) -> bool:
    if not group or not is_mergeable(msg):
        return False
    last = group[-1]
    if msg.author.id != last.author.id:
        return False
    if (msg.created_at - last.created_at).total_seconds() > MERGE_WINDOW:
        return False
    current_len = sum(len(m.clean_content) + 1 for m in group)
    return current_len + len(msg.clean_content) <= MERGE_MAX_LEN


# ---------- payload building ----------

def webhook_name(user) -> str:
    name = (getattr(user, "display_name", None) or getattr(user, "name", None) or "").strip()
    # Webhook usernames may not contain "discord" or "clyde"
    name = _BANNED_NAME.sub(lambda m: m.group(0)[0] + "🗪" + m.group(0)[3:], name)
    return name[:80] or "Unknown User"


def split_content(text: str, limit: int = MAX_CONTENT) -> List[str]:
    """Split on line boundaries where possible, preserving newlines."""
    if len(text) <= limit:
        return [text] if text else []
    chunks, current = [], ""
    for line in text.split("\n"):
        while len(line) > limit:  # a single gigantic line
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def reply_embed(msg: discord.Message) -> discord.Embed:
    ref = msg.reference.resolved if msg.reference else None
    embed = discord.Embed(color=discord.Color(0x25C059))
    if isinstance(ref, discord.Message):
        body = ref.clean_content or "Click to see attachment 🖼️"
        body = (body[:56] + "...") if len(body) > 56 else body
        embed.set_author(
            name=f"↪️ {webhook_name(ref.author)}: {body}"[:256],
            icon_url=ref.author.display_avatar.url,
            url=ref.jump_url,
        )
    else:
        embed.set_author(name="↪️ Original message was deleted")
    return embed


async def download(att: discord.Attachment) -> Optional[FileSpec]:
    for attempt in range(3):
        try:
            data = await att.read()
            return FileSpec(data=data, filename=att.filename, url=att.url, description=att.description)
        except discord.NotFound:
            return None
        except (discord.HTTPException, aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(2 * (attempt + 1))
    return None


async def build_payloads(msgs: List[discord.Message], add_stamp: bool) -> List[Payload]:
    """Turn one message (or a merged group of messages from one author) into the
    minimum number of webhook sends. Usually exactly one."""
    first = msgs[0]
    username = webhook_name(first.author)
    avatar = first.author.display_avatar.url

    lines: List[str] = []
    if add_stamp:
        lines.append(f"-# 🕒 <t:{int(first.created_at.timestamp())}:f>")

    embeds: List[discord.Embed] = []
    files: List[FileSpec] = []
    extra_urls: List[str] = []

    for m in msgs:
        if m.reference and m.type == discord.MessageType.reply:
            embeds.append(reply_embed(m))
        if m.clean_content:
            lines.append(m.clean_content)

        # Forwarded messages: content lives in the snapshot
        for snap in getattr(m, "message_snapshots", None) or []:
            snap_text = getattr(snap, "content", "") or ""
            quoted = "\n".join(f"> {ln}" for ln in snap_text.split("\n")) if snap_text else ""
            lines.append("> *Forwarded*" + (f"\n{quoted}" if quoted else ""))
            embeds.extend(e for e in (getattr(snap, "embeds", None) or []) if e.type == "rich")
            extra_urls.extend(a.url for a in (getattr(snap, "attachments", None) or []))

        embeds.extend(rich_embeds(m))

        for att in m.attachments:
            spec = await download(att) if att.size <= UPLOAD_LIMIT else None
            if spec is None:
                extra_urls.append(att.url)
            else:
                files.append(spec)

        for sticker in m.stickers:
            extra_urls.append(sticker.url)

        if getattr(m, "poll", None) is not None:
            lines.append(f"📊 **Poll:** {m.poll.question}")

    lines.extend(f"📎 {u}" for u in extra_urls)
    chunks = split_content("\n".join(lines))

    # Pack files into batches (max 10 files and UPLOAD_LIMIT bytes per message)
    file_batches: List[List[FileSpec]] = []
    batch, batch_size = [], 0
    for f in files:
        if batch and (len(batch) >= MAX_FILES_PER_MSG or batch_size + len(f.data) > UPLOAD_LIMIT):
            file_batches.append(batch)
            batch, batch_size = [], 0
        batch.append(f)
        batch_size += len(f.data)
    if batch:
        file_batches.append(batch)

    embed_batches = [embeds[i:i + MAX_EMBEDS_PER_MSG] for i in range(0, len(embeds), MAX_EMBEDS_PER_MSG)]

    payloads = [Payload(username, avatar, content=c) for c in chunks]
    if not payloads and (embed_batches or file_batches):
        payloads.append(Payload(username, avatar))
    if not payloads:
        return []

    # Attach the first embed/file batch to the last text chunk, overflow gets its own sends
    last = payloads[-1]
    if embed_batches:
        last.embeds = embed_batches.pop(0)
    if file_batches:
        last.files = file_batches.pop(0)
    for eb in embed_batches:
        payloads.append(Payload(username, avatar, embeds=eb))
    for fb in file_batches:
        payloads.append(Payload(username, avatar, files=fb))
    return payloads


# ---------- sending ----------

async def _send(webhook: discord.Webhook, p: Payload) -> discord.WebhookMessage:
    kwargs = {
        "username": p.username,
        "allowed_mentions": NO_MENTIONS,  # never ping anyone in the destination server
        "wait": True,
    }
    if p.avatar_url:
        kwargs["avatar_url"] = p.avatar_url
    if p.content:
        kwargs["content"] = p.content
    if p.embeds:
        kwargs["embeds"] = p.embeds
    if p.files:
        kwargs["files"] = [f.to_file() for f in p.files]
    return await webhook.send(**kwargs)


async def _send_with_retry(webhook: discord.Webhook, p: Payload) -> discord.WebhookMessage:
    """Retries transient failures. 429s are already handled inside discord.py."""
    delay = 2
    for attempt in range(SEND_ATTEMPTS):
        try:
            return await _send(webhook, p)
        except (discord.NotFound, discord.Forbidden) as e:
            raise WebhookGone(str(e)) from e
        except discord.HTTPException as e:
            if e.status == 401:
                raise WebhookGone(str(e)) from e
            if e.status < 500 or attempt == SEND_ATTEMPTS - 1:
                raise
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == SEND_ATTEMPTS - 1:
                raise
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)


def _fallbacks(p: Payload):
    """Progressively simpler versions of a payload that Discord rejected (4xx)."""
    urls = "\n".join(f"📎 {f.url}" for f in p.files)
    text = "\n".join(x for x in (p.content, urls) if x)
    if not text:
        text = "**[krtmover]** Unsupported content"
    # 1. same author, no embeds, files as links (handles 413 too-large / bad embeds)
    yield Payload(p.username, p.avatar_url, content=text[:MAX_CONTENT])
    # 2. generic author (handles rejected usernames / avatars)
    prefixed = f"**{p.username}:** {text}"
    yield Payload("Unknown User", None, content=prefixed[:MAX_CONTENT])


async def send_payloads(webhook: discord.Webhook, payloads: List[Payload]) -> bool:
    """Send every payload in order. Returns False if any of them couldn't be sent at all."""
    ok = True
    for p in payloads:
        try:
            await _send_with_retry(webhook, p)
            continue
        except WebhookGone:
            raise
        except Exception as err:
            logger.warning("krtmover: send rejected (%s), trying fallback", err)
        for fb in _fallbacks(p):
            try:
                await _send_with_retry(webhook, fb)
                break
            except WebhookGone:
                raise
            except Exception as err:
                logger.warning("krtmover: fallback rejected (%s)", err)
        else:
            ok = False
    return ok
