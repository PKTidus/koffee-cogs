import asyncio
import aiohttp
import time
import typing
from datetime import datetime, timedelta, timezone

from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import humanize_number, humanize_timedelta
import discord
from discord import Webhook

from .utils import msgFormatter, webhookSettings, webhookFinder, WEBHOOK_EMPTY_AVATAR, WEBHOOK_EMPTY_NAME
from .utils_copy import timestampEmbed
from .utils_relay import relayGetData, relayAddChannel, relayRemoveChannel, relayCheckInput, fixMsgrelayStoreV2alpha
from .utils_krt import (
    STAMP_GAP, WebhookGone, build_payloads, can_join, is_mergeable, is_system, send_payloads,
)

import logging
logger = logging.getLogger(__name__)


DEFAULT_JOB = {
    "fromChannel": None,
    "toWebhook": None,
    "notifyChannel": None,
    "lastId": None,       # checkpoint: last source message fully handled
    "lastCreated": None,  # unix timestamp of that message
    "moved": 0,           # source messages successfully copied
    "sent": 0,            # webhook messages created
    "skipped": 0,         # system messages (joins, pins...) and empty messages
    "failed": 0,
    "deleted": 0,
    "failedIds": [],      # last 200 failed source message ids
    "delete": False,      # delete source messages after they're copied
    "merge": False,       # merge consecutive plain-text messages by the same author
    "running": False,
    "done": False,
    "error": None,
}


class Krtmover(commands.Cog):
    """Move messages around, cross-channels, cross-server!
    
    **`[p]krtmove`** - Transfer an entire channel (oldest → newest) to another channel/server. Resumable, survives restarts. *(long-running)*
    - *Requires server admins with **Administrator** permissions*

    **`[p]msgcopy`** - Copies a set # of messages from one channel to another *(single-use)*
    - *Requires users with **Manage Messages** permissions*

    **`[p]msgrelay`** - Forward new messages to other channels/servers *(continuous)*
    - *Requires server admins with **Administrator** permissions*

    Based on Msgmover by coffeebank.
    """

    def __init__(self, bot):
        self.config = Config.get_conf(self, identifier=806715409318936616)
        self.bot = bot
        default_guild = {
            "msgrelayStoreV2": {},
            "relayTimer": 20,
            "krtJob": DEFAULT_JOB,
        }
        """
            "msgrelayStoreV2": {
                "chanId": [
                    {
                        "toWebhook": str,
                        "pref": bool,
                        "pref": bool,
                    },
                ],
            }
        """
        self.config.register_guild(**default_guild)
        # krtmove runtime state, keyed by guild id
        self._tasks: typing.Dict[int, asyncio.Task] = {}
        self._jobs: typing.Dict[int, dict] = {}      # live job dicts (same shape as DEFAULT_JOB)
        self._stopping: typing.Set[int] = set()
        self._session: typing.Dict[int, tuple] = {}  # (monotonic start, moved at start)

    async def cog_load(self):
        # Auto-resume transfers that were running when the bot/cog stopped
        for guild_id, data in (await self.config.all_guilds()).items():
            if data.get("krtJob", {}).get("running"):
                self._start_task(guild_id)

    async def cog_unload(self):
        # Jobs stay marked as running in config, so they resume on next load
        for task in self._tasks.values():
            task.cancel()

    # This cog does not store any End User Data
    async def red_get_data_for_user(self, *, user_id: int):
        return {}
    async def red_delete_data_for_user(self, *, requester, user_id: int) -> None:
        pass



    # krtmove: whole-channel transfer

    def _start_task(self, guild_id: int):
        self._stopping.discard(guild_id)
        task = asyncio.create_task(self._runner(guild_id))
        self._tasks[guild_id] = task
        task.add_done_callback(lambda t, g=guild_id: self._tasks.pop(g, None) if self._tasks.get(g) is t else None)

    async def _save(self, guild_id: int, job: dict):
        await self.config.guild_from_id(guild_id).krtJob.set(job)

    async def _notify(self, job: dict, text: str):
        channel = self.bot.get_channel(job.get("notifyChannel") or 0)
        if channel:
            try:
                await channel.send(text)
            except discord.HTTPException:
                pass

    async def _runner(self, guild_id: int):
        await self.bot.wait_until_red_ready()
        job = await self.config.guild_from_id(guild_id).krtJob()
        self._jobs[guild_id] = job
        self._session[guild_id] = (time.monotonic(), job["moved"])
        backoff = 5
        while True:
            try:
                finished = await self._transfer(guild_id, job)
                job["running"] = False
                job["done"] = finished
                job["error"] = None
                await self._save(guild_id, job)
                if finished:
                    await self._notify(job, f"✅ **krtmove finished!** Moved {humanize_number(job['moved'])} messages "
                                            f"({humanize_number(job['failed'])} failed, {humanize_number(job['skipped'])} skipped).")
                else:
                    await self._notify(job, "⏸️ krtmove stopped. Use `krtmove resume` to continue.")
                return
            except asyncio.CancelledError:
                raise
            except WebhookGone as err:
                job["running"] = False
                job["error"] = f"Destination webhook is gone: {err}"
                await self._save(guild_id, job)
                await self._notify(job, "❌ krtmove stopped: the destination webhook was deleted or is invalid. "
                                        "Set a new one with `krtmove destination <webhook URL>`, then `krtmove resume` (progress is kept).")
                return
            except Exception as err:
                # Network blips, Discord outages... wait and pick up from the checkpoint
                logger.exception("krtmove: transfer error in guild %s, retrying in %ss", guild_id, backoff)
                job["error"] = f"{type(err).__name__}: {err} (retrying)"
                await self._save(guild_id, job)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 600)

    async def _transfer(self, guild_id: int, job: dict) -> bool:
        """Copy from the checkpoint onward. Returns True when the channel is fully done,
        False when stopped by the user."""
        channel = self.bot.get_channel(job["fromChannel"]) or await self.bot.fetch_channel(job["fromChannel"])
        after = discord.Object(id=job["lastId"]) if job["lastId"] else None
        delete_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        deleter = asyncio.create_task(self._delete_worker(channel, delete_queue, job))

        async def checkpoint(msg: discord.Message):
            job["lastId"] = msg.id
            job["lastCreated"] = msg.created_at.timestamp()
            await self._save(guild_id, job)

        async def flush(msgs: typing.List[discord.Message]):
            prev = job["lastCreated"]
            add_stamp = prev is None or msgs[0].created_at.timestamp() - prev > STAMP_GAP
            try:
                payloads = await build_payloads(msgs, add_stamp)
            except Exception:
                logger.exception("krtmove: could not format message(s) %s", [m.id for m in msgs])
                payloads = None
            if payloads == []:
                job["skipped"] += len(msgs)
            elif payloads and await send_payloads(webhook, payloads):
                job["moved"] += len(msgs)
                job["sent"] += len(payloads)
                if job["delete"]:
                    for m in msgs:
                        await delete_queue.put(m.id)  # blocks if deletes fall behind
            else:
                job["failed"] += len(msgs)
                job["failedIds"] = (job["failedIds"] + [m.id for m in msgs])[-200:]
                for m in msgs:
                    logger.warning("krtmove: failed to copy %s", m.jump_url)
            await checkpoint(msgs[-1])

        unloading = False
        try:
            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(job["toWebhook"], session=session)
                group: typing.List[discord.Message] = []
                async for msg in channel.history(limit=None, after=after, oldest_first=True):
                    if guild_id in self._stopping:
                        break
                    if group and not can_join(group, msg):
                        await flush(group)
                        group = []
                    if is_system(msg):
                        job["skipped"] += 1
                        await checkpoint(msg)
                    elif job["merge"] and is_mergeable(msg):
                        group.append(msg)
                    else:
                        await flush([msg])
                if group:
                    await flush(group)
            return guild_id not in self._stopping
        except asyncio.CancelledError:
            unloading = True  # don't block, leftover queued deletions are just skipped
            raise
        finally:
            if not unloading:
                # Let queued deletions finish (they're already copied), then stop the worker
                try:
                    await asyncio.wait_for(delete_queue.join(), timeout=300)
                except asyncio.TimeoutError:
                    logger.warning("krtmove: gave up waiting on %s queued deletions", delete_queue.qsize())
            deleter.cancel()

    async def _delete_worker(self, channel, queue: asyncio.Queue, job: dict):
        """Deletes copied source messages in the background so it doesn't slow the copy down.
        Messages < 14 days old are bulk-deleted 100 at a time; older ones one by one."""
        while True:
            batch = [await queue.get()]
            while len(batch) < 100 and not queue.empty():
                batch.append(queue.get_nowait())
            try:
                cutoff = discord.utils.time_snowflake(discord.utils.utcnow() - timedelta(days=13, hours=23))
                young = [i for i in batch if i > cutoff]
                old = [i for i in batch if i <= cutoff]
                if len(young) >= 2:
                    try:
                        await channel.delete_messages([discord.Object(id=i) for i in young])
                        job["deleted"] += len(young)
                        young = []
                    except discord.HTTPException:
                        pass
                for i in old + young:
                    try:
                        await channel.get_partial_message(i).delete()
                        job["deleted"] += 1
                    except discord.NotFound:
                        pass
                    except discord.HTTPException as err:
                        logger.warning("krtmove: could not delete %s: %s", i, err)
            except Exception:
                logger.exception("krtmove: delete worker error")
            finally:
                for _ in batch:
                    queue.task_done()

    async def _get_job(self, guild: discord.Guild) -> dict:
        return self._jobs.get(guild.id) or await self.config.guild(guild).krtJob()

    @commands.group(name="krtmove", aliases=["krt"])
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def krtmove(self, ctx: commands.Context):
        """Transfer an entire channel to another channel/server

        Copies everything oldest → newest via webhook, saving progress after every message.
        If the bot restarts, the transfer resumes automatically.

        Quick start:
        1. (optional) `[p]krtmove merge true` - fewer webhook messages, much faster
        2. (optional) `[p]krtmove delete true` - delete source messages once copied
        3. `[p]krtmove start #source-channel <webhook URL or #channel>`
        4. `[p]krtmove status` to watch progress
        """

    @krtmove.command(name="start")
    async def krt_start(self, ctx, fromChannel: typing.Union[discord.TextChannel, discord.Thread], toChannel: typing.Union[discord.TextChannel, str], start_after_id: typing.Optional[int] = None):
        """Start a transfer from the very beginning of fromChannel

        toChannel can be a #channel (in a server the bot is in) or a webhook URL (any server).
        start_after_id is an optional message ID. If provided, the transfer starts copying messages *after* this one.
        """
        if ctx.guild.id in self._tasks:
            return await ctx.send("A transfer is already running in this server. See `krtmove status` or `krtmove stop`.")
        old = await self.config.guild(ctx.guild).krtJob()
        if old["lastId"] and not old["done"]:
            return await ctx.send("There's an unfinished transfer saved. Use `krtmove resume` to continue it, "
                                  "or `krtmove reset` to throw away its progress first.")
        me = fromChannel.permissions_for(ctx.guild.me)
        if not (me.read_message_history and me.view_channel):
            return await ctx.send(f"I need **View Channel** and **Read Message History** in {fromChannel.mention}.")
        toWebhook = await relayCheckInput(self, ctx, toChannel)
        if toWebhook == False:
            return

        job = dict(DEFAULT_JOB, failedIds=[])
        job.update(
            fromChannel=fromChannel.id, toWebhook=toWebhook, notifyChannel=ctx.channel.id,
            delete=old["delete"], merge=old["merge"], running=True,
        )
        if start_after_id:
            try:
                m = await fromChannel.fetch_message(start_after_id)
                job["lastId"] = m.id
                job["lastCreated"] = m.created_at.timestamp()
            except discord.NotFound:
                return await ctx.send("Could not find that message ID in the source channel.")
            except discord.HTTPException:
                return await ctx.send("Failed to fetch the start message. Please check the ID.")

        if job["delete"] and not me.manage_messages:
            return await ctx.send(f"Delete mode is on but I don't have **Manage Messages** in {fromChannel.mention}.")
        await self.config.guild(ctx.guild).krtJob.set(job)
        self._start_task(ctx.guild.id)
        msg_suffix = f" after message `{start_after_id}`" if start_after_id else " (oldest first)"
        await ctx.send(f"🚚 Transfer started from {fromChannel.mention}{msg_suffix}. "
                       f"Merge: **{job['merge']}**, Delete: **{job['delete']}**. Check progress with `{ctx.clean_prefix}krtmove status`.")

    @krtmove.command(name="resume")
    async def krt_resume(self, ctx):
        """Resume a stopped transfer from its last checkpoint"""
        if ctx.guild.id in self._tasks:
            return await ctx.send("Already running.")
        job = await self.config.guild(ctx.guild).krtJob()
        if not job["fromChannel"] or job["done"]:
            return await ctx.send("Nothing to resume. Use `krtmove start`.")
        job["running"] = True
        job["notifyChannel"] = ctx.channel.id
        await self.config.guild(ctx.guild).krtJob.set(job)
        self._start_task(ctx.guild.id)
        await ctx.send("▶️ Resumed.")

    @krtmove.command(name="destination", aliases=["dest"])
    async def krt_destination(self, ctx, toChannel: typing.Union[discord.TextChannel, str]):
        """Change where the saved transfer sends to, keeping its progress"""
        if ctx.guild.id in self._tasks:
            return await ctx.send("Stop the transfer first.")
        toWebhook = await relayCheckInput(self, ctx, toChannel)
        if toWebhook == False:
            return
        await self.config.guild(ctx.guild).krtJob.set_raw("toWebhook", value=toWebhook)
        await self.config.guild(ctx.guild).krtJob.set_raw("error", value=None)
        self._jobs.pop(ctx.guild.id, None)
        await ctx.send("Destination updated. Use `krtmove resume` to continue.")

    @krtmove.command(name="stop")
    async def krt_stop(self, ctx):
        """Stop the transfer gracefully (progress is kept)"""
        if ctx.guild.id not in self._tasks:
            return await ctx.send("No transfer is running.")
        self._stopping.add(ctx.guild.id)
        await ctx.send("Stopping after the current message... Use `krtmove resume` to continue later.")

    @krtmove.command(name="reset")
    async def krt_reset(self, ctx):
        """Forget the saved transfer (does not touch any messages)"""
        if ctx.guild.id in self._tasks:
            return await ctx.send("Stop the transfer first.")
        job = await self.config.guild(ctx.guild).krtJob()
        await self.config.guild(ctx.guild).krtJob.set(dict(DEFAULT_JOB, failedIds=[], delete=job["delete"], merge=job["merge"]))
        self._jobs.pop(ctx.guild.id, None)
        await ctx.send("Transfer progress cleared.")

    @krtmove.command(name="merge")
    async def krt_merge(self, ctx, enabled: bool):
        """Merge consecutive plain-text messages by the same author (within 5 min) into one

        Biggest speed-up available: chat channels often have 2-3x fewer webhook sends this way.
        Messages with attachments, embeds, stickers or replies are never merged. Can be changed mid-transfer."""
        await self._set_flag(ctx, "merge", enabled)

    @krtmove.command(name="delete")
    async def krt_delete(self, ctx, enabled: bool):
        """Delete source messages after they've been copied successfully

        Failed messages are never deleted, so whatever is left in the source afterwards is what failed.
        ⚠️ Irreversible. Take a backup first. Can be changed mid-transfer."""
        await self._set_flag(ctx, "delete", enabled)

    async def _set_flag(self, ctx, key: str, value: bool):
        live = self._jobs.get(ctx.guild.id)
        if live is not None:
            live[key] = value  # picked up by the running transfer immediately
        await self.config.guild(ctx.guild).krtJob.set_raw(key, value=value)
        await ctx.send(f"`{key}` set to **{value}**.")

    @krtmove.command(name="status")
    async def krt_status(self, ctx):
        """Show transfer progress"""
        job = await self._get_job(ctx.guild)
        if not job["fromChannel"]:
            return await ctx.send("No transfer set up. Use `krtmove start`.")
        running = ctx.guild.id in self._tasks
        state = "🟢 Running" if running else ("✅ Done" if job["done"] else "⏸️ Stopped")
        e = discord.Embed(title="krtmove status", color=await ctx.embed_colour(), description=state)
        e.add_field(name="Source", value=f"<#{job['fromChannel']}>")
        e.add_field(name="Moved", value=humanize_number(job["moved"]))
        e.add_field(name="Webhook msgs sent", value=humanize_number(job["sent"]))
        e.add_field(name="Skipped / Failed", value=f"{humanize_number(job['skipped'])} / {humanize_number(job['failed'])}")
        e.add_field(name="Deleted", value=humanize_number(job["deleted"]))
        e.add_field(name="Merge / Delete", value=f"{job['merge']} / {job['delete']}")
        if job["lastCreated"]:
            reached = datetime.fromtimestamp(job["lastCreated"], tz=timezone.utc)
            e.add_field(name="Reached", value=f"<t:{int(job['lastCreated'])}:f>")
            channel = self.bot.get_channel(job["fromChannel"])
            if channel:
                span = (discord.utils.utcnow() - channel.created_at).total_seconds()
                done = (reached - channel.created_at).total_seconds()
                if span > 0:
                    e.add_field(name="Timeline progress", value=f"~{min(done / span, 1) * 100:.1f}% (by date, not count)")
        if running and ctx.guild.id in self._session:
            start, moved0 = self._session[ctx.guild.id]
            elapsed = time.monotonic() - start
            if elapsed > 60:
                rate = (job["moved"] - moved0) / elapsed * 3600
                e.add_field(name="Speed (this session)", value=f"{humanize_number(int(rate))} msgs/hour")
                e.add_field(name="Session time", value=humanize_timedelta(seconds=int(elapsed)) or "0s")
        if job["error"]:
            e.add_field(name="Last error", value=str(job["error"])[:1000], inline=False)
        await ctx.send(embed=e)



    # msgcopy

    @commands.command(name="msgcopy", aliases=["msgmove", "msgmover"])
    @commands.has_permissions(manage_messages=True)
    @commands.bot_has_permissions(add_reactions=True, read_message_history=True)
    async def msgcopy(self, ctx, fromChannel: discord.TextChannel, toChannel: typing.Union[discord.TextChannel, str], maxMessages:int, skipMessages:int=0):
        """Copies messages from one channel to another

        toChannel can either be a #channel or a webhook URL.
        
        Retrieve 'maxMessages' number of messages from history, and optionally discard 'skipMessages' number of messages from the retrieved list.
        
        Retrieving more than 10 messages will result in Discord ratelimit throttling, so please be patient.
        
        - *Errors? Please [help us by reporting them in our Support Discord >](https://coffeebank.github.io/discord)*
        - *See all commands:* **`[p]help Msgmover`**"""

        # Error catching
        toWebhook = await relayCheckInput(self, ctx, toChannel)
        if toWebhook == False:
            return

        if maxMessages <= 0:
            return await ctx.send("Error: Please input a valid number of messages to copy.")
        if skipMessages >= maxMessages:
            return await ctx.send("Error: Cannot skip more messages than the max number of messages you are retrieving.")

        # Start webhook session
        await ctx.message.add_reaction("⏳")
        try:
            async with aiohttp.ClientSession() as session:
                webhook = Webhook.from_url(toWebhook, session=session)

                # Retrieve messages, sorted by oldest first
                # Can't use oldest_first= since that will only return earliest messages in channel, instead of what we want
                msgList = [message async for message in fromChannel.history(limit=maxMessages)]
                msgList.reverse()
                if skipMessages > 0:
                    # https://stackoverflow.com/a/37105499
                    msgList = msgList[:-skipMessages or None]

                # Send them via webhook
                msgItemLast = msgList[0].created_at
                for msgItem in msgList:
                    # Send timestamp if it's been more than 10mins time difference
                    # If they equal, it means it's the first item, so send timestamp
                    if msgItemLast == msgItem.created_at or (msgItem.created_at-msgItemLast).total_seconds() > 600:
                        await webhook.send(
                            username=WEBHOOK_EMPTY_NAME,
                            avatar_url=WEBHOOK_EMPTY_AVATAR,
                            embed=await timestampEmbed(self, ctx, msgItem.created_at)
                        )
                        await asyncio.sleep(1)
                    configJson = webhookSettings({"attachsAsUrl": False, "userProfiles": True})
                    whMsg = await msgFormatter(self, webhook, msgItem, configJson)
                    if whMsg == False:
                        await ctx.send("Failed to send: "+str(msgItem))
                    else:
                        # Trigger edited tag if it was edited
                        if msgItem.edited_at:
                            await msgFormatter(self, webhook, msgItem, configJson, editMsgId=whMsg.id)
                    # Save timestamp to msgItemLast
                    msgItemLast = msgItem.created_at

                # Add react on complete
                try:
                    await ctx.message.add_reaction("✅")
                except discord.NotFound:
                    await ctx.send("Done!")
        finally:
            await session.close()

    @commands.command(name="msgcount")
    @commands.bot_has_permissions(add_reactions=True)
    async def msgcount(self, ctx):
        """Find how many messages it has been after a message
        
        Reply to a message to use this command."""
        if ctx.message.reference:
            await ctx.message.add_reaction("⏳")
            messages = [message async for message in ctx.channel.history(limit=None, after=ctx.message.reference.resolved)]
            await ctx.message.add_reaction("✅")
            return await ctx.send(str(len(messages))+" + 2 (your bot command and this message)")
        else:
            return await ctx.send("Please reply to a message to use this command!")


    # msgrelay

    @commands.group(name="msgrelay")
    @commands.has_permissions(administrator=True)
    @commands.bot_has_permissions(add_reactions=True, embed_links=True)
    async def msgrelay(self, ctx: commands.Context):
        """Forward new messages to other channels/servers

        Create message relays - as if a portal connects the two chats.

        Have two-way conversations across 2+ servers at once!
        
        *[Join the Support Discord for announcements and more info](https://coffeebank.github.io/discord)*"""
        if not ctx.invoked_subcommand:
            pass

    @msgrelay.command(name="settings")
    @commands.has_permissions(administrator=True)
    @commands.bot_has_permissions(embed_links=True)
    async def mmmrsettings(self, ctx):
        """See relay settings (⚠️ Sensitive info)
        """
        # Message Relays
        msgrelayStoreV2 = await self.config.guild(ctx.guild).msgrelayStoreV2()
        relayList = ""
        for relayId in msgrelayStoreV2:
            relayList = f"**<#{relayId}>**\n{str(msgrelayStoreV2[relayId])}\n\n"
            eg = discord.Embed(color=(await ctx.embed_colour()), description=str(relayList)[:4090])
            await ctx.send(embed=eg)
        # Relay settings
        es = discord.Embed(color=(await ctx.embed_colour()), title="Message Relay Settings in this Server")
        es.add_field(name="Relay Timer", value=await self.config.guild(ctx.guild).relayTimer(), inline=True)
        await ctx.send(embed=es)

    @msgrelay.command(name="add")
    @commands.bot_has_permissions(add_reactions=True)
    async def mmmradd(self, ctx, fromChannel: discord.TextChannel, toChannel: typing.Union[discord.TextChannel, str]):
        """Create a message relay
        
        Cross-server relays must be a webhook. [How to create webhooks >](https://support.discord.com/hc/article_attachments/1500000463501/Screen_Shot_2020-12-15_at_4.41.53_PM.png)"""

        # Error catching
        relayResp = await relayCheckInput(self, ctx, toChannel)
        if relayResp == False:
            return

        # Create entry
        relayAdd = await relayAddChannel(self, ctx, fromChannel, relayResp)
        if relayAdd == False:
            return await ctx.send("Setup was stopped. Exited.")
        else:
            await self.config.guild(ctx.guild).msgrelayStoreV2.set(relayAdd)

        # Test
        try:
            await fromChannel.send("**Channels are now linked!**\nThis is a sample message.")
        except Exception as err:
            logger.error(err)
            return await ctx.send("Setup was successfully completed, but some permissions may be missing.")
        await ctx.message.add_reaction("✅")

    @msgrelay.command(name="edit")
    @commands.bot_has_permissions(add_reactions=True)
    async def mmmredit(self, ctx, fromChannel: discord.TextChannel, itemToEdit: int, toChannel: typing.Union[discord.TextChannel, str]):
        """Edit a message relay
        
        fromChannel: the originating #channel
        itemToEdit: put 1, 2, ... for which one you want to delete. (check `[p]msgrelay settings` for list of relays from each channel)
        
        Currently only supports webhook urls. [How to create webhooks.](https://support.discord.com/hc/article_attachments/1500000463501/Screen_Shot_2020-12-15_at_4.41.53_PM.png)
        *[Help us develop support for #channels >](https://coffeebank.github.io/discord)*"""

        # Error catching
        if itemToEdit <= 0:
            return await ctx.send("Error: If there's only one relay in the channel, please use 1 for itemToEdit.")
        relayResp = await relayCheckInput(self, ctx, toChannel)
        if relayResp == False:
            return

        # Ask for input and have it successfully complete before deleting old entry
        relayAdd = await relayAddChannel(self, ctx, fromChannel, relayResp)
        if relayAdd == False:
            return await ctx.send("Setup was stopped. Exited.")
        else:
            # Create new entry
            await self.config.guild(ctx.guild).msgrelayStoreV2.set(relayAdd)
            # Delete old entry
            await relayRemoveChannel(self, ctx, fromChannel, itemToEdit)

        # Test
        try:
            await fromChannel.send("**Channels are now linked!**\nThis is a sample message.")
        except Exception as err:
            logger.error(err)
            return await ctx.send("Setup was successfully completed, but some permissions may be missing.")
        await ctx.message.add_reaction("✅")

    @msgrelay.command(name="delete", aliases=["remove"])
    @commands.bot_has_permissions(add_reactions=True)
    async def mmmrdelete(self, ctx, fromChannel: discord.TextChannel, itemToDelete: int):
        """Delete a message relay
        
        fromChannel: the originating #channel
        itemToDelete: put 1, 2, ... for which one you want to delete. (check `[p]msgrelay settings` for list of relays from each channel)
        
        To delete all relays for a fromChannel, set itemToDelete to 0 (zero)."""
        result = await relayRemoveChannel(self, ctx, fromChannel, itemToDelete)
        if result == False:
            return await ctx.send("Deletion failed. Please report this error to the support server at <https://coffeebank.github.io/discord>.")
        await ctx.message.add_reaction("✅")

    @msgrelay.command(name="settimer")
    @commands.bot_has_permissions(add_reactions=True)
    async def mmmrsettimer(self, ctx, seconds: int):
        """Seconds after the relay checks for edited/deleted messages
        
        To disable checking for edits/deleted messages after sending, set seconds to 0 (zero)."""
        await self.config.guild(ctx.guild).relayTimer.set(seconds)
        await ctx.message.add_reaction("✅")



    # Listeners

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Ignore message if it's a webhook
        # REQUIRED IF TWO-WAY CHAT REDIRECTS
        # TO STOP A TWO-WAY REDIRECT, USE [p]msgrelay delete #channel
        if message.webhook_id:
            return

        # only do anything if message is sent in a guild
        if not message.guild:
            return

        # Retrieve webhook info from channel store
        relayStore = await self.config.guild(message.guild).msgrelayStoreV2()
        if not str(message.channel.id) in relayStore:
            return

        hookData = relayStore[str(message.channel.id)]
        relayTimer = await self.config.guild(message.guild).relayTimer()
        
        # Migrate data to multi-hook support if needed
        try:
            assert isinstance(hookData, list)
        except AssertionError:
            hookData = await fixMsgrelayStoreV2alpha(self, message)

        # Send along webhook for each in array
        try:
            async with aiohttp.ClientSession() as session:
                for wh in hookData:
                    configJson = relayGetData(wh)
                    webhook = Webhook.from_url(wh["toWebhook"], session=session)
                    whResult = await msgFormatter(self, webhook, message, configJson)
                    wh["whResult"] = whResult.id
                # Wait, then check for edits/deletes
                if relayTimer <= 0:
                    return
                else:
                    await asyncio.sleep(relayTimer)
                    try:
                        endMsg = await message.channel.fetch_message(message.id)
                    except discord.NotFound:
                        for wf in hookData:
                            configJson = relayGetData(wh)
                            webhook = Webhook.from_url(wf["toWebhook"], session=session)
                            await msgFormatter(self, webhook, message, configJson, deleteMsgId=wf.get("whResult", None))
                    else:
                        if endMsg.edited_at:
                            for wf in hookData:
                                configJson = relayGetData(wh)
                                webhook = Webhook.from_url(wf["toWebhook"], session=session)
                                await msgFormatter(self, webhook, endMsg, configJson, editMsgId=wf.get("whResult", None))
        finally:
            await session.close()
