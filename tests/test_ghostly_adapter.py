"""Behaviour tests for the Ghostly platform adapter against a fake ``ghostly`` CLI (no daemon, no network).

Run with ``tests/run`` from the repo root: it finds the Hermes Agent's Python and runs pytest with it.
``HERMES_AGENT_DIR`` points at the Hermes Agent checkout (default ``~/.hermes/hermes-agent``).
"""
import asyncio
import gc
import importlib.util
import json
import logging
import os
import stat
import sys
import time
import warnings
from pathlib import Path

import pytest

AGENT = Path(os.environ.get("HERMES_AGENT_DIR") or Path.home() / ".hermes" / "hermes-agent")
sys.path.insert(0, str(AGENT))

FAKE = r"""#!/usr/bin/env bash
# Fake ghostly CLI: logs argv (+stdin for --stdin) to $FAKE_LOG, answers from $FAKE_DIR.
log="$FAKE_LOG"; dir="$FAKE_DIR"
stdin=""
for a in "$@"; do [ "$a" = "--stdin" ] && stdin="$(cat)"; done
printf '%s\n' "$(printf '%s\x1f' "$@")STDIN=$stdin" >> "$log"
prof=""; if [ "$1" = "-p" ]; then prof="$2"; shift 2; fi
case "$1 $2" in
  "status "*) if [ -e "$dir/status-busy" ]; then echo 'Profile default is in use' >&2; exit 1; fi
     if [ -e "$dir/status-no-events" ]; then echo '{"profile":"default"}'; else echo '{"events":{"lastSeq":41}}'; fi;;
  "chat list") cat "$dir/chats.json";;
  "group show") cat "$dir/group.json";;
  "file save") shift 2; p=""; while [ $# -gt 0 ]; do [ "$1" = "--path" ] && p="$2"; shift; done; echo data > "$p"; echo '{"saved":true}';;
  "listen "*) if [ -f "$dir/listen-error" ]; then cat "$dir/listen-error" >&2; exit 1; fi
             if [ -n "$prof" ]; then cat "$dir/events-$prof.jsonl"; else cat "$dir/events.jsonl"; fi;;
  *) if [ "$1" = "send" ] && printf '%s' "$stdin" | grep -q SECRETSEED; then echo '{"error":{"code":"secret"}}'; exit 5; fi
     echo '{"messageId":"m1","delivery":"sent"}';;
esac
"""


@pytest.fixture
def env(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    fake = tmp_path / "ghostly"
    fake.write_text(FAKE)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("GHOSTLY_BIN", str(fake))
    monkeypatch.setenv("FAKE_LOG", str(tmp_path / "calls.log"))
    monkeypatch.setenv("FAKE_DIR", str(tmp_path))
    (tmp_path / "calls.log").write_text("")
    (tmp_path / "chats.json").write_text(json.dumps({"chats": [
        {"id": "newdm", "label": "stranger", "peerName": "Someone"}]}))
    (tmp_path / "group.json").write_text(json.dumps({"id": "G2", "name": "New Group", "members": [
        {"key": "me", "me": True}, {"key": "OWNERKEY", "name": "Ana Laptop"}, {"key": "BOTKEY", "name": "Bot"}]}))
    spec = importlib.util.spec_from_file_location("ghostly_adapter_under_test", Path(__file__).parents[1] / "adapter.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Register the platform the way the gateway's plugin loader does, so Platform("ghostly") resolves.
    from gateway.platform_registry import PlatformEntry, platform_registry
    platform_registry.register(PlatformEntry(
        name="ghostly", label="Ghostly", adapter_factory=mod.GhostlyAdapter, check_fn=mod.check_requirements,
        source="plugin", plugin_name="ghostly-platform"))
    chats = home / "ghostly-platform" / "chats.json"
    chats.parent.mkdir(parents=True)
    chats.write_text(json.dumps({
        "OWNERDM": {"label": "owner", "name": "Ana", "role": "owner"},
        "PEERDM": {"label": "peer", "name": "the peer agent", "role": "agent_peer"},
        "group:G1": {"label": "dev", "name": "the dev group", "role": "group", "mentions_only": True,
                     "members": {"OWNERKEY": {"name": "Ana", "owner": True}, "BOTKEY": {"name": "Peer"}}},
    }))
    return mod, tmp_path, chats


def make_adapter(mod, inbound=True, **extra):
    from gateway.config import PlatformConfig
    extra = {"inbound": inbound, "owner_name": "Ana", **extra}
    adapter = mod.GhostlyAdapter(PlatformConfig(enabled=True, extra=extra))
    captured = []

    async def fake_handle(event):
        captured.append(event)

    adapter.handle_message = fake_handle
    adapter._enqueue_text_event = lambda event: captured.append(event)
    adapter._load_chats()
    return adapter, captured


def calls(tmp):
    return [line.split("\x1f") for line in (tmp / "calls.log").read_text().splitlines() if line]


def dm(chat, text, **extra):
    return {"seq": 50, "id": f"message.received:{chat}:{text}", "type": "message.received", "chat": chat,
            "message": {"id": "p1", "from": "peer", "text": text, "timestamp": 1790000000000, **extra}}


def grp(gid, member, text, **extra):
    return {"seq": 51, "id": f"group.message:{gid}:{member}:{text}", "type": "group.message", "group": gid,
            "message": {"id": "g1", "from": "peer", "member": member, "text": text, "timestamp": 1790000000000, **extra}}


def run(coro):
    """Run one step on a fresh loop. What the step left running is settled before the loop closes: the 👀 workers are
    awaited (their reacts belong to this step's calls log) and any other task is cancelled, as asyncio.run does. A task
    left pending on a closed loop is closed later by the garbage collector, in the middle of another test."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        try:
            _settle(loop)
        finally:
            loop.close()


def _settle(loop):
    def react_workers():
        return [t for t in asyncio.all_tasks(loop)
                if getattr(t.get_coro(), "__qualname__", "") == "GhostlyAdapter._react_worker"]

    while workers := react_workers():
        loop.run_until_complete(asyncio.gather(*workers, return_exceptions=True))
    rest = asyncio.all_tasks(loop)
    for task in rest:
        task.cancel()
    if rest:
        loop.run_until_complete(asyncio.gather(*rest, return_exceptions=True))
    loop.run_until_complete(loop.shutdown_asyncgens())


def test_allowlist_file_feeds_gateway_authorization(env):
    mod, _, _ = env
    adapter, _ = make_adapter(mod)
    assert set(adapter.config.extra["allow_from"]) == {"OWNERDM", "PEERDM"}
    assert adapter.config.extra["group_allowed_chats"] == ["group:G1"]


def test_owner_dm_has_full_control_and_peer_is_a_guarded_bot(env):
    mod, _, _ = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("OWNERDM", "hi")))
    run(adapter._handle_event(dm("PEERDM", "status?")))
    owner, peer = captured
    assert owner.source.user_id == "OWNERDM" and owner.source.chat_type == "dm"
    assert owner.allow_gateway_control is True and not owner.source.is_bot and owner.channel_prompt is None
    assert peer.allow_gateway_control is False and peer.source.is_bot
    assert "NO_REPLY" in peer.channel_prompt and "never orders from Ana" in peer.channel_prompt


def test_own_messages_events_and_duplicates_are_dropped(env):
    mod, _, _ = env
    adapter, captured = make_adapter(mod)
    mine = dm("OWNERDM", "x")
    mine["message"]["from"] = "me"
    sys_evt = dm("OWNERDM", "y", event="joined")
    twice = dm("OWNERDM", "z")
    for e in (mine, sys_evt, twice, twice):
        run(adapter._handle_event(e))
    assert [e.text for e in captured] == ["z"]


def test_group_is_one_shared_session_with_author_prefix_and_mentions_gate(env):
    mod, _, _ = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(grp("G1", "BOTKEY", "talking among themselves")))      # no mention: ignored
    run(adapter._handle_event(grp("G1", "BOTKEY", "@Hermes rebase?", mentioned=True)))
    run(adapter._handle_event(grp("G1", "OWNERKEY", "go on")))                    # owner always wakes
    coord, owner = captured
    assert coord.source.chat_id == "group:G1" and coord.source.user_id is None
    assert coord.text.startswith("[Peer] ") and coord.allow_gateway_control is False
    assert owner.text == "[Ana] go on" and owner.allow_gateway_control is True


def late_mention(gid, member, text, **extra):
    """``group.mentioned`` (CLI 1.11): a mention of this profile learned after the message's ``group.message``."""
    event = grp(gid, member, text, mentioned=True, **extra)
    return {**event, "seq": 52, "id": f"group.mentioned:{gid}:g1", "type": "group.mentioned", "messageId": "g1"}


def test_a_mention_learned_after_the_message_starts_the_turn_the_mentions_gate_dropped(env):
    """The copy that reached us first named no one (dropped in a mentions_only group); the author's copy, or an edit,
    names me later and arrives as ``group.mentioned``: that is the turn, once."""
    mod, tmp, _ = env
    (tmp / "events.jsonl").write_text(
        json.dumps(grp("G1", "BOTKEY", "@Hermes rebase?")) + "\n"
        + json.dumps(late_mention("G1", "BOTKEY", "@Hermes rebase?")) + "\n")
    adapter, captured = make_adapter(mod)
    _one_listen_round(adapter, captured)
    listen = next(c for c in calls(tmp) if c[0] == "listen")
    assert "group.mentioned" in [listen[i + 1] for i, a in enumerate(listen) if a == "--type"]
    assert [e.text for e in captured] == ["[Peer] @Hermes rebase?"]
    turn = captured[0]
    assert turn.source.chat_id == "group:G1" and turn.reply_expected is True and turn.allow_gateway_control is False
    assert adapter._read_cursor() == 52


def test_a_late_mention_never_doubles_a_turn_the_message_already_started(env):
    """The owner and a delegate are read without a mention, and a group without mentions_only reads everyone: their
    ``group.message`` was the turn already."""
    mod, tmp, chats = env
    data = json.loads(chats.read_text())
    data["group:G1"]["members"]["DELEGATEKEY"] = {"name": "Deputy", "delegate": True}
    data["group:G3"] = {"label": "all", "name": "the open group", "role": "group", "members": {}}
    chats.write_text(json.dumps(data))
    adapter, captured = make_adapter(mod)
    read_group(adapter, grp("G1", "OWNERKEY", "and you?"), late_mention("G1", "OWNERKEY", "and you, @Hermes?"),
               grp("G1", "DELEGATEKEY", "go on"), late_mention("G1", "DELEGATEKEY", "go on @Hermes"),
               grp("G3", "BOTKEY", "anyone?"), late_mention("G3", "BOTKEY", "anyone? @Hermes"))
    assert [e.text for e in captured] == ["[Ana] and you?", "[Deputy] go on", "[BOTKEY] anyone?"]
    assert len(reacts(tmp)) == 1                    # the owner's message, once


def test_a_message_the_mentions_gate_drops_is_logged_without_its_text(env, caplog):
    import logging
    mod, _, _ = env
    adapter, captured = make_adapter(mod)
    with caplog.at_level(logging.INFO, logger=mod.logger.name):
        run(adapter._handle_event(grp("G1", "BOTKEY", "talking among themselves")))
    assert captured == []
    dropped = [r.getMessage() for r in caplog.records if "mention" in r.getMessage()]
    assert len(dropped) == 1 and "group:G1" in dropped[0] and "g1" in dropped[0] and "BOTKEY"[:8] in dropped[0]
    assert "talking among themselves" not in caplog.text


def test_owner_messages_get_a_seen_reaction_right_away_and_others_do_not(env):
    """The owner gets a 👀 on each message the moment it is read: their DM, and all they write in a group."""
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)

    async def read_all():
        await adapter._handle_event(dm("OWNERDM", "hi"))
        await adapter._handle_event(grp("G1", "OWNERKEY", "@Hermes go on", mentioned=True))
        await adapter._handle_event(grp("G1", "OWNERKEY", "talking to another agent"))
        await adapter._handle_event(dm("PEERDM", "status?"))
        await adapter._handle_event(grp("G1", "BOTKEY", "@Hermes rebase?", mentioned=True))
        await asyncio.gather(*adapter._seen_tasks)

    run(read_all())
    assert sorted(reacts(tmp)) == sorted([["react", "OWNERDM", "p1", mod.SEEN_EMOJI],
                                          ["group", "react", "G1", "g1", mod.SEEN_EMOJI],
                                          ["group", "react", "G1", "g1", mod.SEEN_EMOJI]])   # they run concurrently
    assert len(captured) == 5          # the reaction never stands in for handling the message


def reacts(tmp):
    return [c[:-1] for c in calls(tmp) if "react" in c[:2]]


SLOW_REACT = r"""#!/usr/bin/env bash
# Wraps the fake CLI: a react stays alive $FAKE_REACT_SECONDS and notes how many reacts are alive with it; a message
# id with "bad" in it fails.
case " $* " in
  *" react "*)
    live="$FAKE_DIR/live"; mkdir -p "$live"; touch "$live/$$"
    ls "$live" | wc -l | tr -d ' ' >> "$FAKE_DIR/react-peaks"
    sleep "$FAKE_REACT_SECONDS"; rm -f "$live/$$"
    case " $* " in *" bad"*) echo '{"error":{"code":"offline"}}'; exit 1;; esac;;
esac
exec "$FAKE_REAL" "$@"
"""


def slow_reacts(tmp, monkeypatch, seconds):
    wrapper = tmp / "ghostly-slow-react"
    wrapper.write_text(SLOW_REACT)
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("FAKE_REAL", os.environ["GHOSTLY_BIN"])
    monkeypatch.setenv("GHOSTLY_BIN", str(wrapper))
    monkeypatch.setenv("FAKE_REACT_SECONDS", str(seconds))


def owner_dm(n):
    event = dm("OWNERDM", f"message {n}")
    event["message"]["id"] = f"m{n}"
    return event


async def reacts_done(adapter):
    while adapter._seen_tasks:
        await asyncio.gather(*list(adapter._seen_tasks), return_exceptions=True)


def test_a_burst_of_owner_messages_runs_a_few_reacts_at_a_time_and_skips_the_oldest(env, monkeypatch):
    """40 owner messages at once (a catch-up after a restart): every turn starts right away, at most
    SEEN_REACTS_AT_ONCE react processes are alive at any moment, and the newest messages still get their 👀."""
    mod, tmp, _ = env
    slow_reacts(tmp, monkeypatch, 0.3)
    adapter, captured = make_adapter(mod)

    async def burst():
        for n in range(40):
            await adapter._handle_event(owner_dm(n))
        assert len(captured) == 40                      # no turn waited on a reaction
        await reacts_done(adapter)

    run(burst())
    peak = max(int(n) for n in (tmp / "react-peaks").read_text().split())
    reacted = [c[2] for c in reacts(tmp)]
    assert peak <= mod.SEEN_REACTS_AT_ONCE
    assert len(reacted) <= mod.SEEN_REACTS_AT_ONCE + mod.SEEN_REACTS_QUEUED
    assert set(reacted) >= {f"m{n}" for n in range(40 - mod.SEEN_REACTS_QUEUED, 40)}
    assert not adapter._seen_pending and not adapter._seen_tasks


def test_a_few_owner_messages_all_get_a_seen_reaction_and_a_failed_react_breaks_nothing(env, monkeypatch):
    """A react the CLI refuses, or one that raises, is logged; the reacts behind it and every turn still go on."""
    mod, tmp, _ = env
    slow_reacts(tmp, monkeypatch, 0.05)
    monkeypatch.setattr(mod, "SEEN_REACTS_AT_ONCE", 1)     # one worker: a failure that killed it would strand the rest
    real_cli = mod._cli

    async def cli(args, *a, **kw):
        if "boom" in args:
            raise OSError("the CLI vanished")
        return await real_cli(args, *a, **kw)

    monkeypatch.setattr(mod, "_cli", cli)
    adapter, captured = make_adapter(mod)
    ids = ["boom", "bad1", "ok1", "bad2", "ok2"]

    async def read_all():
        for n, mid in enumerate(ids):
            event = owner_dm(n)
            event["message"]["id"] = mid
            await adapter._handle_event(event)
        await reacts_done(adapter)

    run(read_all())
    assert len(captured) == len(ids)
    assert [c[2] for c in reacts(tmp)] == ["ok1", "ok2"]   # the refused ones exit before the fake logs them
    assert len((tmp / "react-peaks").read_text().split()) == 4
    assert not adapter._seen_pending and not adapter._seen_tasks


def test_a_seen_reaction_left_queued_by_a_step_never_lands_in_a_later_tests_calls(env, monkeypatch):
    """Most steps read owner messages and return without awaiting their 👀, leaving the react workers to run(). Those
    must be done before its loop closes. A worker still waiting there stays alive until the garbage collector closes
    it, at a random point of a later test; that close is caught as an error and the worker runs the next queued react
    in whatever loop is running then, with that test's FAKE_LOG."""
    mod, tmp, _ = env
    monkeypatch.setattr(mod, "SEEN_REACTS_AT_ONCE", 1)      # one worker, so reacts are still queued behind it
    real_cli = mod._cli
    started = []                                            # (FAKE_LOG, message id) of each react CLI started

    async def cli(args, *a, **kw):
        started.append((os.environ["FAKE_LOG"], args[-2]))
        return await real_cli(args, *a, **kw)

    monkeypatch.setattr(mod, "_cli", cli)
    adapter, _ = make_adapter(mod)

    async def read_all():
        for n in range(3):
            await adapter._handle_event(owner_dm(n))

    # pytest keeps a test's warnings, and what they point at, until the test ends: that is what kept a leaked worker
    # alive into the next test. Here the step's warnings are let go at once, as at the end of a test.
    with warnings.catch_warnings(record=True):
        run(read_all())
    later = tmp / "later-test-calls.log"
    later.write_text("")
    monkeypatch.setenv("FAKE_LOG", str(later))              # what the next test's env fixture does
    del adapter, read_all                                   # and the step's references end with it

    async def a_later_test():
        gc.collect()
        await asyncio.sleep(0.3)

    run(a_later_test())
    assert [mid for log, mid in started if log == str(later)] == []    # the CLI is killed with its transport, mostly
    assert sorted(c[2] for c in reacts(tmp)) == ["m0", "m1", "m2"]     # each one ran in the step that queued it
    assert later.read_text() == ""


def list_owner_keys(tmp, *keys, mtime=None):
    """Write owner_keys.txt where the adapter reads it by default (<HERMES_HOME>/ghostly-platform)."""
    path = tmp / ".hermes" / "ghostly-platform" / "owner_keys.txt"
    path.parent.mkdir(exist_ok=True)
    path.write_text("# The owner's member keys in groups, one per line.\n\n" + "".join(f"{k}\n" for k in keys))
    if mtime:
        os.utime(path, (mtime, mtime))


def read_group(adapter, *events):
    async def read_all():
        for event in events:
            await adapter._handle_event(event)
        await asyncio.gather(*adapter._seen_tasks)

    run(read_all())


def test_a_listed_owner_key_is_read_without_a_mention_and_gets_a_seen_reaction(env):
    """The owner writes in a group from two devices; the second key is only in owner_keys.txt."""
    mod, tmp, _ = env
    list_owner_keys(tmp, "OWNERKEY", "PHONEKEY")
    adapter, captured = make_adapter(mod)
    read_group(adapter, grp("G1", "PHONEKEY", "talking to another agent", nick="Ana Laptop"))
    assert [e.text for e in captured] == ["[Ana] talking to another agent"]
    assert captured[0].allow_gateway_control is True        # their message, their authority
    assert captured[0].reply_expected is False              # read and 👀; a reply only when it is for me
    assert reacts(tmp) == [["group", "react", "G1", "g1", mod.SEEN_EMOJI]]


def test_other_members_still_need_a_mention_when_the_owner_keys_are_listed(env):
    mod, tmp, _ = env
    list_owner_keys(tmp, "OWNERKEY", "PHONEKEY")
    adapter, captured = make_adapter(mod)
    read_group(adapter, grp("G1", "BOTKEY", "talking among themselves"), grp("G1", "STRANGERKEY", "hi"))
    assert captured == [] and reacts(tmp) == []
    read_group(adapter, grp("G1", "BOTKEY", "@Hermes rebase?", mentioned=True))
    assert [e.text for e in captured] == ["[Peer] @Hermes rebase?"] and captured[0].allow_gateway_control is False
    assert reacts(tmp) == []


def test_a_key_added_to_the_owner_keys_file_counts_at_the_next_message(env):
    """No restart: the list is read again when the file changes (and when it first appears)."""
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    read_group(adapter, grp("G1", "PHONEKEY", "before the file"))           # no file yet
    assert captured == [] and reacts(tmp) == []
    list_owner_keys(tmp, "OWNERKEY", mtime=1_900_000_000)
    read_group(adapter, grp("G1", "PHONEKEY", "still not listed"))
    assert captured == [] and reacts(tmp) == []
    list_owner_keys(tmp, "OWNERKEY", "PHONEKEY", mtime=1_900_000_100)
    read_group(adapter, grp("G1", "PHONEKEY", "listed now"))
    assert [e.text for e in captured] == ["[Ana] listed now"]
    assert reacts(tmp) == [["group", "react", "G1", "g1", mod.SEEN_EMOJI]]
    list_owner_keys(tmp, "OWNERKEY", mtime=1_900_000_200)                    # taken off the list again
    read_group(adapter, grp("G1", "PHONEKEY", "off the list again"))
    assert len(captured) == 1


def test_a_listed_owner_key_may_answer_an_approval_in_the_group(env, monkeypatch):
    mod, tmp, _ = env
    list_owner_keys(tmp, "PHONEKEY")
    adapter, _ = make_adapter(mod)
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval", lambda key, choice, **kw: resolved.append(choice) or 1)
    run(adapter._send_exec_approval_prompt(_approval_prompt("group:G1")))
    run(adapter._handle_event({"type": "button.pressed", "group": "G1", "messageId": "m1", "button": "once", "by": "BOTKEY"}))
    assert resolved == []
    run(adapter._handle_event({"type": "button.pressed", "group": "G1", "messageId": "m1", "button": "once", "by": "PHONEKEY"}))
    assert resolved == ["once"]


def test_edits_replace_the_sent_message_and_too_long_or_empty_ones_ask_for_a_new_send(env):
    """edit/1: the gateway's progress and approval notes change in place instead of piling up."""
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    sent = run(adapter.send("OWNERDM", "working..."))
    assert run(adapter.edit_message("OWNERDM", sent.message_id, "done")).success
    assert run(adapter.edit_message("group:G1", "g9", "fixed")).success
    assert not run(adapter.edit_message("OWNERDM", sent.message_id, "x" * (mod.MAX_MESSAGE_LENGTH + 1))).success
    assert not run(adapter.edit_message("OWNERDM", "", "no id")).success
    edits = [c for c in calls(tmp) if "edit" in c[:2]]
    assert [c[:-1] for c in edits] == [["edit", "OWNERDM", "m1", "--stdin"], ["group", "edit", "G1", "g9", "--stdin"]]
    assert [c[-1] for c in edits] == ["STDIN=done", "STDIN=fixed"]


def test_lifecycle_statuses_stay_out_of_groups_and_peers_and_edit_one_bubble_for_the_owner(env):
    """"⏳ Still waiting for the other Hermes process…" every 15 s would pile up in a group: groups and peers get no
    status at all, the owner's DM gets one message per status kind, edited in place."""
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    for s in (1, 2, 3):
        assert run(adapter.send_or_update_status("group:G1", "lifecycle", f"⏳ Still waiting ({s * 15}s)...")).success
        assert run(adapter.send_or_update_status("PEERDM", "lifecycle", f"⏳ Still waiting ({s * 15}s)...")).success
    assert calls(tmp) == []
    for s in (1, 2, 3):
        assert run(adapter.send_or_update_status("OWNERDM", "lifecycle", f"⏳ Still waiting ({s * 15}s)...")).success
    sent = [c for c in calls(tmp) if c[:2] == ["send", "OWNERDM"]]
    edits = [c for c in calls(tmp) if c[:1] == ["edit"]]
    assert len(sent) == 1 and len(edits) == 2
    assert edits[-1][-1] == "STDIN=⏳ Still waiting (45s)..."


def test_lifecycle_lines_sent_as_ordinary_messages_stay_out_of_groups_and_peers(env):
    """A gateway from before ``send_or_update_status`` sends each lifecycle line through ``send``, so "⏳ Still
    waiting…", "⚡ Interrupting…" and "✅ Background task finished" would land in a group. A group and a peer get none;
    the owner's DM keeps them; a real reply still goes."""
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    lines = [
        "⏳ Another Hermes process is using this session; waiting for it to finish before starting your turn...",
        "⏳ Still waiting for the other Hermes process on this session (15s)...",
        "Stopped waiting for another Hermes process on this session. Your message was not processed.",
        "⚡ Interrupting current task (4 min elapsed, iteration 0/500). I'll respond to your message shortly.",
        "⚠️ I seem to be stuck (no activity for 5 min). Send /stop to cancel the current task.",
        "✅ Background task finished — `npm run build`",
    ]
    for line in lines:
        assert run(adapter.send("group:G1", line)).success
        assert run(adapter.send("PEERDM", line)).success
    assert calls(tmp) == []
    assert run(adapter.send("group:G1", "Finished the fix: the test passes.")).success
    assert [c[:2] for c in calls(tmp)] == [["group", "send"]]
    assert run(adapter.send("OWNERDM", lines[1])).success
    assert [c[:2] for c in calls(tmp)][-1] == ["send", "OWNERDM"]


def test_unknown_chats_are_auto_added_with_restricted_roles(env):
    mod, _, chats = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("newdm", "hello")))
    run(adapter._handle_event(grp("G2", "OWNERKEY", "hi group")))
    saved = json.loads(chats.read_text())
    assert saved["newdm"]["role"] == "contact" and saved["newdm"]["auto"] is True
    assert saved["group:G2"]["role"] == "group"
    assert saved["group:G2"]["members"]["OWNERKEY"].get("owner") is True      # owner key known from G1
    assert "owner" not in saved["group:G2"]["members"]["BOTKEY"]
    assert "newdm" in adapter.config.extra["allow_from"]
    assert "group:G2" in adapter.config.extra["group_allowed_chats"]
    contact = captured[0]
    assert contact.allow_gateway_control is False and "NOT Ana" in contact.channel_prompt


def test_unresolvable_chat_is_ignored(env):
    mod, _, chats = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("ghost", "hi")))
    assert captured == [] and "ghost" not in json.loads(chats.read_text())


def test_allowlist_edits_are_picked_up_without_restart(env):
    mod, _, chats = env
    adapter, captured = make_adapter(mod)
    data = json.loads(chats.read_text())
    data["PEERDM"]["role"] = "owner"
    chats.write_text(json.dumps(data))
    os.utime(chats, (1, 2_000_000_000))
    run(adapter._handle_event(dm("PEERDM", "now I am the owner")))
    assert captured[0].allow_gateway_control is True


def test_an_allowlist_being_edited_is_never_written_over(env):
    mod, _, chats = env
    adapter, captured = make_adapter(mod)
    for n, broken in enumerate([
            chats.read_text()[:-1] + ', "FRIENDDM": {"label": "friend", "role": "contact"},}',  # trailing comma
            json.dumps([{"OWNERDM": {"role": "owner"}}])]):                                        # not an object
        chats.write_text(broken)
        os.utime(chats, (1, 2_000_000_000 + n))
        run(adapter._handle_event(dm("newdm", f"hello {n}")))
        assert chats.read_text() == broken
    # The previous allowlist stays in use, the new chat too; once the file reads again its next message saves it.
    assert [e.text for e in captured] == ["hello 0", "hello 1"]
    assert {"OWNERDM", "newdm"} <= set(adapter.config.extra["allow_from"])
    chats.write_text(json.dumps({"OWNERDM": {"label": "owner", "name": "Ana", "role": "owner"}}))
    os.utime(chats, (1, 2_000_000_100))
    run(adapter._handle_event(dm("newdm", "hello again")))
    assert set(json.loads(chats.read_text())) == {"OWNERDM", "newdm"}


def test_received_file_is_saved_and_typed(env):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("OWNERDM", "", file={"id": "F1", "name": "a.png", "mime": "image/png", "size": 5})))
    event = captured[0]
    assert event.message_type.value == "photo" and Path(event.media_urls[0]).read_text() == "data\n"


def _media(adapter, name, age_days):
    adapter.media_dir.mkdir(parents=True, exist_ok=True)
    path = adapter.media_dir / name
    path.write_text("x")
    old = time.time() - age_days * 86400
    os.utime(path, (old, old))
    return path


def test_received_files_older_than_the_bound_are_pruned_at_connect_and_on_save(env):
    mod, _, _ = env
    adapter, _ = make_adapter(mod, inbound=False)
    stale, fresh = _media(adapter, "1-old-a.webm", 8), _media(adapter, "2-new-b.png", 1)
    assert run(adapter.connect()) is True
    assert not stale.exists() and fresh.exists()
    # On a save, at most once every few hours.
    stale = _media(adapter, "3-old-c.webm", 8)
    run(adapter._save_file({"id": "F2", "name": "d.png"}, "OWNERDM"))
    assert stale.exists()
    adapter._media_pruned_at -= mod.MEDIA_PRUNE_INTERVAL_SECONDS
    run(adapter._save_file({"id": "F3", "name": "e.png"}, "OWNERDM"))
    assert not stale.exists() and fresh.exists() and len(list(adapter.media_dir.iterdir())) == 3



def test_a_media_path_that_is_not_a_folder_never_fails_connect(env):
    mod, _, _ = env
    adapter, _ = make_adapter(mod, inbound=False)
    adapter.media_dir.parent.mkdir(parents=True, exist_ok=True)
    adapter.media_dir.write_text("not a folder")
    assert run(adapter.connect()) is True

def test_media_max_age_days_sets_the_bound_and_zero_keeps_everything(env):
    mod, _, _ = env
    adapter, _ = make_adapter(mod, inbound=False, media_max_age_days=0)
    ancient = _media(adapter, "1-old-a.webm", 400)
    run(adapter.connect())
    assert ancient.exists()
    adapter, _ = make_adapter(mod, inbound=False, media_max_age_days=2)
    week, day = _media(adapter, "2-old-b.webm", 3), _media(adapter, "3-new-c.webm", 1)
    run(adapter.connect())
    assert not ancient.exists() and not week.exists() and day.exists()


def test_send_routes_dm_and_group_and_chunks_long_text(env):
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    assert run(adapter.send("OWNERDM", "hi")).success
    assert run(adapter.send("group:G1", "x" * 8000)).success
    log = calls(tmp)
    assert log[0][:2] == ["send", "OWNERDM"]
    group_sends = [c for c in log if c[:3] == ["group", "send", "G1"]]
    assert len(group_sends) == 3 and all(len(c[-1]) - len("STDIN=") <= mod.MAX_MESSAGE_LENGTH for c in group_sends)


def test_secret_guard_refusal_is_a_failure_and_warns_the_chat(env):
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    result = run(adapter.send("OWNERDM", "here SECRETSEED"))
    assert not result.success and "secret guard" in result.error
    assert any("blocked" in c[-1] for c in calls(tmp))


def test_files_go_out_natively_in_dms_only(env):
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    f = tmp / "r.txt"
    f.write_text("r")
    assert run(adapter.send_document("OWNERDM", str(f), caption="go on")).success
    assert any(c[:3] == ["file", "send", "OWNERDM"] for c in calls(tmp))
    assert not run(mod._deliver("group:G1", "", [str(f)])).get("success")


def test_typing_is_throttled_and_skipped_for_groups(env):
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    for _ in range(3):
        run(adapter.send_typing("OWNERDM"))
    run(adapter.send_typing("group:G1"))
    run(adapter.stop_typing("OWNERDM"))
    typing = [c for c in calls(tmp) if c[0] == "typing"]
    assert [c[1:3] for c in typing] == [["OWNERDM", "--kind"], ["OWNERDM", "--stop"]]


def test_inbound_off_by_default_never_listens(env):
    mod, tmp, _ = env
    from gateway.config import PlatformConfig
    adapter = mod.GhostlyAdapter(PlatformConfig(enabled=True, extra={}))
    assert run(adapter.connect()) is True
    assert adapter.inbound is False and adapter._listen_task is None
    assert not any(c[0] == "listen" for c in calls(tmp))


def test_home_chat_given_as_a_label_warns_at_start_with_the_id(env, monkeypatch, caplog):
    # Hermes mirrors a cron delivery into the session whose origin chat_id is the target as given: a label never matches.
    mod, _, _ = env
    monkeypatch.setenv("GHOSTLY_HOME_CHANNEL", "owner")
    caplog.set_level("WARNING")
    assert run(make_adapter(mod, inbound=False)[0].connect()) is True
    assert [r.getMessage() for r in caplog.records if "home chat" in r.getMessage()] == [
        "[ghostly] home chat owner is a label: cron reports won't reach its session; set GHOSTLY_HOME_CHANNEL=OWNERDM"]
    caplog.clear()
    run(make_adapter(mod, inbound=False)[0].connect(is_reconnect=True))   # a reconnect's fresh adapter says it once only
    monkeypatch.setenv("GHOSTLY_HOME_CHANNEL", "OWNERDM")
    run(make_adapter(mod, inbound=False)[0].connect())
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_delegate_speaks_for_the_owner_but_keeps_the_exceptions_in_the_prompt(env):
    mod, _, chats = env
    data = json.loads(chats.read_text())
    data["PEERDM"]["delegate"] = True
    data["group:G1"]["members"]["BOTKEY"]["delegate"] = True
    chats.write_text(json.dumps(data))
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("PEERDM", "restart service X")))
    run(adapter._handle_event(grp("G1", "BOTKEY", "go on with the rebase")))   # a delegate wakes the group without @
    direct, group = captured
    assert direct.allow_gateway_control is True and direct.source.is_bot      # the loop guard still applies
    assert "real money" in direct.channel_prompt and "Ana's DIRECT ok" in direct.channel_prompt
    assert group.allow_gateway_control is True and "Peer speaks for Ana" in group.channel_prompt


def test_first_listen_starts_at_the_current_seq_and_advances_the_cursor(env):
    mod, tmp, _ = env
    (tmp / "events.jsonl").write_text(json.dumps(dm("OWNERDM", "new") | {"seq": 42}) + "\n")
    adapter, captured = make_adapter(mod)
    adapter._running = True

    async def one_round():
        task = asyncio.create_task(adapter._listen_loop())
        for _ in range(100):
            await asyncio.sleep(0.05)
            if captured:
                break
        adapter._running = False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    run(one_round())
    listen = next(c for c in calls(tmp) if c[0] == "listen")
    assert listen[listen.index("--since") + 1] == "41"
    assert adapter._read_cursor() == 42 and captured[0].text == "new"


def test_a_failing_listen_logs_the_clis_reason_and_retries_at_most_every_retry_max(env, monkeypatch, caplog):
    """While the daemon is down every ``listen`` fails: the log says why, and the retries stay frequent enough that the
    owner's first message after the daemon is back is read within RETRY_MAX, not a minute and a half later."""
    import logging
    mod, tmp, _ = env
    (tmp / "listen-error").write_text("starting\ndaemon: socket closed (ECONNREFUSED)\n\n")
    adapter, _ = make_adapter(mod)
    adapter._running = True
    monkeypatch.setattr(mod.random, "uniform", lambda a, b: b)      # the worst jitter every time
    real_sleep, waits = asyncio.sleep, []

    async def fake_sleep(seconds, *args):
        waits.append(seconds)
        if len(waits) >= 10:  # a long outage
            adapter._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    with caplog.at_level(logging.INFO):
        run(asyncio.wait_for(adapter._listen_loop(), 30))
    exits = [r.getMessage() for r in caplog.records if "listen exited" in r.getMessage()]
    assert len(exits) == 10
    assert all(m == "[ghostly] listen exited with 1: daemon: socket closed (ECONNREFUSED)" for m in exits)
    assert waits[0] < mod.RETRY_MAX and max(waits) == mod.RETRY_MAX <= 15


@pytest.mark.parametrize("trouble", ["status-busy", "status-no-events"])
def test_first_listen_waits_for_the_current_seq_instead_of_replaying_the_journal(env, monkeypatch, trouble):
    """With no cursor, a ``status`` that fails ("Profile default is in use" while the daemon starts) or has no
    ``events.lastSeq`` must not become ``--since 0``: no cursor is written and nothing listens until it answers."""
    mod, tmp, _ = env
    monkeypatch.setattr(mod, "RETRY_INITIAL", 0.05)
    (tmp / trouble).write_text("")
    (tmp / "events.jsonl").write_text(json.dumps(dm("OWNERDM", "new") | {"seq": 42}) + "\n")
    adapter, captured = make_adapter(mod)
    adapter._running = True

    async def rounds():
        task = asyncio.create_task(adapter._listen_loop())
        for _ in range(100):
            await asyncio.sleep(0.05)
            if sum(c[0] == "status" for c in calls(tmp)) >= 2:
                break
        assert not adapter._cursor_path().exists()
        assert not any("listen" in c for c in calls(tmp)) and captured == []
        (tmp / trouble).unlink()                       # the daemon is up now
        for _ in range(100):
            await asyncio.sleep(0.05)
            if captured:
                break
        adapter._running = False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    run(rounds())
    assert sum(c[0] == "status" for c in calls(tmp)) >= 3
    listen = next(c for c in calls(tmp) if c[0] == "listen")
    assert listen[listen.index("--since") + 1] == "41"
    assert adapter._read_cursor() == 42 and captured[0].text == "new"


def test_a_file_being_saved_holds_only_its_own_chat(env):
    """A received file can take minutes to save: the other chats' messages go on meanwhile, the same chat's next
    messages keep their order behind it, and the cursor stays before the file until it is saved."""
    mod, tmp, _ = env
    fake = tmp / "ghostly"                         # `file save` waits until the test creates <tmp>/release
    fake.write_text(fake.read_text().replace(
        '"file save") shift 2;', '"file save") while [ ! -e "$dir/release" ]; do sleep 0.05; done; shift 2;'))
    events = [dm("PEERDM", "", file={"id": "F1", "name": "big.bin"}) | {"seq": 60},
              dm("PEERDM", "did it arrive?") | {"seq": 61},
              dm("OWNERDM", "are you there?") | {"seq": 62}]
    (tmp / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    adapter, captured = make_adapter(mod)
    adapter._running = True

    async def until(done):
        for _ in range(100):
            await asyncio.sleep(0.05)
            if done():
                break

    async def scenario():
        task = asyncio.create_task(adapter._listen_loop())
        await until(lambda: captured)
        while_saving = ([e.source.chat_id for e in captured], adapter._read_cursor())
        (tmp / "release").write_text("")
        await until(lambda: len(captured) == 3)
        await asyncio.sleep(0.05)
        adapter._running = False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return while_saving

    assert run(scenario()) == (["OWNERDM"], 59)
    assert [(e.source.chat_id, bool(e.media_urls), e.text) for e in captured[1:]] == [
        ("PEERDM", True, ""), ("PEERDM", False, "did it arrive?")]
    assert adapter._read_cursor() == 62


def test_a_restart_while_a_file_saves_does_not_replay_the_messages_already_handled(env):
    """The cursor stays before a file being saved, so a restart reads it again; the other chats' messages handled
    after it are written next to the cursor and skipped on that replay instead of becoming a second turn."""
    mod, tmp, _ = env
    fake = tmp / "ghostly"                         # `file save` waits until the test creates <tmp>/release
    fake.write_text(fake.read_text().replace(
        '"file save") shift 2;', '"file save") while [ ! -e "$dir/release" ]; do sleep 0.05; done; shift 2;'))
    events = [dm("PEERDM", "", file={"id": "F1", "name": "big.bin"}) | {"seq": 60},
              dm("OWNERDM", "deploy it now") | {"seq": 61}]
    (tmp / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    captured = []

    async def until(done):
        for _ in range(100):
            await asyncio.sleep(0.05)
            if done():
                break

    async def scenario():
        first, seen = make_adapter(mod)
        first._running = True
        first._listen_task = asyncio.create_task(first._listen_loop())
        await until(lambda: seen)
        await first.disconnect()                   # the gateway restarts while the file is still being saved
        captured.extend(seen)
        cursor = first._read_cursor()
        second, seen = make_adapter(mod)
        second._running = True
        (tmp / "release").write_text("")
        second._listen_task = asyncio.create_task(second._listen_loop())   # the fake replays 60 and 61 again
        await until(lambda: seen)
        await asyncio.sleep(0.2)
        await second.disconnect()
        captured.extend(seen)
        return cursor, second._read_cursor()

    assert run(scenario()) == (59, 61)
    assert [(e.source.chat_id, e.text) for e in captured] == [("OWNERDM", "deploy it now"), ("PEERDM", "")]


def test_gateway_notices_never_reach_a_ghostly_chat_and_the_owner_gets_a_short_line(env):
    """The gateway's silence warning (the one the gateway really produces, in any catalog language) and its other
    error notices stay in the log: groups and peers get nothing, the owner's DM gets "👀 seen" / a short error line."""
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    from gateway.run_turn import _unexpected_silence_reply
    from agent.i18n import t
    from agent.turn_failure_copy import FAILED_TURN_NOTICE
    warning = _unexpected_silence_reply()
    for text in (warning, t("gateway.errors.unexpected_silence", lang="pt"), FAILED_TURN_NOTICE):
        assert run(adapter.send("group:G1", text)).success
        assert run(adapter.send("PEERDM", text)).success
    assert run(adapter.edit_message("group:G1", "m9", warning)).success
    assert calls(tmp) == []
    assert run(adapter.send("OWNERDM", warning)).success
    assert run(adapter.send("OWNERDM", FAILED_TURN_NOTICE)).success
    sent = [c for c in calls(tmp) if c[:2] == ["send", "OWNERDM"]]
    assert len(sent) == 2
    assert mod.OWNER_SILENCE_REPLY in "".join(sent[0]) and warning not in "".join(sent[0])
    assert mod.OWNER_ERROR_REPLY in "".join(sent[1]) and FAILED_TURN_NOTICE not in "".join(sent[1])
    assert run(adapter.send("group:G1", "a normal reply")).success      # normal replies are untouched
    assert "a normal reply" in "".join(calls(tmp)[-1])


def test_the_first_send_never_loads_the_catalogs_on_the_loop(env, monkeypatch):
    """Parsing the 17 locale catalogs takes seconds with the pure-Python YAML parser: the first send must not do it on
    the gateway's loop. connect() loads them in the background; then a notice in any catalog language is caught."""
    mod, tmp, _ = env
    from agent import i18n_layers
    from agent.i18n import _catalog_cache, _current_home, supported_languages, t
    parse = i18n_layers.parse_locale_file

    def slow_parse(path):
        time.sleep(0.05)  # 17 files: ~0.85 s, a fraction of what the pure-Python parser takes on a loaded host
        return parse(path)

    monkeypatch.setattr(i18n_layers, "parse_locale_file", slow_parse)
    adapter, _ = make_adapter(mod, inbound=False)

    async def first_send():
        gaps, done = [], asyncio.Event()

        async def heartbeat():
            last = time.perf_counter()
            while not done.is_set():
                await asyncio.sleep(0.005)
                now = time.perf_counter()
                gaps.append(now - last)
                last = now

        beat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0.02)
        result = await adapter.send("OWNERDM", "a normal reply")
        done.set()
        await beat
        return result, max(gaps)

    result, stall = run(first_send())
    assert result.success
    assert stall < 0.3, f"the first send blocked the loop for {stall:.3f} s"
    assert run(adapter.connect()) is True
    home = _current_home()
    mod._notice_warmups[home].join(timeout=60)
    assert all((home, lang) in _catalog_cache for lang in supported_languages(home))
    assert run(adapter.send("group:G1", t("gateway.errors.unexpected_silence", lang="de"))).success
    assert not [c for c in calls(tmp) if "G1" in c]


def test_tool_progress_stays_out_of_groups_and_reaches_dms(env):
    """The tool feed ("✍️ Writing …", "💻 Running …", a command block) is dropped in a group, sends and edits alike;
    DMs keep it, and a group's short status lines (▶️ / ✅ / ⚠️ / 🔴) still go out."""
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    feed = "📚 Reading skill \"ghostly-chat\"\n✍️ Writing ~/code/x.md\n💻 Running\n```\ngit status\n```"
    assert run(adapter.send("group:G1", feed)).success
    assert run(adapter.send("group:G1", "✍️ Writing NOTES.md")).success
    assert run(adapter.edit_message("group:G1", "m9", feed + "\n🔍 Searching files")).success
    assert calls(tmp) == []
    for status in ("▶️ started on the release notes", "✅ PR merged", "⚠️ CI is red", "PR opened",
                   "🔴 blocked: the Desktop CI"):
        assert run(adapter.send("group:G1", status)).success
    sent = ["".join(c) for c in calls(tmp)]
    assert len(sent) == 5 and "blocked" in sent[-1]
    log = tmp / "calls.log"
    assert run(adapter.send("OWNERDM", feed)).success
    assert log.read_text().count("Reading skill") == 1
    assert run(adapter.send("PEERDM", feed)).success
    assert log.read_text().count("Reading skill") == 2


def _walk_every_catalog(mod, content):
    """The notice check as it was before the map was cached: every catalog walked on every call."""
    from agent.i18n import _load_catalog, supported_languages
    from agent.turn_failure_copy import FAILED_TURN_NOTICE, PARTIAL_FAILED_TURN_NOTICE
    prefixes = {}
    for lang in supported_languages():
        for key, text in _load_catalog(lang).items():
            if key.startswith("gateway.errors.") and isinstance(text, str):
                prefix = text.split("{", 1)[0].strip()
                if len(prefix) >= 20:
                    prefixes[prefix] = "silence" if key == "gateway.errors.unexpected_silence" else "error"
    for text in (FAILED_TURN_NOTICE, PARTIAL_FAILED_TURN_NOTICE):
        prefixes[str(text).strip()[:80]] = "error"
    text = (content or "").strip()
    if text.startswith("♻️ Recovered reply") and "\n\n" in text:
        text = text.split("\n\n", 1)[1].strip()
    if text.startswith(mod._LIFECYCLE_PREFIXES):
        return "status"
    return next((kind for prefix, kind in prefixes.items() if text.startswith(prefix)), None)


def test_the_notice_check_builds_its_map_once_and_answers_as_the_full_walk_did(env, monkeypatch):
    """Every outbound send and edit asks whether the text is a gateway notice. The {prefix: kind} map is built once per
    set of catalogs (not walked again per call, ~5 ms with 17 languages), gives the same answers as the walk, and is
    built again when Hermes hands out new catalogs (reset_language_cache, a pack or overlay)."""
    mod, _, _ = env
    from agent import i18n
    from agent.i18n import t
    from agent.turn_failure_copy import FAILED_TURN_NOTICE, PARTIAL_FAILED_TURN_NOTICE
    samples = ["", "hello", "Sure, here is the plan for tomorrow.", "⏳ Still waiting for the other Hermes process",
               FAILED_TURN_NOTICE, PARTIAL_FAILED_TURN_NOTICE, "♻️ Recovered reply — x:\n\n" + FAILED_TURN_NOTICE]
    for lang in i18n.supported_languages():
        silence = t("gateway.errors.unexpected_silence", lang=lang)
        catalog = i18n._load_catalog(lang)
        errors = [v for k, v in sorted(catalog.items()) if k.startswith("gateway.errors.") and isinstance(v, str)]
        samples += [silence, silence[:12], *errors[:5]]
    builds = []
    real_build = mod._build_notice_prefixes
    monkeypatch.setattr(mod, "_build_notice_prefixes", lambda c: builds.append(1) or real_build(c))
    assert [mod._gateway_notice_kind(s) for s in samples] == [_walk_every_catalog(mod, s) for s in samples]
    assert {mod._gateway_notice_kind(s) for s in samples} == {None, "status", "error", "silence"}
    assert len(builds) == 1

    import time
    start = time.perf_counter()
    for _ in range(200):
        mod._outbound_for_notice({"role": "owner"}, "Sure, here is the plan for tomorrow.")
    assert (time.perf_counter() - start) / 200 < 0.002  # the full walk took ~5 ms per check
    assert len(builds) == 1

    # New catalogs (what reset_language_cache and a pack lead to) rebuild the map, and a new notice text is recognised.
    i18n.reset_language_cache()
    extra = "The tool runner went away before answering"
    home = i18n._current_home()
    for lang in i18n.supported_languages(home):
        i18n._load_catalog(lang, home)
    i18n._catalog_cache[(home, "en")] = {**i18n._catalog_cache[(home, "en")],
                                         "gateway.errors.test_only": extra + " ({detail})"}
    try:
        assert mod._gateway_notice_kind(extra + " (boom)") == "error" == _walk_every_catalog(mod, extra + " (boom)")
        assert len(builds) >= 2
    finally:
        i18n.reset_language_cache()


def test_unaddressed_group_messages_and_peer_dms_may_stay_silent(env):
    """reply_expected tells the gateway whether a bare NO_REPLY is a valid answer: only a group message that mentions
    me (or replies to my message) and the owner's DM require a reply."""
    mod, _, _ = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(grp("G1", "OWNERKEY", "talking to the coordinator")))
    run(adapter._handle_event(grp("G1", "OWNERKEY", "@Hermes status?", mentioned=True)))
    run(adapter._handle_event(grp("G1", "OWNERKEY", "and this?", replyTo={"id": "x", "from": "me", "snippet": "s"})))
    run(adapter._handle_event(dm("PEERDM", "👍")))
    run(adapter._handle_event(dm("OWNERDM", "hi")))
    assert [e.reply_expected for e in captured] == [False, True, True, False, None]
    from gateway.response_filters import silence_allowed
    assert [silence_allowed(None, e.reply_expected) for e in captured] == [True, False, False, True, False]


def test_the_owners_tap_on_another_agents_approval_starts_no_turn(env):
    """Each agent's approval card is its own: the tap arrives everywhere as the owner's reply to that agent's message, and
    only the agent that asked acts. The others start no turn and leave no 👀."""
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    theirs = {"id": "a1", "from": "peer", "member": "BOTKEY", "snippet": "⚠️ Hermes wants to run a command", "found": True}
    read_group(adapter,
               grp("G1", "OWNERKEY", "Allow Once", replyTo={**theirs, "button": "once"}),
               grp("G1", "OWNERKEY", "Allow Once", replyTo={**theirs, "button": "once"},
                   press={"messageId": "a1", "button": "once", "label": "Allow Once"}))
    assert captured == [] and reacts(tmp) == []


def test_the_owners_reply_to_another_members_message_is_for_that_member(env):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    theirs = {"id": "a1", "from": "peer", "member": "BOTKEY", "snippet": "may I run it?", "found": True}
    read_group(adapter, grp("G1", "OWNERKEY", "yes, go", replyTo=theirs))
    assert captured == [] and reacts(tmp) == []
    # Still mine: he names me in that reply, he quotes my message, he quotes his own, or he writes to everyone.
    read_group(adapter,
               grp("G1", "OWNERKEY", "@Hermes and you?", replyTo=theirs, mentioned=True),
               grp("G1", "OWNERKEY", "go", replyTo={"id": "m9", "from": "me", "snippet": "may I run it?", "found": True}),
               grp("G1", "OWNERKEY", "anyone seen this?",
                   replyTo={"id": "o1", "from": "peer", "member": "OWNERKEY", "snippet": "CI is down", "found": True}),
               grp("G1", "OWNERKEY", "good morning"))
    assert [e.text.splitlines()[-1] for e in captured] == ["@Hermes and you?", "go", "anyone seen this?", "[Ana] good morning"]
    assert [e.reply_expected for e in captured] == [True, True, False, False]
    assert len(reacts(tmp)) == 4


def test_a_tap_on_my_own_approval_in_the_group_still_resolves_it(env, monkeypatch):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval", lambda key, choice, **kw: resolved.append(choice) or 1)
    run(adapter._send_exec_approval_prompt(_approval_prompt("group:G1")))
    run(adapter._handle_event(grp("G1", "OWNERKEY", "Approve", replyTo={"id": "m1", "from": "me", "snippet": "⚠️ run?", "found": True, "button": "once"},
                                  press={"messageId": "m1", "button": "once", "label": "Approve"})))
    assert resolved == ["once"] and captured == []


def test_a_dropped_gateway_notice_is_logged_at_info_with_its_first_line_only(env, caplog):
    """Keeping a notice out of a group or a peer is the policy working, not a fault: it goes to agent.log at INFO, so
    errors.log (WARNING and up) stays for real failures, and only the notice's first line is copied into the log."""
    import logging
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    from gateway.run_turn import _unexpected_silence_reply
    from agent.turn_failure_copy import FAILED_TURN_NOTICE
    caplog.set_level(logging.INFO, logger=mod.logger.name)
    notice = FAILED_TURN_NOTICE + "\nsecond line with details"
    for _ in range(20):
        assert run(adapter.send("group:G1", notice)).success
    assert run(adapter.send("PEERDM", _unexpected_silence_reply())).success
    assert run(adapter.edit_message("group:G1", "m9", notice)).success
    assert calls(tmp) == []
    ours = [r for r in caplog.records if r.name == mod.logger.name and "kept out" in r.getMessage()]
    assert len(ours) == 22
    assert [r for r in ours if r.levelno >= logging.WARNING] == []
    assert all("second line" not in r.getMessage() for r in ours)


def test_owner_dm_is_never_told_to_answer_with_a_silence_marker(env):
    mod, _, _ = env
    assert "NO_REPLY" not in mod.PLATFORM_HINT
    assert "NO_REPLY" in mod._NO_LOOP


def _one_listen_round(adapter, captured, profile=None):
    adapter._running = True

    async def one_round():
        task = asyncio.create_task(adapter._listen_loop(profile))
        for _ in range(100):
            await asyncio.sleep(0.05)
            if captured:
                break
        adapter._running = False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    run(one_round())


def _with_tester(chats):
    data = json.loads(chats.read_text())
    data["BHCHAT"] = {"label": "tester", "name": "Test agent", "role": "agent_peer", "profile": "testing",
                      "note": "Test chat: test scenarios only, testnet only."}
    chats.write_text(json.dumps(data))


def test_a_second_profile_is_listened_to_with_its_own_cursor_and_answered_through_it(env):
    """The test chat lives in the ``testing`` profile: read with ``-p testing``, answered the same way."""
    mod, tmp, chats = env
    _with_tester(chats)
    (tmp / "events-testing.jsonl").write_text(json.dumps(dm("BHCHAT", "scenario 1") | {"seq": 7}) + "\n")
    from gateway.config import PlatformConfig
    adapter = mod.GhostlyAdapter(PlatformConfig(enabled=True, extra={"inbound": True, "profiles": ["testing"]}))
    captured = []
    adapter._enqueue_text_event = lambda event: captured.append(event)
    adapter._load_chats()
    assert adapter.profiles == ["testing"] and "BHCHAT" in adapter.config.extra["allow_from"]
    _one_listen_round(adapter, captured, "testing")
    listen = next(c for c in calls(tmp) if "listen" in c)
    assert listen[:3] == ["-p", "testing", "listen"] and listen[listen.index("--since") + 1] == "41"
    assert adapter._read_cursor("testing") == 7 and adapter._read_cursor() is None    # the default cursor is untouched
    event = captured[0]
    assert event.text == "scenario 1" and event.source.chat_id == "BHCHAT" and event.allow_gateway_control is False
    assert "testnet only" in event.channel_prompt and "NO_REPLY" in event.channel_prompt
    assert run(adapter.send("BHCHAT", "ok")).success and run(adapter.edit_message("BHCHAT", "m1", "ok!")).success
    run(adapter.send_typing("BHCHAT"))
    out = [c for c in calls(tmp) if "BHCHAT" in c]
    assert [c[:4] for c in out] == [["-p", "testing", "send", "BHCHAT"], ["-p", "testing", "edit", "BHCHAT"],
                                    ["-p", "testing", "typing", "BHCHAT"]]
    assert run(adapter.send("OWNERDM", "hi")).success and calls(tmp)[-1][:2] == ["send", "OWNERDM"]


def test_a_chat_seen_from_the_wrong_profile_is_ignored(env):
    mod, _, chats = env
    _with_tester(chats)
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("BHCHAT", "on the default stream")))                 # its entry says testing
    run(adapter._handle_event(dm("OWNERDM", "on the test stream"), "testing"))  # the owner's chat is the default's
    assert captured == []


def test_a_chat_whose_entry_names_the_default_profile_is_the_default_profiles(env):
    """``"profile": "default"`` is the default profile, as in ``extra.profiles``: read, answered, routed to."""
    mod, _, chats = env
    data = json.loads(chats.read_text())
    data["OWNERDM"]["profile"] = "default"
    chats.write_text(json.dumps(data))
    adapter, captured = make_adapter(mod)
    assert adapter._listened_profiles() == [] and mod._pargs("OWNERDM") == []
    run(adapter._handle_event(dm("OWNERDM", "hello")))
    assert [e.text for e in captured] == ["hello"]
    assert adapter._owner_dm() == "OWNERDM"


def test_an_events_gap_is_one_warning_with_the_range_and_the_cursor_moves_past_it(env, caplog):
    """The daemon's journal lost 11-60 (a crash, a trim): ``listen --since 10`` says so with an ``events.gap`` whose seq
    is the cursor itself. One warning names the range and the count (no message text), the next event is a turn."""
    mod, tmp, _ = env
    gap = {"seq": 10, "id": "events.gap:10:61", "type": "events.gap", "at": 1790000000000, "from": 11, "to": 60}
    owner = dm("OWNERDM", "still there?") | {"seq": 61}
    (tmp / "events.jsonl").write_text(json.dumps(gap) + "\n" + json.dumps(owner) + "\n")
    adapter, captured = make_adapter(mod)
    adapter._write_cursor(10)
    with caplog.at_level(logging.INFO):
        _one_listen_round(adapter, captured)
    listen = next(c for c in calls(tmp) if c[0] == "listen")
    assert listen[listen.index("--since") + 1] == "10"
    gaps = [r for r in caplog.records if "events.gap" in r.getMessage()]
    assert len(gaps) == 1 and gaps[0].levelno == logging.WARNING
    assert "events 11-60 (50) were lost" in gaps[0].getMessage()
    assert not any("still there?" in r.getMessage() for r in caplog.records)
    assert [e.text for e in captured] == ["still there?"] and adapter._read_cursor() == 61


def test_new_chats_of_a_second_profile_are_discovered_there_and_remember_it(env):
    mod, tmp, chats = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("newdm", "hello"), "testing"))
    assert json.loads(chats.read_text())["newdm"]["profile"] == "testing"
    assert ["-p", "testing", "chat", "list"] == next(c for c in calls(tmp) if "list" in c)[:4]
    assert captured[0].allow_gateway_control is False
    run(adapter.send("newdm", "hi"))
    assert calls(tmp)[-1][:3] == ["-p", "testing", "send"]


def test_a_profile_named_by_an_allowlisted_chat_is_listened_to_without_config(env):
    mod, _, chats = env
    _with_tester(chats)
    adapter, _ = make_adapter(mod)                      # no extra.profiles
    assert adapter._listened_profiles() == ["testing"]
    assert mod._pargs("BHCHAT") == ["-p", "testing"] and mod._pargs("OWNERDM") == []


def test_a_chat_added_in_a_new_profile_while_running_is_listened_to_and_answered_through_it(env, monkeypatch):
    """chats.json is read again without a restart: a chat added there in a profile no entry named yet is listened to
    with ``-p`` within CHATS_CHECK_SECONDS, and a send to it goes out through that profile, not the default one."""
    mod, tmp, chats = env
    monkeypatch.setattr(mod, "CHATS_CHECK_SECONDS", 0.05, raising=False)
    (tmp / "events.jsonl").write_text("")
    (tmp / "events-testing.jsonl").write_text(json.dumps(dm("BHCHAT", "scenario 1") | {"seq": 7}) + "\n")
    adapter, captured = make_adapter(mod)

    async def scenario():
        await adapter.connect()
        await asyncio.sleep(0.2)
        _with_tester(chats)                             # added while the gateway runs
        sent = await adapter.send("BHCHAT", "hello")
        for _ in range(100):
            await asyncio.sleep(0.05)
            if captured:
                break
        loops = len(adapter._profile_tasks)            # the chats.json check and one loop for testing, however many checks
        await adapter.disconnect()
        return sent, loops

    sent, loops = run(scenario())
    assert sent.success and loops == 2
    assert [e.text for e in captured] == ["scenario 1"]
    assert ["-p", "testing", "listen"] in [c[:3] for c in calls(tmp)]
    assert ["-p", "testing", "send", "BHCHAT"] in [c[:4] for c in calls(tmp)]
    assert ["send", "BHCHAT"] not in [c[:2] for c in calls(tmp)]


def test_a_chats_json_left_invalid_is_logged_once_not_at_every_check(env, monkeypatch, caplog):
    """The file is checked every CHATS_CHECK_SECONDS and before each send: one that stays invalid (an edit left half
    done) is one error until it changes again, not a line per check. A new bad version is logged again."""
    mod, tmp, chats = env
    monkeypatch.setattr(mod, "CHATS_CHECK_SECONDS", 0.02, raising=False)
    (tmp / "events.jsonl").write_text("")
    adapter, _ = make_adapter(mod)
    chats.write_text("{ not json")

    async def scenario():
        await adapter.connect()
        await asyncio.sleep(0.3)                        # ~15 checks
        await adapter.send("OWNERDM", "hello")
        chats.write_text("[ still not json")
        os.utime(chats, (time.time() + 5, time.time() + 5))
        await asyncio.sleep(0.2)
        kept = "OWNERDM" in adapter.chats               # the previous allowlist is kept meanwhile
        chats.write_text(json.dumps({"NEWDM": {"label": "new", "role": "contact"}}))
        os.utime(chats, (time.time() + 10, time.time() + 10))
        await asyncio.sleep(0.2)
        await adapter.disconnect()
        return kept

    caplog.set_level(logging.ERROR, logger=mod.logger.name)
    assert run(scenario())
    bad = [r for r in caplog.records if "not valid JSON" in r.getMessage()]
    assert len(bad) == 2
    assert list(adapter.chats) == ["NEWDM"]             # the file fixed is read again


def _approval_prompt(chat_id, session_key="sk1"):
    from gateway.platforms.base import ExecApprovalPrompt
    return ExecApprovalPrompt(chat_id=chat_id, session_key=session_key, text="⚠️ run?\n\nrm -rf x\n\nreason",
                              actions=[("Approve", "once", "primary"), ("Session", "session", ""), ("Deny", "deny", "danger")],
                              command="rm -rf x", description="reason", smart_denied=False)


def test_exec_approval_goes_out_as_buttons_and_a_press_by_the_owner_resolves_it(env, monkeypatch):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    assert mod.GhostlyAdapter.supports_exec_approval_buttons()
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval", lambda key, choice, **kw: resolved.append((key, choice)) or 1)
    result = run(adapter._send_exec_approval_prompt(_approval_prompt("OWNERDM")))
    assert result.success and result.message_id == "m1"
    sent = [c for c in calls(tmp) if c[0] == "send"][-1]
    assert sent[:2] == ["send", "OWNERDM"] and "--once" in sent
    assert {"once:Approve", "session:Session", "deny:Deny"} <= set(sent)
    assert "once=primary" in sent and "deny=danger" in sent
    run(adapter._handle_event({"type": "button.pressed", "chat": "OWNERDM", "messageId": "m1", "button": "once", "by": "OWNERDM"}))
    assert resolved == [("sk1", "once")]
    closed = [c for c in calls(tmp) if c[0] == "button"][-1]
    assert closed[:4] == ["button", "update", "OWNERDM", "m1"] and "--close" in closed and "once" in closed
    # The press's own reply is not a new turn, and a second press does nothing.
    run(adapter._handle_event({"type": "button.pressed", "chat": "OWNERDM", "messageId": "m1", "button": "deny", "by": "OWNERDM"}))
    assert resolved == [("sk1", "once")] and captured == []


def test_an_approval_press_from_someone_else_is_refused(env, monkeypatch):
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval", lambda key, choice, **kw: resolved.append(choice) or 1)
    run(adapter._send_exec_approval_prompt(_approval_prompt("group:G1")))
    run(adapter._handle_event({"type": "button.pressed", "group": "G1", "messageId": "m1", "button": "once", "by": "BOTKEY"}))
    assert resolved == []
    run(adapter._handle_event({"type": "button.pressed", "group": "G1", "messageId": "m1", "button": "once", "by": "OWNERKEY"}))
    assert resolved == ["once"]


def test_the_reply_a_tap_sends_answers_the_approval_instead_of_starting_a_turn(env, monkeypatch):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval", lambda key, choice, **kw: resolved.append(choice) or 1)
    run(adapter._send_exec_approval_prompt(_approval_prompt("OWNERDM")))
    run(adapter._handle_event(dm("OWNERDM", "Deny", press={"messageId": "m1", "button": "deny", "label": "Deny"})))
    assert resolved == ["deny"] and captured == []


def _sends_to(tmp, chat):
    return [c for c in calls(tmp) if c[0] == "send" and c[1] == chat]


def test_a_colleague_chats_approval_goes_to_the_owners_dm_and_the_colleague_gets_one_line(env, monkeypatch):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval", lambda key, choice, **kw: resolved.append((key, choice)) or 1)
    result = run(adapter._send_exec_approval_prompt(_approval_prompt("PEERDM", session_key="peer-session")))
    assert result.success
    card = _sends_to(tmp, "OWNERDM")
    assert len(card) == 1 and "once:Approve" in card[0] and "--once" in card[0]
    log = (tmp / "calls.log").read_text()          # the card's text spans lines in the fake CLI's log
    assert "from the chat with the peer agent" in log and "rm -rf x" in log
    note = _sends_to(tmp, "PEERDM")
    assert len(note) == 1 and "Ana" in note[0][-1] and "rm -rf x" in note[0][-1]
    assert not any(c for c in note[0] if c.startswith("once:"))         # no buttons in the colleague's chat
    # A tap by the colleague in its own chat resolves nothing; the owner's tap in their DM does.
    run(adapter._handle_event({"type": "button.pressed", "chat": "PEERDM", "messageId": "m1", "button": "once", "by": "PEERDM"}))
    assert resolved == []
    run(adapter._handle_event({"type": "button.pressed", "chat": "OWNERDM", "messageId": "m1", "button": "session", "by": "OWNERDM"}))
    assert resolved == [("peer-session", "session")] and captured == []


def test_the_owner_typing_approve_in_their_dm_answers_the_routed_approval(env, monkeypatch):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval",
                        lambda key, choice, **kw: resolved.append((key, choice, kw.get("reason"))) or 1)
    run(adapter._send_exec_approval_prompt(_approval_prompt("PEERDM", session_key="peer-session")))
    run(adapter._handle_event(dm("OWNERDM", "/approve session")))
    assert resolved == [("peer-session", "session", None)] and captured == []
    closed = [c for c in calls(tmp) if c[0] == "button"][-1]
    assert closed[:4] == ["button", "update", "OWNERDM", "m1"] and "session" in closed
    # Nothing routed is pending any more: the next /approve is a normal gateway command for the owner's own session.
    run(adapter._handle_event(dm("OWNERDM", "/approve")))
    assert len(resolved) == 1 and len(captured) == 1


def test_the_owner_typing_deny_with_a_reason_denies_the_routed_approval(env, monkeypatch):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    resolved = []
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval",
                        lambda key, choice, **kw: resolved.append((key, choice, kw.get("reason"))) or 1)
    run(adapter._send_exec_approval_prompt(_approval_prompt("PEERDM", session_key="peer-session")))
    run(adapter._handle_event(dm("OWNERDM", "/deny not now")))
    assert resolved == [("peer-session", "deny", "not now")] and captured == []


def test_the_owners_own_approvals_stay_in_their_dm_with_no_extra_line(env, monkeypatch):
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    run(adapter._send_exec_approval_prompt(_approval_prompt("OWNERDM")))
    assert len(_sends_to(tmp, "OWNERDM")) == 1 and _sends_to(tmp, "PEERDM") == []


def test_a_late_press_says_the_command_did_not_run(env, monkeypatch):
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    import tools.approval
    monkeypatch.setattr(tools.approval, "resolve_gateway_approval", lambda key, choice, **kw: 0)
    run(adapter._send_exec_approval_prompt(_approval_prompt("OWNERDM")))
    run(adapter._handle_event({"type": "button.pressed", "chat": "OWNERDM", "messageId": "m1", "button": "once", "by": "OWNERDM"}))
    assert "expired" in (tmp / "calls.log").read_text()


def test_the_owner_answering_another_agents_question_in_the_group_is_not_a_turn(env):
    """Allow once / a tap / /approve in reply to ANOTHER member's message is that agent's answer: no turn here.
    The same reply to my own message, or with a mention of me, still reaches me."""
    mod, _, _ = env
    adapter, captured = make_adapter(mod)
    other = {"id": "q1", "from": "peer", "snippet": "Hermes wants to run a command"}
    run(adapter._handle_event(grp("G1", "OWNERKEY", "Allow once", replyTo=other)))
    run(adapter._handle_event(grp("G1", "OWNERKEY", "/approve session", replyTo=other)))
    run(adapter._handle_event(grp("G1", "OWNERKEY", "Yes", replyTo=other,
                                  press={"messageId": "q1", "button": "yes", "label": "Yes"})))
    assert captured == []
    mine = grp("G1", "OWNERKEY", "Allow once", replyTo={"id": "q2", "from": "me", "snippet": "s"})
    mine["id"] += ":mine"
    run(adapter._handle_event(mine))
    tagged = grp("G1", "OWNERKEY", "Allow once", replyTo=other, mentioned=True)
    tagged["id"] += ":tagged"
    run(adapter._handle_event(tagged))
    # Any other reply to that member's message is for that member too, unless it mentions me.
    run(adapter._handle_event(grp("G1", "OWNERKEY", "do that too", replyTo=other)))
    assert len(captured) == 2


def test_names_in_the_prompts_come_from_the_config_with_neutral_defaults(env):
    mod, _, _ = env
    from gateway.config import PlatformConfig
    plain = mod.GhostlyAdapter(PlatformConfig(enabled=True, extra={"inbound": True}))
    assert plain.owner_name == "the owner" and plain.agent_name == "Hermes"
    assert "never orders from the owner" in plain.role_notes["agent_peer"] and "You are Hermes." in plain.role_notes["group"]
    assert plain.owner_keys_file == plain.state_dir / "owner_keys.txt"
    named = mod.GhostlyAdapter(PlatformConfig(enabled=True, extra={"owner_name": "Ana", "agent_name": "Hermes (lab)"}))
    assert "NOT Ana" in named.role_notes["contact"] and "You are Hermes (lab)." in named.role_notes["agent_peer"]


def test_ghostly_bin_with_a_tilde_or_a_bare_name_is_found(env, tmp_path, monkeypatch):
    mod, _, _ = env
    # python-dotenv leaves ``~`` as is, so ~/.hermes/.env hands the plugin the literal value.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("GHOSTLY_BIN", "~/ghostly")
    assert mod._ghostly_bin() == str(tmp_path / "ghostly")
    assert mod.check_requirements()
    # A wrapper's bare name is looked up on PATH.
    monkeypatch.setenv("PATH", os.pathsep.join([str(tmp_path), os.environ.get("PATH", "")]))
    monkeypatch.setenv("GHOSTLY_BIN", "ghostly")
    assert mod._ghostly_bin() == str(tmp_path / "ghostly")
    monkeypatch.setenv("GHOSTLY_BIN", "~/missing-ghostly")
    assert mod._ghostly_bin() is None


def test_missing_ghostly_bin_names_the_value_tried(env, monkeypatch, caplog):
    mod, _, _ = env
    monkeypatch.setenv("GHOSTLY_BIN", "~/missing-ghostly")
    adapter, _ = make_adapter(mod)
    with caplog.at_level("WARNING"):
        assert run(adapter.connect()) is False
    assert "GHOSTLY_BIN=~/missing-ghostly" in caplog.text


def test_malformed_allowlist_entries_are_dropped_and_the_rest_keeps_working(env, caplog):
    mod, tmp, chats = env
    chats.write_text(json.dumps({
        "OWNERDM": {"label": "owner", "name": "Ana", "role": "owner"},
        "BADDM": "owner",                                                          # a bare role, not an object
        "group:G1": {"label": "dev", "role": "group", "mentions_only": True,
                     "members": {"OWNERKEY": {"name": "Ana", "owner": True}, "BOTKEY": "Peer"}},  # a bare name
        "group:G3": {"label": "ops", "role": "group", "members": ["OWNERKEY"]},    # a list, not an object
    }))
    with caplog.at_level("ERROR"):
        adapter, captured = make_adapter(mod)
    read_group(adapter, grp("G1", "OWNERKEY", "owner talking"), grp("G1", "BOTKEY", "@Hermes hi", mentioned=True),
               grp("G3", "OWNERKEY", "@Hermes ops", mentioned=True), grp("G2", "OWNERKEY", "new group"))
    run(adapter._handle_event(dm("BADDM", "hi")))                               # unresolvable now: ignored, no error
    run(adapter._handle_event(dm("OWNERDM", "hi")))
    assert [e.text.split("] ", 1)[-1] for e in captured] == ["owner talking", "@Hermes hi", "@Hermes ops", "new group", "hi"]
    assert json.loads(chats.read_text())["group:G2"]["members"]["OWNERKEY"].get("owner") is True
    # Saving the new group leaves the owner's malformed entries in the file as written (only memory ignores them).
    on_disk = json.loads(chats.read_text())
    assert on_disk["BADDM"] == "owner" and on_disk["group:G1"]["members"]["BOTKEY"] == "Peer"
    assert on_disk["group:G3"]["members"] == ["OWNERKEY"]
    assert "BADDM" not in adapter.config.extra["allow_from"]
    # One error line per dropped entry, naming it.
    errors = [r.getMessage() for r in caplog.records if r.name == mod.logger.name and r.levelname == "ERROR"]
    assert len(errors) == 3
    assert any("BADDM" in e for e in errors) and any("BOTKEY" in e for e in errors) and any("group:G3" in e for e in errors)



def test_one_line_group_replies_that_start_with_an_emoji_are_not_taken_for_tool_progress(env):
    """Only the gateway's progress shapes are dropped in a group (a progress verb, or a tool name then ``:`` / ``...``);
    a short reply that happens to start with one of the tool emoji goes out."""
    mod, tmp, _ = env
    adapter, _ = make_adapter(mod)
    for reply in ("❓ Which branch?", "🔍 Looking into it", "💡 We could ship it on Friday", "🧪 tests green on One",
                  "📝 Notes from the call:\n- the relay is back"):
        assert run(adapter.send("group:G1", reply)).success
    assert sum(c[:2] == ["group", "send"] for c in calls(tmp)) == 5
    for feed in ("📝 skill_manage...", '⚙️ process_manage: "list"', '🌐 browser_exec: "open the page"',
                 "👁️ Looking at the image", "🐍 Running code", "🔍 Searching the web for relay status"):
        assert mod._is_tool_progress(feed), feed


def test_a_hermes_without_the_catalog_cache_still_keeps_notices_out(env, monkeypatch):
    """``agent.i18n._catalog_cache`` is private: if a Hermes update renames it, the check falls back to loading the
    catalogs (slow once) instead of seeing no notice at all and letting every one into groups."""
    mod, tmp, _ = env
    from agent import i18n
    from agent.i18n import t
    import types

    class WithoutCache(types.ModuleType):          # agent.i18n as the adapter imports it, minus the private cache
        def __getattr__(self, name):
            if name == "_catalog_cache":
                raise AttributeError(name)
            return getattr(i18n, name)

    monkeypatch.setitem(sys.modules, "agent.i18n", WithoutCache("agent.i18n"))
    adapter, _ = make_adapter(mod, inbound=False)
    assert mod._gateway_notice_kind(t("gateway.errors.unexpected_silence", lang="de")) == "silence"
    assert run(adapter.send("group:G1", t("gateway.errors.unexpected_silence", lang="de"))).success
    assert not [c for c in calls(tmp) if "G1" in c]
