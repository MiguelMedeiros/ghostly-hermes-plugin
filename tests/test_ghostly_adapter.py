"""Behaviour tests for the Ghostly platform adapter against a fake ``ghostly`` CLI (no daemon, no network).

Run with ``tests/run`` from the repo root: it finds the Hermes Agent's Python and runs pytest with it.
``HERMES_AGENT_DIR`` points at the Hermes Agent checkout (default ``~/.hermes/hermes-agent``).
"""
import asyncio
import importlib.util
import json
import os
import stat
import sys
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
  "status "*) echo '{"events":{"lastSeq":41}}';;
  "chat list") cat "$dir/chats.json";;
  "group show") cat "$dir/group.json";;
  "file save") shift 2; p=""; while [ $# -gt 0 ]; do [ "$1" = "--path" ] && p="$2"; shift; done; echo data > "$p"; echo '{"saved":true}';;
  "listen "*) if [ -n "$prof" ]; then cat "$dir/events-$prof.jsonl"; else cat "$dir/events.jsonl"; fi;;
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
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


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


def test_received_file_is_saved_and_typed(env):
    mod, tmp, _ = env
    adapter, captured = make_adapter(mod)
    run(adapter._handle_event(dm("OWNERDM", "", file={"id": "F1", "name": "a.png", "mime": "image/png", "size": 5})))
    event = captured[0]
    assert event.message_type.value == "photo" and Path(event.media_urls[0]).read_text() == "data\n"


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
