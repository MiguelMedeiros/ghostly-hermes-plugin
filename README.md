# ghostly-hermes-plugin

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) gateway platform for
[Ghostly](https://github.com/MiguelMedeiros/ghostly), the peer-to-peer messenger. Your Hermes gets a
Ghostly profile and talks to you there: every allowlisted chat and group is a normal gateway session.

- Replies, edits in place, files and voice notes (DMs), text in groups.
- A 👀 on each of your messages as soon as it is read, and a "thinking" status while Hermes works.
- Exec approvals as Ghostly buttons. An approval asked in another agent's chat comes to your own chat.
- Roles per chat: you (the owner), delegates, other agents (loop guard, no control), contacts, and
  groups that only wake Hermes on a mention.
- Gateway notices (errors, "still waiting" lines) never reach a group or another agent.
- Cron jobs and `send_message` can deliver to a Ghostly chat (`deliver=ghostly`).

It drives the local `ghostly` CLI: no Ghostly server is involved, and the plugin needs no API key.

Setting it up with an AI agent? Point it at [AGENTS.md](AGENTS.md).

## Requirements

- Hermes Agent 0.22 or newer, with its gateway.
- The [Ghostly CLI](https://github.com/MiguelMedeiros/ghostly/tree/main/packages/cli) 1.5.0 or newer
  (`npm install -g @ghostlytools/cli`, Node 22.12+), with a profile and its daemon running
  (`ghostly daemon status`).
- A Ghostly chat between that profile and you (`ghostly invite create`, then join from your app).

## Install

```bash
hermes plugins install MiguelMedeiros/ghostly-hermes-plugin --enable
```

Or by hand: `git clone https://github.com/MiguelMedeiros/ghostly-hermes-plugin ~/.hermes/plugins/ghostly-platform`,
then `hermes plugins enable ghostly-platform`.

## Configure

1. Merge [config/gateway-ghostly.yaml](config/gateway-ghostly.yaml) into `~/.hermes/config.yaml`
   (`platforms.ghostly` with `inbound: true`, your `owner_name`, and the display settings).
2. Write the allowlist `~/.hermes/ghostly-platform/chats.json` from
   [config/chats.example.json](config/chats.example.json). Chat ids come from `ghostly chat list`,
   group ids and member keys from `ghostly group list` and `ghostly group show <id>`.
3. Set `GHOSTLY_HOME_CHANNEL=<your chat label or id>` in `~/.hermes/.env` (where cron jobs deliver).
   `GHOSTLY_BIN` is optional, for a `ghostly` that is not on `PATH`.
4. Restart the gateway: `hermes gateway restart`.

| Role in `chats.json` | What it may do |
|---|---|
| `owner` | Full authority: commands, approvals. Never answered with a silent `NO_REPLY`. |
| `"delegate": true` (on a chat or a group member) | Speaks for the owner, except for real money, secrets, deleting data, publishing and production deploys. |
| `agent_peer`, `coordinator` | Another agent: a colleague, never orders. Marked as a bot so two agents do not loop. |
| `contact` | Anyone else. Reply only. Unknown chats that write in are added with this role. |
| `group` | Each message is prefixed with its author. `"mentions_only": true` wakes Hermes only on a mention, except for the owner's messages. |

Your member keys in groups can also go in `~/.hermes/ghostly-platform/owner_keys.txt`, one per line
(read again when it changes), for example when you write from a second device.

## Tests

```bash
tests/run
```

It runs `tests/` with your Hermes Agent's own Python, against a fake `ghostly` CLI (no daemon, no
network). Set `HERMES_AGENT_DIR` if Hermes is not in `~/.hermes/hermes-agent`. pytest is installed
into `.tests-deps/`, never into Hermes itself.

## Privacy

- Only allowlisted chats are answered with authority; an unknown chat becomes a `contact`.
- The CLI's secret guard blocks a reply that looks like a key or seed, and the chat gets a short warning.
- State (allowlist, cursor, received files) stays in `~/.hermes/ghostly-platform/`; the plugin writes the allowlist with mode 600.

## Related

- [ghostly-voice-agent](https://github.com/MiguelMedeiros/ghostly-voice-agent): optional, answers your
  Ghostly calls by voice and hands work to an agent like this one.

## License

MIT, see [LICENSE](LICENSE).
