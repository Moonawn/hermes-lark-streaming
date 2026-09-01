# Hermes Lark Streaming — Moonawn fork

[中文版](README.zh-CN.md) · [Maintenance and deployment](docs/MAINTENANCE.md) · [MIT license](LICENSE)

An independently maintained fork of [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming), based on upstream **v1.7.0** (`aef71a8`). The upstream project in turn credits [Cheerwhy/hermes-lark-streaming](https://github.com/Cheerwhy/hermes-lark-streaming). Original attribution and the MIT license are retained.

**Development candidate: `1.7.0+moonawn.4`. No stable release is implied.** This fork prioritizes complete final answers, bounded streaming cleanup, and a calmer reading experience. It is a Hermes plugin, not a Codex plugin.

## What changes

- The final response replaces streamed progress even when it is shorter. Distinct later final phases are handed back to the gateway instead of being silently discarded.
- Card creation, updates and sealing share one writer. An old ACK cannot clear a newer answer revision; CardKit publication retries reuse one UUID so an accepted request with a lost ACK does not create a second loading card.
- Completion has a deadline for every chat. Cancellation releases waiters and ends the session; an orphaned completion does not live forever. A failed card gets a bounded attempt to close its typing animation.
- Legacy text fallback retains the complete answer, splits it without dropping separators, and reuses per-turn, per-part UUIDs on retry.
- Optional **separate final delivery** keeps progress-card status independent of final-message delivery. Native failed chunks cannot be hidden behind a later successful chunk.
- Optional **compact progress cards** show status and collapsible tool activity; the full answer stays in a separate message.
- Experimental native Feishu **verified delivery** stages the original final and planned payloads in a private SQLite outbox, validates the destination, and reads back every acknowledged message body. It resumes persisted work rather than rerunning the model.
- Optional per-chat queues retain individual messages and check for withdrawn queued messages. No chat is serialized unless explicitly configured.
- Feishu reply routing is normalized before Hermes derives the session and delivery metadata: a synthetic `thread_id` on an ordinary reply is removed, while a genuine topic thread is preserved.
- A feature-detected compression guard suppresses Hermes' late “compaction complete” success message only when that attempt's commit fence was cancelled. Normal completion remains visible.
- Optional first-answer activation leaves preflight compression on ordinary status messages and creates the CardKit stream only when answer text exists. Fast final-only, stopped, and interrupted turns do not leave a loading-only card.

Upstream v1.7.0's relay support, schema-error recovery, Markdown escape cache and bounded panel history remain in place.

## Choose a display mode

| Mode | Process card | Final answer | Intended use |
| --- | --- | --- | --- |
| Default / legacy | Full streamed body and tools | Same card; lossless text fallback if necessary | Compatibility |
| Separate + full | Full streamed preview | Independent native message | Keep the typing experience |
| Separate + compact | Short status and collapsible tools | Independent native message | Long answers and busy groups |

Set `streaming_card_start: first_answer` to keep preflight/compression outside the streaming card. The first published card already contains the answer element, so it starts at “Writing…” without flashing a stale “loading context” hint. The default `message_start` behavior remains compatible with existing Profiles.

The card's “Final answer follows” label describes generation and the next delivery step; it is **not** a delivery receipt. `verified` means the read-back matches the expected outbound payload; it does not prove a person read the message or that every client rendered it identically.

Merge [the compact example](examples/compact-progress.yaml) into a **test Profile**, not over an existing configuration. To retain streamed body text, set `progress_card: full`. Experimental verified delivery is a separate opt-in; see [the verified example](examples/verified-native-delivery.yaml) and [its limits](docs/MAINTENANCE.md#verified-delivery).

## Develop and test

```bash
git clone https://github.com/Moonawn/hermes-lark-streaming.git
cd hermes-lark-streaming
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-test.txt
python scripts/test_offline.py tests -q
```

Python 3.11–3.13 is covered by CI. The test runner uses a temporary `HERMES_HOME` and blocks Python socket network calls. Native-adapter tests require a separate, pinned Hermes checkout:

```bash
HERMES_SRC_DIR=/path/to/hermes-source python scripts/test_offline.py \
  tests/test_verified_delivery.py tests/test_final_delivery_local.py \
  tests/test_delivery_reliability.py tests/test_task_group_queue.py tests/integration -q -rs
```

When `HERMES_SRC_DIR` is supplied, the native adapter must import successfully; missing dependencies fail the job rather than silently skipping its delivery tests. CI checks fixed Hermes commits, uses read-only repository permissions, and has no Feishu credentials, notifications, releases, deployments or scheduled upstream synchronization.

## Install carefully

Use a reviewed commit in a new test Profile first. Do not install the upstream and this fork simultaneously. Read the [deployment guide](docs/AGENT_GUIDE.md) before changing a running gateway. The default branch retains the inherited name `github_sync`; it does not automatically synchronize with upstream.

Report fork-specific issues and PRs to [Moonawn/hermes-lark-streaming](https://github.com/Moonawn/hermes-lark-streaming/issues). Do not upload real credentials, chat/message IDs, profiles, logs or outbox databases. Upstream documentation under `docs/` is historical unless explicitly marked as fork documentation.
