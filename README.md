# Hermes Lark Streaming — Moonawn fork

[中文版](README.zh-CN.md) · [Maintenance and deployment](docs/MAINTENANCE.md) · [MIT license](LICENSE)

An independently maintained fork of [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming), based on upstream **v1.7.0** (`aef71a8`). The upstream project in turn credits [Cheerwhy/hermes-lark-streaming](https://github.com/Cheerwhy/hermes-lark-streaming). Original attribution and the MIT license are retained.

**Development candidate: `1.7.0+moonawn.7`. No stable release is implied.** This fork prioritizes complete final answers, bounded streaming cleanup, and a continuous single-card reading experience. It is a Hermes plugin, not a Codex plugin.

## What changes

- The final response replaces streamed progress even when it is shorter. Distinct later final phases are handed back to the gateway instead of being silently discarded.
- Card creation, updates and sealing share one writer. An old ACK cannot clear a newer answer revision; CardKit publication retries reuse one UUID so an accepted request with a lost ACK does not create a second loading card.
- Completion has a deadline for every chat. Cancellation releases waiters and ends the session; an orphaned completion does not live forever. A failed card gets a bounded attempt to close its typing animation.
- Legacy text fallback retains the complete answer, splits it without dropping separators, and reuses per-turn, per-part UUIDs on retry.
- Recommended **single-card completion** keeps reasoning/tool progress, streamed answer text, and the authoritative final in one CardKit message. A normal turn publishes one card; a lossless plain-message fallback is used only after card delivery fails.
- Optional **separate final delivery** remains available for deployments that require native-message read-back, but it is no longer this fork's recommended presentation.
- Optional **compact progress cards** apply only to separate-message delivery; the full answer then stays in another message.
- Experimental native Feishu **verified delivery** stages the original final and planned payloads in a private SQLite outbox, validates the destination, and reads back every acknowledged message body. It resumes persisted work rather than rerunning the model.
- Optional per-chat queues retain individual messages and check for withdrawn queued messages. No chat is serialized unless explicitly configured.
- Feishu reply routing is normalized before Hermes derives the session and delivery metadata: a synthetic `thread_id` on an ordinary reply is removed, while a genuine topic thread is preserved.
- A feature-detected compression guard suppresses Hermes' late “compaction complete” success message only when that attempt's commit fence was cancelled. Normal completion remains visible.
- Hermes lifecycle notices are associated with their originating turn and absorbed by that turn's CardKit lifecycle. Preflight compression, provider retry, and late status callbacks no longer create text bubbles or a second status card beside the answer.
- Optional first-answer activation still delays CardKit creation until answer text exists. It is useful when minimizing placeholder lifetime matters more than showing immediate feedback, but it does not provide the continuous one-card experience recommended by this fork.

Upstream v1.7.0's relay support, schema-error recovery, Markdown escape cache and bounded panel history remain in place.

## Choose a display mode

| Mode | Process card | Final answer | Intended use |
| --- | --- | --- | --- |
| Single-card stream (recommended) | Full streamed body and tools | Same card; lossless text fallback if necessary | Continuous reading |
| Separate + full | Full streamed preview | Independent native message | Keep the typing experience |
| Separate + compact | Short status and collapsible tools | Independent native message | Long answers and busy groups |

The recommended pair is `final_delivery: card` plus `streaming_card_start: message_start`. The CardKit message appears at turn start with one neutral preparation hint, then owns reasoning/tools, streamed answer text, the authoritative final, and completion. Automatic compression and provider lifecycle callbacks are associated with that turn instead of being published as extra messages. A provider that emits only a final still replaces the hint and seals that final in the same card.

`streaming_card_start: first_answer` remains available when a deployment prefers no visible placeholder during long preflight work. It buffers reasoning/tools until answer text exists, so a short answer can appear almost complete rather than visibly stream. Lifecycle notices are still absorbed while the turn is owned by HLS.

On the normal single-card path, the gateway reply is suppressed only after CardKit owns completion; create, final-write, or seal failure triggers the full text fallback instead of silently losing the answer. In separate mode, “Final answer follows” describes the next message and is **not** a delivery receipt.

Merge [the single-card example](examples/single-card-streaming.yaml) into a **test Profile**, not over an existing configuration. Separate compact progress is an alternative in [the compact example](examples/compact-progress.yaml). Experimental verified delivery is a separate-message opt-in; see [the verified example](examples/verified-native-delivery.yaml) and [its limits](docs/MAINTENANCE.md#verified-delivery).

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
