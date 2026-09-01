# Hermes Lark Streaming — Moonawn 维护版

[English](README.md) · [维护与部署](docs/MAINTENANCE.md) · [MIT 许可证](LICENSE)

这是 [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming) 的独立 fork，基于上游 **v1.7.0 / `aef71a8`**。上游亦基于 [Cheerwhy/hermes-lark-streaming](https://github.com/Cheerwhy/hermes-lark-streaming) 发展而来，原有署名与 MIT 许可证保留。此项目是 Hermes 插件，不是 Codex 插件。

**当前是开发候选 `1.7.0+moonawn.9`，尚未发布稳定版。** 维护重点是终稿完整送达、流式任务可靠收尾，以及连续、易读的单卡体验。

## 已加入的修正

- **终稿权威性**：最终回答可以比过程文本短；较长的过渡语不再覆盖较短终稿。相同消息的不同终稿阶段交回 gateway 发送。
- **更新顺序**：同一卡片的创建、刷新、收尾共用一个 writer；旧 ACK 不会清掉新内容的待刷新标记；CardKit 发布重试复用同一 UUID，服务端已接收但客户端丢 ACK 时不会再产生第二张加载卡。
- **超时与取消**：所有聊天都受收尾时限保护；取消释放等待者、结束生命周期，过期的收尾任务可以回收。失败卡片会尝试停止服务端打字动画，此动作也有时限。
- **无损兜底**：按片段发送完整正文，保留分隔符；重试复用同一轮、同一片段的 UUID，不再只截取开头。
- **单卡完成**：推荐让 CardKit 同时承载 reasoning/tool 过程、逐步正文和权威终稿；正常完成只产生一张卡，封卡失败才发送无损普通消息兜底。
- **可选独立答复**：仍可将过程卡和最终消息分开；它适合需要原生消息回读的特殊部署，不再作为本 fork 的推荐展示。
- **可选紧凑显示**：仅在独立答复模式中保留短状态与可折叠工具过程，完整正文放在另一条消息里。
- **实验性回读校验**：原生 Feishu adapter 可先保存终稿和待发送载荷，再逐条回读消息正文；异常恢复使用已保存内容，不重新调用模型。
- **可选群内队列**：按明确配置的群串行处理，保留独立入站消息，检查排队期间被撤回的消息；默认不改变群的并发策略。
- **引用回复路由**：在 Hermes 计算 session 与投递 metadata 前清除普通 Feishu 引用回复携带的伪 `thread_id`，真实话题 thread 保持不变。
- **压缩状态防误报**：若 Hermes 的 commit fence 已取消压缩提交，抑制后台清理阶段迟到的“compaction complete”成功提示；正常完成提示不变。
- **状态归属单卡**：Hermes 的 preflight compression、provider retry 与迟到状态 callback 会绑定到原始 turn，由该轮 CardKit 生命周期吸收，不再在答案旁生成普通消息或第二张状态卡。
- **可选首个答案再开流**：仍可等到出现 answer 内容才创建 CardKit；适合更在意占位卡存续时间的部署，但短回答可能一出现就接近完整，不是本 fork 推荐的连续单卡体验。

保留上游 v1.7.0 的 relay 支持、schema 错误恢复、Markdown 缓存与面板记录上限。

## 展示选择

| 模式 | 过程卡 | 最终正文 |
| --- | --- | --- |
| 单卡流式（推荐） | 完整流式正文和工具过程 | 同一张卡片，必要时文本兜底 |
| 独立答复 + full | 保留流式正文预览 | 另发独立答复 |
| 独立答复 + compact | 短状态、可折叠工具过程 | 另发独立答复，适合长文与多人群 |

推荐同时配置 `final_delivery: card` 与 `streaming_card_start: message_start`。用户消息开始处理时即出现一张带中性准备提示的 CardKit；之后 reasoning/tool、逐步正文、权威终稿和完成状态都由这张卡承载。自动压缩与 provider 生命周期 callback 绑定到该轮，不再另发普通状态消息。provider 只有 final、没有 delta 时，也会在同一卡片内替换提示并封定终稿。

`streaming_card_start: first_answer` 仍可用于不希望长时间显示占位卡的环境；它会缓存 reasoning/tool，直到首段正文才开卡，因此短回答可能看起来像瞬间完成。HLS 已接管该轮时，生命周期状态同样不会另发消息。

正常单卡路径在 CardKit 的最终 batch update 与 close ACK 成功后抑制 gateway 普通回复；若建卡、写终稿或封卡失败，插件才发送完整文本兜底，避免为了保持形式而静默丢答。独立答复模式中的“生成完成 · Final answer follows”只表示另一条正文即将投递，**不等于正文送达回执**。

将 [单卡配置示例](examples/single-card-streaming.yaml) 合并到测试 Profile 的现有配置，不要整份覆盖。过程卡与正文分离是可选替代方案，见 [紧凑示例](examples/compact-progress.yaml)；回读校验另行显式启用，见 [示例](examples/verified-native-delivery.yaml) 与 [边界说明](docs/MAINTENANCE.md#verified-delivery)。

## 开发验证

```bash
git clone https://github.com/Moonawn/hermes-lark-streaming.git
cd hermes-lark-streaming
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-test.txt
python scripts/test_offline.py tests -q
```

CI 覆盖 Python 3.11–3.13。离线测试使用临时 `HERMES_HOME`，阻止 Python socket 网络访问。原生发送器兼容测试需要固定版本的 Hermes 源码：

```bash
HERMES_SRC_DIR=/path/to/hermes-source python scripts/test_offline.py \
  tests/test_verified_delivery.py tests/test_final_delivery_local.py \
  tests/test_delivery_reliability.py tests/test_task_group_queue.py tests/integration -q -rs
```

提供 `HERMES_SRC_DIR` 后，无法导入原生 adapter 会直接失败，不能靠跳过交付测试获得绿色结果。CI 没有飞书凭据，不发送通知、不发布版本、不部署，也不定时覆盖 fork。

先在新测试 Profile 使用审核过的 commit，具体步骤见 [部署指南](docs/AGENT_GUIDE.md)。不要同时加载本 fork 和同名上游插件。默认分支沿用 `github_sync` 名称，但已移除自动同步逻辑。

本 fork 的问题与改进请提交到 [自己的 Issue/PR 区](https://github.com/Moonawn/hermes-lark-streaming/issues)。不要上传真实凭据、群与消息 ID、Profile、原始日志或交付数据库。未标注为 fork 文档的旧 `docs/` 内容保留作上游历史参考。
