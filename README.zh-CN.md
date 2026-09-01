# Hermes Lark Streaming — Moonawn 维护版

[English](README.md) · [维护与部署](docs/MAINTENANCE.md) · [MIT 许可证](LICENSE)

这是 [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming) 的独立 fork，基于上游 **v1.7.0 / `aef71a8`**。上游亦基于 [Cheerwhy/hermes-lark-streaming](https://github.com/Cheerwhy/hermes-lark-streaming) 发展而来，原有署名与 MIT 许可证保留。此项目是 Hermes 插件，不是 Codex 插件。

**当前是开发候选 `1.7.0+moonawn.4`，尚未发布稳定版。** 维护重点是终稿完整送达、流式任务可靠收尾，以及更安静、易读的展示。

## 已加入的修正

- **终稿权威性**：最终回答可以比过程文本短；较长的过渡语不再覆盖较短终稿。相同消息的不同终稿阶段交回 gateway 发送。
- **更新顺序**：同一卡片的创建、刷新、收尾共用一个 writer；旧 ACK 不会清掉新内容的待刷新标记；CardKit 发布重试复用同一 UUID，服务端已接收但客户端丢 ACK 时不会再产生第二张加载卡。
- **超时与取消**：所有聊天都受收尾时限保护；取消释放等待者、结束生命周期，过期的收尾任务可以回收。失败卡片会尝试停止服务端打字动画，此动作也有时限。
- **无损兜底**：按片段发送完整正文，保留分隔符；重试复用同一轮、同一片段的 UUID，不再只截取开头。
- **独立答复**：可将过程卡和最终消息分开。卡片存在不代表正文已送达，前一片段失败也不会被后一片段的成功掩盖。
- **紧凑显示**：可只保留短状态与可折叠工具过程，完整正文放在独立答复里，减少长卡片跳动。
- **实验性回读校验**：原生 Feishu adapter 可先保存终稿和待发送载荷，再逐条回读消息正文；异常恢复使用已保存内容，不重新调用模型。
- **可选群内队列**：按明确配置的群串行处理，保留独立入站消息，检查排队期间被撤回的消息；默认不改变群的并发策略。
- **引用回复路由**：在 Hermes 计算 session 与投递 metadata 前清除普通 Feishu 引用回复携带的伪 `thread_id`，真实话题 thread 保持不变。
- **压缩状态防误报**：若 Hermes 的 commit fence 已取消压缩提交，抑制后台清理阶段迟到的“compaction complete”成功提示；正常完成提示不变。
- **首个答案再开流**：可让 preflight compression 只显示普通状态消息，直到出现 answer 内容才创建 CardKit 流式卡；仅返回 final、压缩期停止或被新消息打断时，不留下加载占位卡。

保留上游 v1.7.0 的 relay 支持、schema 错误恢复、Markdown 缓存与面板记录上限。

## 展示选择

| 模式 | 过程卡 | 最终正文 |
| --- | --- | --- |
| 默认兼容模式 | 完整流式正文和工具过程 | 原卡片，必要时文本兜底 |
| 独立答复 + full | 保留流式正文预览 | 另发独立答复 |
| 独立答复 + compact | 短状态、可折叠工具过程 | 另发独立答复，适合长文与多人群 |

配置 `streaming_card_start: first_answer` 后，preflight/compression 阶段不创建主流式卡；首张主卡直接带回答元素，从“正在生成答复”开始，不再短暂闪回“正在加载上下文”。默认值仍是兼容旧 Profile 的 `message_start`。

紧凑模式中的“生成完成 · Final answer follows”表示正文将由独立消息承载，**不等于正文送达回执**。回读状态 `verified` 表示服务端正文与预期发送载荷一致，不代表用户已阅读，也不保证不同客户端的排版完全相同。

将 [紧凑配置示例](examples/compact-progress.yaml) 合并到测试 Profile 的现有配置，不要整份覆盖。希望继续看正文流式预览时，把 `progress_card` 改成 `full`。回读校验另行显式启用，见 [示例](examples/verified-native-delivery.yaml) 与 [边界说明](docs/MAINTENANCE.md#verified-delivery)。

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
