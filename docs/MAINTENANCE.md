# Fork 维护约定

本 fork 保留 MIT 许可证和原作者署名，可以公开改进与深度定制。Fork-specific 改动在自己的仓库维护；向原作者贡献时，另行挑选通用的小改动提交 PR。

## 代码和配置的边界

- 公开：通用插件代码、离线测试、无凭据示例、变更说明。
- 私有：真实 app 凭据、chat/user/message ID、Profile、日志、会话数据库和交付 outbox。
- 各机器共用经审核的代码版本，各 Profile 保留自己的配置与数据。不要维护多份无来源的热补丁目录。
- 可继续沿用 `github_sync` 作为主分支；该名字不代表自动同步。功能分支通过本仓库的 PR 合并。

## 更新上游

`upstream` 指向 Aowen-Nowor，`origin` 指向 Moonawn。定期检查上游提交，先在集成分支 merge 或 cherry-pick，再跑完整离线测试和固定版本的 Hermes 兼容测试。冲突要按行为审查，不能用“全部采用上游”覆盖自己的交付逻辑。

旧的定时 Gitee 强制同步、自动 release 和飞书通知 workflow 已移除。新的 CI 仅验证代码，仓库权限为只读，checkout 不保存 Git 凭据。CI 不能部署机器人或向群发送测试消息。

## 展示策略

`final_delivery: separate_message` 将过程卡与正文分离。`progress_card: compact` 只在该模式生效，避免把兼容模式中唯一的正文隐藏。推荐收起过程面板，关闭 reasoning 展示；想看逐步正文时使用 `progress_card: full`，并适当调低刷新频率。

配置初值：正文刷新 800ms、面板刷新 1000ms。它们是易读性起点，不是平台限流保证；高并发仍需观察错误率和时延。紧凑模式以“生成完成 · Final answer follows”明确提示独立正文仍在后续投递，不改变保存的终稿，也不把过程状态当交付回执。

卡片实体创建后还要经过一次 IM 发布。发布请求对同一次重试链复用 UUID；即使服务端已经接收而客户端丢失 ACK，也不会因为插件内部重试产生两张加载卡。进程在卡片创建与发布之间崩溃仍可能留下不可见的 CardKit 实体；进程在卡片已经可见后永久挂起或退出，也可能留下未关闭的“生成中”卡片，当前内存态插件无法在重启后恢复这类卡片生命周期。

## 长会话与 Hermes 自动压缩

长会话在开始生成正文前，Hermes 会同步运行 preflight context compression。固定兼容基线 v2026.8.13、v2026.8.16.2 和 v2026.8.27 均把从 `auxiliary.compression.timeout` 读取的辅助模型超时下限钳制为 300 秒；仅把该配置写成更小的值不能缩短这段等待。摘要模型失败后还可能回退到主模型，因此用户看到的首字等待可能明显长于模型真正生成答案的时间。

Hermes 的 host wrapper 可用下面的配置限制前台等待；这是超时保护，不是压缩质量修复：

```yaml
compression:
  context_timeout_seconds: 45
  context_total_ceiling_seconds: 90
  progress_notices: true
```

超时后 Hermes 返回原始上下文继续本轮，commit fence 会阻止迟到的 worker 改写会话数据库；worker 本身不能被 Python 安全强杀，仍会在小型 compression thread pool 中继续到 provider 返回。连续失败会触发 60/300/900 秒冷却，避免每轮都重复等待，但会话仍然过长，之后仍有 provider context overflow、后台 worker 占满和响应质量下降风险。本插件只抑制 fence 已取消后迟到的“compaction complete”误报，不改变 Hermes 的压缩调度或 provider 调用。

长期方案是为 compression 配置快速、稳定且上下文足够的 auxiliary model/provider，并观察其延迟和失败率；在明确的工作阶段边界主动 `/new`，避免单个 session 无限增长。不要把 45/90 秒保护理解为已经成功压缩。

## verified-delivery

此实验模式仅面向 **原生 FeishuAdapter**，不是 relay 的已验证交付方案。默认关闭；不要在 relay 部署中启用。需要同时设置：

```yaml
hermes_lark_streaming:
  final_delivery: separate_message
  verified_delivery: true
gateway:
  delivery_ledger: false
```

这是为了只有一个自动恢复发送者；不要让 Hermes 通用 ledger 与本插件 sender 同时重发正文。仅使用基础独立答复模式时，保留 Hermes 通用 ledger 的原有设置。

Outbox 在 `$HERMES_HOME/delivery/feishu-outbox.sqlite3`，保存原始终稿、每段实际发送载荷、路由、UUID、message_id 和校验状态。目录权限 0700、文件权限 0600；数据仍是明文，必须按私有聊天记录保护。SQLite 不是跨机器的共享队列，也没有自动清理历史记录的策略。

回读校验比较预期**发送载荷**，包括 Markdown 所需的格式化；它不把服务端渲染后的摘要或卡片预览当完整正文。服务端若不提供所需字段，状态不会被假定为成功。

- 入站 event ID 或显式 `metadata.hls_delivery_ref` 标识一次交付。共享回复锚点不代表同一轮；缺少交付标识时，每次调用按独立操作处理。
- 网络结果不明时，在保守的 50 分钟窗口内复用原 UUID；已拿到 message_id 后只回读，不重发该段。
- 重试有次数与时间上限。超窗、路由不符、正文不符或被撤回时进入 `needs_attention`，停止自动补发，并尝试发出一次状态提示。
- 停止/重启可恢复已持久化任务，但模型返回到 outbox 落盘之前的进程崩溃不在保证范围内。
- Profile 路径与 app 身份参与 scope。不要随意更改 scope、跨 Profile 复制数据库或批量重置状态；迁移需单独审查。
- 卡片 API 的 ACK、文本发送 ACK、正文回读确认和用户已读是四个不同层次。本插件不承诺端到端 exactly-once 或用户已读。

未启用 `verified_delivery` 时，最终正文仍由 Hermes 原生 Feishu sender 负责。当前固定 Hermes 基线的原生 sender 使用通用 `truncate_message` 分片，边界处会 `lstrip()`，可能丢失有意义的空白；网络结果不明时，内部重试还会生成新的 UUID，存在服务端已接收却重复发送的窗口。普通短文不容易触发，但要求字节级完整的长文、代码、表格或大段空白不能视为已验证交付。原生 Feishu 部署如重视这类内容，应在测试 Profile 按上述互斥配置启用本插件 verified delivery，再做权限、限流和真实客户端 canary。

## 验证与发布

`python scripts/test_offline.py tests -q` 验证插件本身；以固定 Hermes checkout 运行原生发送器、重试、队列与兼容用例。测试 runner 禁止 Python socket 网络访问，且不使用真实 Profile。

CI 的 Hermes 基线固定为：

| Release | Commit |
| --- | --- |
| v2026.8.13 | `f80f453ae0679347e38abc917c7f94f717bf96c5` |
| v2026.8.16.2 | `7339f5f160db5c96657a3bab60151227cc61f66c` |
| v2026.8.27 | `5fc308a70719a83cccdbba4c0e39c23f5a8239d5` |

兼容检查中的两个旧 public reaction 方法可能被标为可选缺失；这不是交付用例被跳过。真实的权限、限流、客户端展示与部署组合仍需 canary 验证。

每个候选版本先提交自己的 draft PR，说明改变的行为、测试结果、已知限制和回退方式。通过 review、CI 和实际 canary 后再考虑发布稳定 tag；不要把开发分支自动部署到生产。
