# Moonawn fork 的安装与升级

本文件描述本 fork。原作者的仓库、Gitee 同步和 PyPI 包不是本 fork 的发布渠道。

当前开发候选为 `1.7.0+moonawn.8`。Python 需要 3.11 或更高；CardKit SDK 至少为 `lark-oapi>=1.4.24`。不要仅根据版本号大小就覆盖已有的私有修复。

## 先在独立环境验证

1. 克隆 `https://github.com/Moonawn/hermes-lark-streaming.git`，检出待审核的完整 commit SHA，记录 `git rev-parse HEAD`。默认分支可能尚未包含未合并 PR 的改动。
2. 运行 `python scripts/test_offline.py tests -q`，再用目标 Hermes 版本运行原生 adapter 兼容测试。依赖见 `requirements-test.txt`。
3. 为 canary 建立独立 Profile，只在私有配置中放入操作者选择的测试 bot 和测试群。不要复制生产 Profile、历史或数据库进仓库。
4. 将审核后的代码作为该 Profile 唯一一份 `hermes-lark-streaming` 插件加载。保留旧插件目录和配置备份，避免修改共享目录影响其他 bot。
5. 推荐先合并 `examples/single-card-streaming.yaml` 中所需选项，验证正文始终在同一 CardKit 内流式并完成。独立答复与原生回读校验属于替代模式，应单独评估后再开启。
6. 仅在操作者授权的测试渠道进行真实发送，按下方验收。现有网关的部署与重启按当前授权范围执行，不自动扩散到其他 Profile。

## 真实环境验收

- 短回答与明显更短的终稿：核对最后一段，不以 token 动画结束为准。
- 超长中文、emoji、表格、代码块与换行：核对所有分段及尾标。
- 同一话题的连续两轮、同一消息的两个 final 阶段：不能漏掉第二段结果，也不能错误去重。
- 工具调用期间停止、取消或断网：过程卡应结束或记录关闭失败，终稿状态应明确。
- 使用回读校验时，模拟发送 ACK 丢失与 GET 失败：已知 message_id 只能继续读，不能重复发送原正文。
- 测试重启、未完成交付恢复、被撤回的引用和错误 chat/thread：不能跨目标投递。
- 确认 cron、后台任务、图片/文件、澄清交互和其他平台仍保持原行为。

服务端正文回读与客户端截图是不同证据。离线测试或 CardKit 的成功响应不能替代两者。

## 可回退升级

每次升级记录插件 commit、Hermes image/commit、配置改动与回退目录。先小范围 canary，再逐个 Profile 调整代码挂载；不要直接覆盖公共挂载目录。回退时恢复前一版代码和配套配置，保留待处理 outbox 供审查；不要清空交付记录后盲目重发。

只更新本 fork 的代码；不要使用上游 Gitee 自动同步、`reset --hard upstream/...`，也不要将生产目录中的脏修改作为唯一备份。更完整的分支、验证与私有数据边界见 [MAINTENANCE.md](MAINTENANCE.md)。
