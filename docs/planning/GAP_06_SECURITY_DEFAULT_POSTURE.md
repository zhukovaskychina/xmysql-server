# GAP-06：默认安全姿态（免密）关闭与生产边界校验

## 目标

- 避免开发联调开关 `dev_bypass_password_auth=true` 被带入生产非本地监听场景。
- 明确区分“开发默认”和“生产默认”：
  - 本地联调示例可使用 `bind-address=127.0.0.1` + `dev_bypass_password_auth=true`
  - 生产默认使用 `dev_bypass_password_auth=false`

## 已落地变更

- 文件：`server/conf/config.go`
  - 在 `parseMysqldCfg` 中新增地址归一化与安全校验：
    - `normalizeBindAddress`：将 `localhost` 归一化到 `127.0.0.1`
    - `isLocalBindAddress`：仅允许回环地址（如 `127.0.0.1`、`::1`）作为开发联调场景
    - `shouldRejectDevBypass`：当 `dev_bypass_password_auth=true` 且绑定地址不是本地时，启动直接退出
  - 启动时输出安全异常日志：
    - `安全校验异常: 非本地监听下不允许开启 dev_bypass_password_auth`
- 文件：`server/conf/config_test.go`
  - 新增单测覆盖：
    - `TestNormalizeBindAddress_Localhost`
    - `TestIsLocalBindAddress`
    - `TestShouldRejectDevBypass`

## 验收依据（已执行）

1. 代码审阅证据
   - `server/conf/config.go`
   - `server/conf/config_test.go`
2. 本地触发验证（示例）
   - 非本地监听 + 开发免密：
     - 配置 `bind-address=0.0.0.0`，`dev_bypass_password_auth=true`
     - 启动时应直接退出
   - 本地监听 + 开发免密：
     - 配置 `bind-address=127.0.0.1`，`dev_bypass_password_auth=true`
     - 启动不应因为该项被拒绝
3. 已执行固定回归命令
   - `go test ./server/conf -run "TestShouldRejectDevBypass|TestNormalizeBindAddress_Localhost|TestIsLocalBindAddress" -count=1`
   - `./scripts/p0_gap06_security_audit.sh`
   - 结果归档：`reports/p0_gap06/p0_gap06_20260516_231850/gap06_audit_report.md`

## 未完成项

- `go test ./server/conf` 与 `./scripts/p0_gap06_security_audit.sh` 的执行报告已归档
- 生产环境完整“认证失败路径”联调报告（含错误码与链路日志）仍未在 GAP-06 中闭环，后续建议纳入 GAP-08
