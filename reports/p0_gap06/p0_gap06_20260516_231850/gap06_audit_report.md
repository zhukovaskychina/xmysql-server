# GAP-06 安全姿态审计

- run_id: 20260516_231850
- repo: /Users/zhukovasky/GolandProjects/xmysql-server

## 1. 配置文件快照

### conf/default.ini
35:dev_bypass_password_auth = false
4:bind-address = 0.0.0.0

### conf/my.ini
12:dev_bypass_password_auth = false
3:bind-address = 0.0.0.0

### conf/jdbc_local.ini
16:dev_bypass_password_auth = false
6:bind-address	= 127.0.0.1


## 2. 代码审计回归
command: go test ./server/conf -run 'TestShouldRejectDevBypass|TestNormalizeBindAddress_Localhost|TestIsLocalBindAddress' -count=1
ok  	github.com/zhukovaskychina/xmysql-server/server/conf	0.810s
[PASS] conf package regression tests

## 3. 审计结论
- 关注点：非本地监听下不允许开启 dev_bypass_password_auth
- 结论请基于上面 PASS/FAIL 结果与配置快照确认
- 如果 command 报错，请先修复 conf 包单测后再复验
