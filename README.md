# nanobot-payment

监控Gmail收件箱，抓取DBS/POSB PayNow收据邮件，自动匹配 `payment_monitor/payments.db` 里的待付订单并标记为paid。

## 配置

- 邮箱：`gybot588@gmail.com`（Gmail App Password，IMAP）
- 轮询间隔：30秒
- 发件人过滤：目前为空（允许所有来源）。实际使用前在 `.env` 里把 `IMAP_SENDER` 填上真正的DBS发件人地址，比如 `DBS_PayNow_Advice@dbs.com`，否则任何人的邮件只要body格式像就会触发。
- 凭据文件：`.env`（已被 `.gitignore`）

## 常用命令

查看运行状态：

```bash
pgrep -af payment_monitor.monitor
```

看日志（有邮件匹配或错误时才输出）：

```bash
tail -f ~/nanobot-payment/logs/monitor.log
```

停止：

```bash
pkill -f payment_monitor.monitor
```

启动/重启：

```bash
cd ~/nanobot-payment && nohup ./run_monitor.sh > logs/monitor.log 2>&1 &
```

查看DB当前订单状态：

```bash
cd ~/nanobot-payment && .venv/bin/python -c "from payment_monitor.db import connect
for r in connect().execute('SELECT * FROM pending_payments'): print(dict(r))"
```

重置DB（恢复6条pending mock订单）：

```bash
cd ~/nanobot-payment && .venv/bin/python -m payment_monitor.db
```

## 测试

用另一个邮箱给 `gybot588@gmail.com` 发一封模拟DBS格式的邮件，body包含：

```
You have received SGD 8.50 via PayNow on 18 Mar 2026 20:38 SGT.
From: YANG QU
```

30秒后 `logs/monitor.log` 应出现 `[PAID] order ORD-1001 ...`。

## 注意事项

- 邮件被处理后会被标记为已读（`\Seen`），重启后不会重复处理。
- 当前是nohup后台进程，**Linux重启后不会自动拉起**。需要开机自启告诉我，我帮你配systemd服务。
- 上面那条多行python命令，在粘贴到终端时**不要带前置缩进空格**，否则会报 `IndentationError`。
- `.env`、`.venv/`、`payments.db` 已加入 `.gitignore`，不会被提交到GitHub。
