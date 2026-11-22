# RushGo 支付与退款恢复

[简体中文](FINANCIAL_RECOVERY.md) | [English](FINANCIAL_RECOVERY.en.md)

Payment 在自己的 PostgreSQL 数据库中记录尚待履行的资金责任，以及后台任务使用的操作标识。负责托管支付的写入进程启动前，必须验证 Stripe TEST 凭据及其所属账户。任务启动时扫描一次，此后每秒扫描。每批最多认领四行，租约为 30 秒。任务先提交数据库事务，再以十秒超时调用支付方。对支付方的请求均在 Payment 事务之外执行。

创建、取消和退款都使用原 Payment 记录中已持久化的幂等键。租约到期可能使两个请求同时抵达支付方，但两者使用同一个键，针对同一个不可变目标。任务只有在认领令牌、隔离标识（fence）及租约仍有效时，才能在本地标记完成。迟到但已核实的凭据可以保留；过期的认领不能清除较新的租约。

退款结果未知时，任务会在 20 小时窗口内重放同一笔全额退款请求。已知退款则向支付方查询，按原交易的金额与币种核对累计成功金额。待处理、失败、已取消和需要额外操作的金额均不计入。仅收到 HTTP 成功响应或看到退款对象已创建时，状态为 `provider_pending`。已成功金额不足全额时维持 `attention`，不会发起替代退款或部分退款。若已核实的累计成功金额覆盖原交易全额，资金责任即可结清；已有的多笔部分退款也可计入该总额。

取消时先查询原 PaymentIntent。若原先创建操作的响应丢失，任务根据原 Payment 元数据查找对应对象。查不到对象时仍将结果视为未知，也不会创建替代 PaymentIntent。若取消期间发现已收款，任务会唤醒资金核对；对于已进入终态且无法确认的 Booking，这笔款项必须全额退回。

取消或退款发生暂时性失败时，任务从一秒开始按指数退避，上限为 256 秒，最多额外增加 25% 的随机延迟。首次认领取消或退款时重置尝试次数，常规核对轮询不计入次数。资金变更请求达到 32 次，或操作结果不明达到 20 小时，就转入 `attention` 或 `cancel_attention`。已知支付方正在处理的退款每分钟轮询一次，新的可信回执也会唤醒核对流程。进入待处理状态仍需承担资金责任，既不能据此判定已退款，也不能判定永远收不到款。

下面是仅针对 Payment 的限时只读查询，排除了冻结的请求参数、邮箱、客户端密钥和支付方原始响应。执行前确认目标是预期的 Payment 数据库；将账户、运行模式及不可变的操作标识与支付方经过认证的凭据逐一核对。不得删除或改写历史、生成新的操作键、从超时推断失败，或把人工退款当作自动流程的验收证据。结果依然不明确时，按获准的受控流程升级处理。

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '2s';
SELECT current_database(), current_user, clock_timestamp() AS observed_at;
SELECT id, booking_id, payment_generation, provider_account, livemode,
       price, currency, payment_intent_id, provider_outcome, provider_status,
       booking_disposition, create_state, cancel_state, reconciliation_state,
       refund_state, refund_amount_cents, refund_currency, refund_provider_id,
       refund_provider_status, refund_requested_at, refund_completed_at,
       operation_kind, claim_fence, lease_until, due_at, attempts
FROM payments
WHERE managed AND (reconciliation_state NOT IN ('sale_verified', 'resolved')
                   OR cancel_state = 'cancel_attention'
                   OR refund_state = 'attention')
ORDER BY COALESCE(refund_requested_at, created_at), id
LIMIT 200;
COMMIT;
```

监控指标 `rushgo.payment.financial.due`、`attention`、`oldest_age`（秒）和 `attempts`（仅按操作分类）不使用 Booking、Payment 或支付方对象 ID 作为标签。积压时长包含被阻塞的资金任务。汇总查询失败应返回错误，不能把它当作积压为零。[支付与恢复的验证范围](../../../PROJECT_STATUS.md)
