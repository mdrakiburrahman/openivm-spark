select
    md5(cast(concat(coalesce(cast(trade_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(t.effective_timestamp as string), '_dbt_utils_surrogate_key_null_')) as string)) as sk_trade_id,
    trade_id,
    trade_status as status,
    transaction_type,
    trade_type as type,
    executor_name as executed_by,
    t.effective_timestamp,
    t.end_timestamp,
    t.is_current
from
    silver.trades_history t
