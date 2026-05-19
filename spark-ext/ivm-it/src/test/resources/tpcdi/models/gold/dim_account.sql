select
    md5(cast(concat(coalesce(cast(account_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(a.effective_timestamp as string), '_dbt_utils_surrogate_key_null_')) as string)) as sk_account_id,
    a.account_id,
    sk_broker_id,
    sk_customer_id,
    a.status,
    account_desc,
    tax_status,
    a.effective_timestamp,
    a.end_timestamp,
    a.is_current
from
    silver.accounts a
join
    gold.dim_customer c
    on a.customer_id = c.customer_id
    and a.effective_timestamp between c.effective_timestamp and c.end_timestamp
join
    gold.dim_broker b
    using (broker_id)
