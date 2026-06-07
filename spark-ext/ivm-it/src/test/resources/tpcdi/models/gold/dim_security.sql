with s1 as (
    select
        symbol,
        issue_type as issue,
        s.status,
        s.name,
        exchange_id,
        sk_company_id,
        shares_outstanding,
        first_trade_date,
        first_exchange_date,
        dividend,
        s.effective_timestamp,
        s.end_timestamp,
        s.is_current
    from
        silver.securities s
    join
        gold.dim_company c
    on
        s.company_id = c.company_id
    and
        s.effective_timestamp between c.effective_timestamp and c.end_timestamp
)
select
    md5(cast(concat(coalesce(cast(symbol as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(effective_timestamp as string), '_dbt_utils_surrogate_key_null_')) as string)) as sk_security_id,
    *
from
    s1
