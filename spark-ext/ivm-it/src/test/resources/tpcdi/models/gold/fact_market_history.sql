with __dbt__cte__wrk_company_financials as (
-- Work: company financials pre-joined for fact_market_history
select
    f.company_id,
    sk_company_id,
    f.eps,
    f.revenue,
    f.effective_timestamp,
    f.end_timestamp,
    f.is_current
from silver.financials f
join gold.dim_company c
    on f.company_id = c.company_id
    and f.effective_timestamp between c.effective_timestamp and c.end_timestamp
) select
    s.sk_security_id,
    s.sk_company_id,
    dmh.dm_date as sk_date_id,
    (s.dividend / dmh.dm_close) / 100 as yield,
    fifty_two_week_high,
    fifty_two_week_high_date as sk_fifty_two_week_high_date,
    fifty_two_week_low,
    fifty_two_week_low_date as sk_fifty_two_week_low_date,
    dm_close as closeprice,
    dm_high as dayhigh,
    dm_low as daylow,
    dm_vol as volume
from silver.daily_market dmh
join gold.dim_security s
    on s.symbol = dmh.dm_s_symb
    and dmh.dm_date between s.effective_timestamp and s.end_timestamp
left join __dbt__cte__wrk_company_financials f
    using (sk_company_id)
