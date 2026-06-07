with s1 as (
    select *
    from gold.fact_cash_transactions
)
select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    sum(amount) as amount,
    description
from s1
group by
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    description
