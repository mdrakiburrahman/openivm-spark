select
    md5(cast(concat(coalesce(cast(company_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(effective_timestamp as string), '_dbt_utils_surrogate_key_null_')) as string)) as sk_company_id,
    company_id,
    status,
    name,
    industry,
    ceo,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    description,
    founding_date,
    sp_rating,
    case
        when sp_rating in (
            'BB', 'B', 'CCC', 'CC', 'C', 'D',
            'BB+', 'B+', 'CCC+', 'BB-', 'B-', 'CCC-'
        ) then true
        else false
    end as is_lowgrade,
    effective_timestamp,
    end_timestamp,
    is_current
from silver.companies
