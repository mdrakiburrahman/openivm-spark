select
    md5(cast(concat(coalesce(cast(employee_id as string), '_dbt_utils_surrogate_key_null_')) as string)) as sk_broker_id,
    employee_id as broker_id,
    manager_id,
    first_name,
    last_name,
    middle_initial,
    job_code,
    branch,
    office,
    phone
from
    silver.employees
