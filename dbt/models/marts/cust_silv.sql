{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SSN',
        database='dev',
        alias='cust_silv'
    )
}}

-- SCD Type 1: keep only the latest version of each customer in silver.
-- Bronze may hold several inserts for the same person (same SSN, increasing
-- custid), so we dedupe to the highest custid per SSN before merging.
-- dbt's merge strategy on unique_key='SSN' performs the same
-- "update all columns when matched / insert when not matched" logic
-- as the MERGE in the original notebook.

with bronze_deduped as (

    select
        custid,
        fname,
        lname,
        SSN,
        email
    from (
        select
            *,
            row_number() over (partition by SSN order by custid desc) as rn
        from {{ source('test1_bronze', 'customers') }}
    )
    where rn = 1

)

select
    custid,
    fname,
    lname,
    SSN,
    email
from bronze_deduped
