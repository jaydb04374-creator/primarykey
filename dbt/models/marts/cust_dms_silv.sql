{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SSN',
        database='dev',
        schema='test2_silver',
        alias='cust_dms_silv'
    )
}}

-- SCD Type 1: keep only the latest version of each customer in silver.
-- Bronze (DMS) may hold several records for the same person (same SSN,
-- one row per change captured by DMS), so we dedupe to the most recent
-- record per SSN by insert_datetime_bronze before merging.
-- dbt's merge strategy on unique_key='SSN' performs the same
-- "update all columns when matched / insert when not matched" logic
-- as an SCD Type 1 MERGE.

with bronze_deduped as (

    select
        DMS_id,
        fname,
        lname,
        SSN,
        email,
        insert_datetime_bronze
    from (
        select
            *,
            row_number() over (
                partition by SSN
                order by insert_datetime_bronze desc, DMS_id desc
            ) as rn
        from {{ source('test1_bronze', 'customers_dms') }}
    )
    where rn = 1

)

select
    DMS_id,
    fname,
    lname,
    SSN,
    email,
    insert_datetime_bronze
from bronze_deduped
