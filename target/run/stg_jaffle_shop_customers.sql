
  create or replace   view analytics.dbt_amatos.stg_jaffle_shop_customers
  
  
  
  
  as (
    select
        id as customer_id,
        first_name,
        last_name

    from raw.jaffle_shop.customers 
    -- grabbing from our source and table name
  );

