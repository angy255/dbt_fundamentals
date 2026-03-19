
  create or replace   view analytics.dbt_amatos.stg_jaffle_shop_orders
  
  
  
  
  as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status,
        _etl_loaded_at
        
    from raw.jaffle_shop.orders
    -- this is our source macro, instead of hardcoding table reference
  );

