-- created_at: 2026-03-16T15:20:30.632178+00:00
-- finished_at: 2026-03-16T15:20:30.789155+00:00
-- elapsed: 156ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c31198-3203-d4db-0007-647e00018276
-- desc: execute adapter call
show terse schemas in database analytics
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-16T15:20:31.765337+00:00
-- finished_at: 2026-03-16T15:20:31.846308+00:00
-- elapsed: 80ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c31198-3203-d4db-0007-647e0001827a
-- desc: Get table schema
describe table "RAW"."JAFFLE_SHOP"."CUSTOMERS";
-- created_at: 2026-03-16T15:20:31.771476+00:00
-- finished_at: 2026-03-16T15:20:31.889647+00:00
-- elapsed: 118ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c31198-3203-d4f3-0007-647e0001b07a
-- desc: Get table schema
describe table "RAW"."JAFFLE_SHOP"."ORDERS";
-- created_at: 2026-03-16T15:20:33.048022+00:00
-- finished_at: 2026-03-16T15:20:33.244525+00:00
-- elapsed: 196ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c31198-3203-d4f3-0007-647e0001b07e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ANALYTICS"."DBT_AMATOS" LIMIT 10000;
-- created_at: 2026-03-16T15:20:33.250949+00:00
-- finished_at: 2026-03-16T15:20:33.409927+00:00
-- elapsed: 158ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c31198-3203-d4db-0007-647e0001827e
-- desc: execute adapter call
drop table if exists "ANALYTICS"."DBT_AMATOS"."STG_JAFFLE_SHOP_ORDERS" cascade
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.stg_jaffle_shop_orders", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-16T15:20:33.414636+00:00
-- finished_at: 2026-03-16T15:20:33.655492+00:00
-- elapsed: 240ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c31198-3203-d4f3-0007-647e0001b082
-- desc: execute adapter call
create or replace   view analytics.dbt_amatos.stg_jaffle_shop_orders
  
  
  
  
  as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from raw.jaffle_shop.orders
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.stg_jaffle_shop_orders", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-16T15:20:33.853444+00:00
-- finished_at: 2026-03-16T15:20:33.974164+00:00
-- elapsed: 120ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c31198-3203-d4db-0007-647e00018282
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ANALYTICS"."DBT_AMATOS" LIMIT 10000;
-- created_at: 2026-03-16T15:20:33.977328+00:00
-- finished_at: 2026-03-16T15:20:34.134955+00:00
-- elapsed: 157ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c31198-3203-d4fd-0007-647e0001d06a
-- desc: execute adapter call
drop table if exists "ANALYTICS"."DBT_AMATOS"."STG_JAFFLE_SHOP_CUSTOMERS" cascade
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.stg_jaffle_shop_customers", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-16T15:20:34.139492+00:00
-- finished_at: 2026-03-16T15:20:34.346351+00:00
-- elapsed: 206ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c31198-3203-d4f3-0007-647e0001b086
-- desc: execute adapter call
create or replace   view analytics.dbt_amatos.stg_jaffle_shop_customers
  
  
  
  
  as (
    select
        id as customer_id,
        first_name,
        last_name

    from raw.jaffle_shop.customers
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.stg_jaffle_shop_customers", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-16T15:20:34.353124+00:00
-- finished_at: 2026-03-16T15:20:35.955973+00:00
-- elapsed: 1.6s
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.dim_customers
-- query_id: 01c31198-3203-d4db-0007-647e00018286
-- desc: execute adapter call
create or replace transient  table analytics.dbt_amatos.dim_customers
    
    
    
    as (with customers as (

select * from analytics.dbt_amatos.stg_jaffle_shop_customers
),

orders as (

select * from analytics.dbt_amatos.stg_jaffle_shop_orders    

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.dim_customers", "profile_name": "default", "target_name": "dev"} */;
