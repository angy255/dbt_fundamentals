-- created_at: 2026-03-19T02:45:00.108011+00:00
-- finished_at: 2026-03-19T02:45:00.240740+00:00
-- elapsed: 132ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c31f85-3203-d69d-0007-647e000362a6
-- desc: execute adapter call
show terse schemas in database analytics
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:00.710535+00:00
-- finished_at: 2026-03-19T02:45:00.821802+00:00
-- elapsed: 111ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_stripe_payments
-- query_id: 01c31f85-3203-d69d-0007-647e000362aa
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ANALYTICS"."DBT_AMATOS" LIMIT 10000;
-- created_at: 2026-03-19T02:45:00.723020+00:00
-- finished_at: 2026-03-19T02:45:00.858030+00:00
-- elapsed: 135ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.source_not_null_jaffle_shop_customers_id.50aa22178f
-- query_id: 01c31f85-3203-d69d-0007-647e000362ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from raw.jaffle_shop.customers
where id is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.source_not_null_jaffle_shop_customers_id.50aa22178f", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:00.714160+00:00
-- finished_at: 2026-03-19T02:45:00.872984+00:00
-- elapsed: 158ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c01e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ANALYTICS"."DBT_AMATOS" LIMIT 10000;
-- created_at: 2026-03-19T02:45:00.826582+00:00
-- finished_at: 2026-03-19T02:45:01.308801+00:00
-- elapsed: 482ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_stripe_payments
-- query_id: 01c31f85-3203-d712-0007-647e00038152
-- desc: execute adapter call
create or replace   view analytics.dbt_amatos.stg_stripe_payments
  
  
  
  
  as (
    select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,

    --  amount is stored in cents, convert it to dollars
    amount / 100 as amount,
    created as created_at

from raw.stripe.payment
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.stg_stripe_payments", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:00.876707+00:00
-- finished_at: 2026-03-19T02:45:01.460239+00:00
-- elapsed: 583ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_orders
-- query_id: 01c31f85-3203-d69d-0007-647e000362b2
-- desc: execute adapter call
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
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.stg_jaffle_shop_orders", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:01.445260+00:00
-- finished_at: 2026-03-19T02:45:01.570430+00:00
-- elapsed: 125ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.source_unique_jaffle_shop_customers_id.2777a7933e
-- query_id: 01c31f85-3203-d712-0007-647e00038156
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    id as unique_field,
    count(*) as n_records

from raw.jaffle_shop.customers
where id is not null
group by id
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.source_unique_jaffle_shop_customers_id.2777a7933e", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:01.576163+00:00
-- finished_at: 2026-03-19T02:45:01.801993+00:00
-- elapsed: 225ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.stg_jaffle_shop_customers
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c022
-- desc: execute adapter call
create or replace   view analytics.dbt_amatos.stg_jaffle_shop_customers
  
  
  
  
  as (
    select
        id as customer_id,
        first_name,
        last_name

    from raw.jaffle_shop.customers 
    -- grabbing from our source and table name
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.stg_jaffle_shop_customers", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:01.733634+00:00
-- finished_at: 2026-03-19T02:45:01.850055+00:00
-- elapsed: 116ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.fct_orders
-- query_id: 01c31f85-3203-d69d-0007-647e000362b6
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ANALYTICS"."DBT_AMATOS" LIMIT 10000;
-- created_at: 2026-03-19T02:45:01.808789+00:00
-- finished_at: 2026-03-19T02:45:02.336690+00:00
-- elapsed: 527ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.not_null_stg_jaffle_shop_customers_customer_id.1828149cc2
-- query_id: 01c31f85-3203-d69d-0007-647e000362ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from analytics.dbt_amatos.stg_jaffle_shop_customers
where customer_id is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.not_null_stg_jaffle_shop_customers_customer_id.1828149cc2", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:01.808789+00:00
-- finished_at: 2026-03-19T02:45:02.460985+00:00
-- elapsed: 652ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.unique_stg_jaffle_shop_customers_customer_id.b9becc3999
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c02e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from analytics.dbt_amatos.stg_jaffle_shop_customers
where customer_id is not null
group by customer_id
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.unique_stg_jaffle_shop_customers_customer_id.b9becc3999", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:01.809554+00:00
-- finished_at: 2026-03-19T02:45:02.471676+00:00
-- elapsed: 662ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.relationships_stg_jaffle_shop__3a4c5b7e76605024bff9af72d6fbad4e.06e374b302
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c026
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select customer_id as from_field
    from analytics.dbt_amatos.stg_jaffle_shop_orders
    where customer_id is not null
),

parent as (
    select customer_id as to_field
    from analytics.dbt_amatos.stg_jaffle_shop_customers
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.relationships_stg_jaffle_shop__3a4c5b7e76605024bff9af72d6fbad4e.06e374b302", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:02.414613+00:00
-- finished_at: 2026-03-19T02:45:02.577383+00:00
-- elapsed: 162ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.unique_stg_jaffle_shop_orders_order_id.50aa8222ac
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c036
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    order_id as unique_field,
    count(*) as n_records

from analytics.dbt_amatos.stg_jaffle_shop_orders
where order_id is not null
group by order_id
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.unique_stg_jaffle_shop_orders_order_id.50aa8222ac", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:02.369688+00:00
-- finished_at: 2026-03-19T02:45:02.585158+00:00
-- elapsed: 215ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.assert_stg_stripe_payment_total_positive.1a8f153c72
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c032
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
  order_id,
  sum(amount) as total_amount
from analytics.dbt_amatos.stg_stripe_payments
group by 1
having total_amount < 0
  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.assert_stg_stripe_payment_total_positive.1a8f153c72", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:02.473163+00:00
-- finished_at: 2026-03-19T02:45:02.617351+00:00
-- elapsed: 144ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.not_null_stg_jaffle_shop_orders_order_id.489806f7af
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c03a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from analytics.dbt_amatos.stg_jaffle_shop_orders
where order_id is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.not_null_stg_jaffle_shop_orders_order_id.489806f7af", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:01.854773+00:00
-- finished_at: 2026-03-19T02:45:02.766649+00:00
-- elapsed: 911ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.fct_orders
-- query_id: 01c31f85-3203-d6d0-0007-647e0003c02a
-- desc: execute adapter call
create or replace transient  table analytics.dbt_amatos.fct_orders
    
    
    
    as (select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,

    -- amount is stored in cents, convert it to dollars
    amount / 100 as amount,
    created as created_at

from raw.stripe.payment 
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.jaffle_shop.fct_orders", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:02.448037+00:00
-- finished_at: 2026-03-19T02:45:02.878549+00:00
-- elapsed: 430ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.jaffle_shop.accepted_values_stg_jaffle_sho_9e737f1742bbf5f35cc658b19a560db5.a557d85787
-- query_id: 01c31f85-3203-d712-0007-647e0003815a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from analytics.dbt_amatos.stg_jaffle_shop_orders
    group by status

)

select *
from all_values
where value_field not in (
    'placed','shipped','completed','returned','return_pending'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.jaffle_shop.accepted_values_stg_jaffle_sho_9e737f1742bbf5f35cc658b19a560db5.a557d85787", "profile_name": "default", "target_name": "dev"} */;
-- created_at: 2026-03-19T02:45:02.886099+00:00
-- finished_at: 2026-03-19T02:45:03.678375+00:00
-- elapsed: 792ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.jaffle_shop.dim_customers
-- query_id: 01c31f85-3203-d712-0007-647e0003815e
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
