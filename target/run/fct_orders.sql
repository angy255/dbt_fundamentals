
  
    



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
;




  