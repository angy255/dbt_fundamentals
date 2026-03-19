
    
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