select
  order_id,
  sum(amount) as total_amount
from analytics.dbt_amatos.stg_stripe_payments
group by 1
having total_amount < 0