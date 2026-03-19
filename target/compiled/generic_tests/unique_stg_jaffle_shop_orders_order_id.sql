
    
    

select
    order_id as unique_field,
    count(*) as n_records

from analytics.dbt_amatos.stg_jaffle_shop_orders
where order_id is not null
group by order_id
having count(*) > 1


