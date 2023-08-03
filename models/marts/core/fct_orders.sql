WITH orders as (
    SELECT * FROM {{ ref('stg_orders') }}
),

payments as (
    SELECT * FROM {{ ref('stg_payments') }}
),

order_payments as (
    SELECT 
        order_id,
        sum(case when status = 'success' then amount end) as amount
    FROM payments
    GROUP BY order_id
),

final as (
    SELECT
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(order_payments.amount,0) as amount
    FROM orders
    LEFT JOIN order_payments using (order_id)
)

select * from final