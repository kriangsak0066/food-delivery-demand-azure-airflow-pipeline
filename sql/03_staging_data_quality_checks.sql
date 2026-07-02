-- Row count by load date
SELECT
    load_date,
    COUNT(*) AS row_count
FROM staging.stg_food_delivery_orders
GROUP BY load_date
ORDER BY load_date DESC;

-- Duplicate order_id check
SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM staging.stg_food_delivery_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Null critical fields check
SELECT
    SUM(CASE WHEN order_id IS NULL OR order_id = '' THEN 1 ELSE 0 END) AS null_order_id_count,
    SUM(CASE WHEN restaurant_id IS NULL OR restaurant_id = '' THEN 1 ELSE 0 END) AS null_restaurant_id_count,
    SUM(CASE WHEN customer_id IS NULL OR customer_id = '' THEN 1 ELSE 0 END) AS null_customer_id_count,
    SUM(CASE WHEN order_placed_at IS NULL THEN 1 ELSE 0 END) AS null_order_placed_at_count
FROM staging.stg_food_delivery_orders;

-- Numeric range check
SELECT
    SUM(CASE WHEN total < 0 THEN 1 ELSE 0 END) AS negative_total_count,
    SUM(CASE WHEN bill_subtotal < 0 THEN 1 ELSE 0 END) AS negative_bill_subtotal_count,
    SUM(CASE WHEN distance < 0 THEN 1 ELSE 0 END) AS negative_distance_count,
    SUM(CASE WHEN rating < 0 OR rating > 5 THEN 1 ELSE 0 END) AS invalid_rating_count
FROM staging.stg_food_delivery_orders;

-- Business distribution check
SELECT
    order_status,
    COUNT(*) AS order_count
FROM staging.stg_food_delivery_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- Delivery partner check
SELECT
    delivery,
    COUNT(*) AS order_count
FROM staging.stg_food_delivery_orders
GROUP BY delivery
ORDER BY order_count DESC;