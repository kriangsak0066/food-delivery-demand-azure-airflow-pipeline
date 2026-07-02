-- Rebuild warehouse tables from staging.
-- During development, we fully reload warehouse to avoid duplicate dimension keys.

DELETE FROM warehouse.fact_orders;
DELETE FROM warehouse.dim_customer;
DELETE FROM warehouse.dim_restaurant;

-- Load restaurant dimension: one row per restaurant_id
INSERT INTO warehouse.dim_restaurant (
    restaurant_id,
    restaurant_name,
    subzone,
    city
)
SELECT
    restaurant_id,
    MAX(restaurant_name) AS restaurant_name,
    MAX(subzone) AS subzone,
    MAX(city) AS city
FROM staging.stg_food_delivery_orders
WHERE restaurant_id IS NOT NULL
GROUP BY restaurant_id;

-- Load customer dimension: one row per customer_id
INSERT INTO warehouse.dim_customer (
    customer_id
)
SELECT
    customer_id
FROM staging.stg_food_delivery_orders
WHERE customer_id IS NOT NULL
GROUP BY customer_id;

-- Load fact orders
INSERT INTO warehouse.fact_orders (
    order_id,
    restaurant_key,
    customer_key,
    order_placed_at,
    order_date,
    order_hour,
    order_status,
    delivery_partner,
    distance_km,
    items_in_order,
    discount_construct,
    bill_subtotal,
    packaging_charges,
    restaurant_discount_promo,
    restaurant_discount_flat_offs_freebies_others,
    gold_discount,
    brand_pack_discount,
    total_amount,
    rating,
    kpt_duration_minutes,
    rider_wait_time_minutes,
    order_ready_marked,
    customer_complaint_tag,
    source_file_name,
    load_date
)
SELECT
    s.order_id,
    r.restaurant_key,
    c.customer_key,
    s.order_placed_at,
    CAST(s.order_placed_at AS DATE) AS order_date,
    DATEPART(HOUR, s.order_placed_at) AS order_hour,
    s.order_status,
    s.delivery AS delivery_partner,
    s.distance AS distance_km,
    s.items_in_order,
    s.discount_construct,
    s.bill_subtotal,
    s.packaging_charges,
    s.restaurant_discount_promo,
    s.restaurant_discount_flat_offs_freebies_others,
    s.gold_discount,
    s.brand_pack_discount,
    s.total AS total_amount,
    s.rating,
    s.kpt_duration_minutes,
    s.rider_wait_time_minutes,
    s.order_ready_marked,
    s.customer_complaint_tag,
    s.source_file_name,
    s.load_date
FROM staging.stg_food_delivery_orders s
INNER JOIN warehouse.dim_restaurant r
    ON s.restaurant_id = r.restaurant_id
INNER JOIN warehouse.dim_customer c
    ON s.customer_id = c.customer_id;