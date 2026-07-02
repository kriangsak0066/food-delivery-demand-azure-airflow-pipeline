IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'mart'
)
BEGIN
    EXEC('CREATE SCHEMA mart');
END;
GO

CREATE OR ALTER VIEW mart.mart_daily_orders AS
SELECT
    order_date,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    AVG(distance_km) AS avg_distance_km,
    AVG(kpt_duration_minutes) AS avg_kpt_minutes,
    AVG(rider_wait_time_minutes) AS avg_rider_wait_minutes,
    SUM(CASE WHEN order_status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(CASE WHEN order_status <> 'Delivered' THEN 1 ELSE 0 END) AS non_delivered_orders
FROM warehouse.fact_orders
GROUP BY order_date;
GO

CREATE OR ALTER VIEW mart.mart_hourly_demand AS
SELECT
    order_hour,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    AVG(kpt_duration_minutes) AS avg_kpt_minutes,
    AVG(rider_wait_time_minutes) AS avg_rider_wait_minutes
FROM warehouse.fact_orders
GROUP BY order_hour;
GO

CREATE OR ALTER VIEW mart.mart_restaurant_performance AS
SELECT
    r.restaurant_id,
    r.restaurant_name,
    r.subzone,
    r.city,
    COUNT(*) AS total_orders,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_order_value,
    AVG(f.rating) AS avg_rating,
    AVG(f.kpt_duration_minutes) AS avg_kpt_minutes,
    AVG(f.rider_wait_time_minutes) AS avg_rider_wait_minutes
FROM warehouse.fact_orders f
INNER JOIN warehouse.dim_restaurant r
    ON f.restaurant_key = r.restaurant_key
GROUP BY
    r.restaurant_id,
    r.restaurant_name,
    r.subzone,
    r.city;
GO

CREATE OR ALTER VIEW mart.mart_delivery_sla AS
SELECT
    order_date,
    order_hour,
    delivery_partner,
    COUNT(*) AS total_orders,
    AVG(distance_km) AS avg_distance_km,
    AVG(kpt_duration_minutes) AS avg_kpt_minutes,
    AVG(rider_wait_time_minutes) AS avg_rider_wait_minutes,
    SUM(CASE WHEN rider_wait_time_minutes > 10 THEN 1 ELSE 0 END) AS high_rider_wait_orders,
    CAST(
        100.0 * SUM(CASE WHEN rider_wait_time_minutes > 10 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
        AS DECIMAL(10, 2)
    ) AS high_rider_wait_rate_pct
FROM warehouse.fact_orders
GROUP BY
    order_date,
    order_hour,
    delivery_partner;
GO