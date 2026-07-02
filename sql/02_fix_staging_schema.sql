IF COL_LENGTH('staging.stg_food_delivery_orders', 'items_in_order') IS NOT NULL
BEGIN
    ALTER TABLE staging.stg_food_delivery_orders
    ALTER COLUMN items_in_order NVARCHAR(MAX);
END;

IF COL_LENGTH('staging.stg_food_delivery_orders', 'customer_id') IS NOT NULL
BEGIN
    ALTER TABLE staging.stg_food_delivery_orders
    ALTER COLUMN customer_id NVARCHAR(255);
END;