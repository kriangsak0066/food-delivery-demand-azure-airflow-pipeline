IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'staging'
)
BEGIN
    EXEC('CREATE SCHEMA staging');
END;

IF OBJECT_ID('staging.stg_food_delivery_orders', 'U') IS NULL
BEGIN
    CREATE TABLE staging.stg_food_delivery_orders (
        restaurant_id NVARCHAR(50),
        restaurant_name NVARCHAR(255),
        subzone NVARCHAR(255),
        city NVARCHAR(100),
        order_id NVARCHAR(50),
        order_placed_at DATETIME2,
        order_status NVARCHAR(50),
        delivery NVARCHAR(50),
        distance DECIMAL(10, 2),
        items_in_order NVARCHAR(MAX),
        instructions NVARCHAR(MAX),
        discount_construct NVARCHAR(255),
        bill_subtotal DECIMAL(12, 2),
        packaging_charges DECIMAL(12, 2),
        restaurant_discount_promo DECIMAL(12, 2),
        restaurant_discount_flat_offs_freebies_others DECIMAL(12, 2),
        gold_discount DECIMAL(12, 2),
        brand_pack_discount DECIMAL(12, 2),
        total DECIMAL(12, 2),
        rating DECIMAL(3, 2),
        review NVARCHAR(MAX),
        cancellation_rejection_reason NVARCHAR(500),
        restaurant_compensation_cancellation DECIMAL(12, 2),
        restaurant_penalty_rejection DECIMAL(12, 2),
        kpt_duration_minutes DECIMAL(10, 2),
        rider_wait_time_minutes DECIMAL(10, 2),
        order_ready_marked NVARCHAR(100),
        customer_complaint_tag NVARCHAR(255),
        customer_id NVARCHAR(255),
        source_file_name NVARCHAR(255),
        load_date DATE,
        loaded_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;