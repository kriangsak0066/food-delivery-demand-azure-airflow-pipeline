IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'warehouse'
)
BEGIN
    EXEC('CREATE SCHEMA warehouse');
END;

IF OBJECT_ID('warehouse.dim_restaurant', 'U') IS NULL
BEGIN
    CREATE TABLE warehouse.dim_restaurant (
        restaurant_key INT IDENTITY(1,1) PRIMARY KEY,
        restaurant_id NVARCHAR(50) NOT NULL,
        restaurant_name NVARCHAR(255),
        subzone NVARCHAR(255),
        city NVARCHAR(100),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID('warehouse.dim_customer', 'U') IS NULL
BEGIN
    CREATE TABLE warehouse.dim_customer (
        customer_key INT IDENTITY(1,1) PRIMARY KEY,
        customer_id NVARCHAR(255) NOT NULL,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID('warehouse.fact_orders', 'U') IS NULL
BEGIN
    CREATE TABLE warehouse.fact_orders (
        order_key INT IDENTITY(1,1) PRIMARY KEY,
        order_id NVARCHAR(50) NOT NULL,
        restaurant_key INT NOT NULL,
        customer_key INT NOT NULL,
        order_placed_at DATETIME2,
        order_date DATE,
        order_hour INT,
        order_status NVARCHAR(50),
        delivery_partner NVARCHAR(50),
        distance_km DECIMAL(10, 2),
        items_in_order NVARCHAR(MAX),
        discount_construct NVARCHAR(255),
        bill_subtotal DECIMAL(12, 2),
        packaging_charges DECIMAL(12, 2),
        restaurant_discount_promo DECIMAL(12, 2),
        restaurant_discount_flat_offs_freebies_others DECIMAL(12, 2),
        gold_discount DECIMAL(12, 2),
        brand_pack_discount DECIMAL(12, 2),
        total_amount DECIMAL(12, 2),
        rating DECIMAL(3, 2),
        kpt_duration_minutes DECIMAL(10, 2),
        rider_wait_time_minutes DECIMAL(10, 2),
        order_ready_marked NVARCHAR(100),
        customer_complaint_tag NVARCHAR(255),
        source_file_name NVARCHAR(255),
        load_date DATE,
        loaded_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        CONSTRAINT fk_fact_orders_restaurant
            FOREIGN KEY (restaurant_key)
            REFERENCES warehouse.dim_restaurant(restaurant_key),
        CONSTRAINT fk_fact_orders_customer
            FOREIGN KEY (customer_key)
            REFERENCES warehouse.dim_customer(customer_key)
    );
END;