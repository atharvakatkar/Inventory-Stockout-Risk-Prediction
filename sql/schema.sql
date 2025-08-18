-- sales table: transaction-level data
CREATE TABLE sales(
order_id SERIAL PRIMARY KEY,
order_date DATE NOT NULL,
sku_id VARCHAR(20) NOT NULL,
quantity_sold INTEGER NOT NULL,
unit_price NUMERIC(10,2) NOT NULL,
warehouse_id VARCHAR(10) NOT NULL,
promotion_flag BOOLEAN DEFAULT FALSE
);

-- inventory snapshot: current stock info
CREATE TABLE inventory(
sku_id VARCHAR(20) NOT NULL,
warehouse_id VARCHAR(10) NOT NULL,
current_stock INTEGER NOT NULL,
reserved_stock INTEGER DEFAULT 0,
unit_cost NUMERIC(10,2) NOT NULL,
unit_price NUMERIC(10,2) NOT NULL,
PRIMARY KEY(sku_id, warehouse_id)
);

--purchase orders: inbound stock from suppliers
CREATE TABLE purchase_orders(
po_id SERIAL PRIMARY KEY,
sku_id VARCHAR(20) NOT NULL,
supplier_id VARCHAR(10) NOT NULL,
po_date DATE NOT NULL,
expected_delivery_date DATE NOT NULL,
actual_delivery_date DATE,
quantity_ordered INTEGER NOT NULL
);

--suppliers: basic supplier info and lead time override
CREATE TABLE suppliers(
supplier_id VARCHAR(10) PRIMARY KEY,
supplier_name VARCHAR(100) NOT NULL,
lead_time_days_override INTEGER --if null we will calc from POs
);

--sku master: product metadata, categories, default lead time
CREATE TABLE sku_master(
sku_id VARCHAR(20) PRIMARY KEY,
category VARCHAR(50),
subcategory VARCHAR(50),
lead_time_default INTEGER
);

create table supplier_sku_map(
sku_id varchar(20) not null,
supplier_id varchar(10) not null,
primary key (sku_id, supplier_id)
);