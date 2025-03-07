CREATE TABLE IF NOT EXISTS public.dim_customer (
	customer_key VARCHAR(50) NULL,
	"name" VARCHAR(50) NULL,
	contact_no VARCHAR NULL,
	nid VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS public.dim_item (
	item_key VARCHAR(50) NULL,
	item_name VARCHAR(50) NULL,
	"desc" VARCHAR(50) NULL,
	unit_price FLOAT4 NULL,
	man_country VARCHAR(50) NULL,
	supplier VARCHAR(50) NULL,
	unit VARCHAR(50) NULL
);

CREATE TABLE IF NOT EXISTS public.dim_payment (
	payment_key VARCHAR(50) NULL,
	trans_type VARCHAR(50) NULL,
	bank_name VARCHAR(50) NULL
);

CREATE TABLE IF NOT EXISTS public.dim_store (
	store_key VARCHAR(50) NULL,
	division VARCHAR(50) NULL,
	district VARCHAR(50) NULL,
	upazila VARCHAR(50) NULL
);

CREATE TABLE IF NOT EXISTS public.dim_time (
	time_key VARCHAR(50) NULL,
	"date" VARCHAR(50) NULL,
	"hour" INT4 NULL,
	"day" INT4 NULL,
	week VARCHAR(50) NULL,
	"month" INT4 NULL,
	quarter VARCHAR(50) NULL,
	"year" INT4 NULL
);

CREATE TABLE IF NOT EXISTS public.fct_transactions (
	payment_key VARCHAR(50) NULL,
	customer_key VARCHAR(50) NULL,
	time_key VARCHAR(50) NULL,
	item_key VARCHAR(50) NULL,
	store_key VARCHAR(50) NULL,
	quantity INT4 NULL,
	unit VARCHAR(50) NULL,
	unit_price INT4 NULL,
	total_price INT4 NULL
);

\COPY fct_transactions FROM '/backup/fct_transactions.csv' DELIMITER ',' CSV HEADER;
\COPY dim_item FROM '/backup/dim_item.csv' DELIMITER ',' CSV HEADER;
\COPY dim_time FROM '/backup/dim_time.csv' DELIMITER ',' CSV HEADER;
\COPY dim_payment FROM '/backup/dim_payment.csv' DELIMITER ',' CSV HEADER;
\COPY dim_customer FROM '/backup/dim_customer.csv' DELIMITER ',' CSV HEADER;
\COPY dim_store FROM '/backup/dim_store.csv' DELIMITER ',' CSV HEADER;
