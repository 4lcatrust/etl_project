CREATE TABLE IF NOT EXISTS public.customer (
	customer_key VARCHAR(50) NULL,
	"name" VARCHAR(50) NULL,
	contact_no VARCHAR NULL,
	nid VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS public.item (
	item_key VARCHAR(50) NULL,
	item_name VARCHAR(50) NULL,
	"desc" VARCHAR(50) NULL,
	unit_price FLOAT4 NULL,
	man_country VARCHAR(50) NULL,
	supplier VARCHAR(50) NULL,
	unit VARCHAR(50) NULL
);

CREATE TABLE IF NOT EXISTS public.payment (
	payment_key VARCHAR(50) NULL,
	trans_type VARCHAR(50) NULL,
	bank_name VARCHAR(50) NULL
);

CREATE TABLE IF NOT EXISTS public.store (
	store_key VARCHAR(50) NULL,
	division VARCHAR(50) NULL,
	district VARCHAR(50) NULL,
	upazila VARCHAR(50) NULL
);

CREATE TABLE IF NOT EXISTS public.time (
	time_key VARCHAR(50) NULL,
	"date" VARCHAR(50) NULL,
	"hour" INT4 NULL,
	"day" INT4 NULL,
	week VARCHAR(50) NULL,
	"month" INT4 NULL,
	quarter VARCHAR(50) NULL,
	"year" INT4 NULL
);

CREATE TABLE IF NOT EXISTS public.transactions (
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

\COPY transactions FROM '/backup/transactions.csv' DELIMITER ',' CSV HEADER;
\COPY item FROM '/backup/item.csv' DELIMITER ',' CSV HEADER;
\COPY time FROM '/backup/time.csv' DELIMITER ',' CSV HEADER;
\COPY payment FROM '/backup/payment.csv' DELIMITER ',' CSV HEADER;
\COPY customer FROM '/backup/customer.csv' DELIMITER ',' CSV HEADER;
\COPY store FROM '/backup/store.csv' DELIMITER ',' CSV HEADER;
