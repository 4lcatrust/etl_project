-- MySQL source seed (database `app`). Deliberately uses types the Postgres source
-- doesn't (DECIMAL, TINYINT(1)/boolean, DATETIME) to exercise BronzeExtract's type
-- mapping across a second dialect. Runs once on first container init.

CREATE TABLE IF NOT EXISTS customers (
  id            BIGINT       NOT NULL PRIMARY KEY,
  full_name     VARCHAR(100) NOT NULL,
  email         VARCHAR(120) NOT NULL,
  is_active     TINYINT(1)   NOT NULL DEFAULT 1,
  credit_limit  DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
  id           BIGINT       NOT NULL PRIMARY KEY,
  customer_id  BIGINT       NOT NULL,
  amount       DECIMAL(10,2) NOT NULL,
  status       VARCHAR(20)  NOT NULL,
  ordered_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (id, full_name, email, is_active, credit_limit, created_at) VALUES
  (1, 'Ayu Lestari',   'ayu@example.com',   1, 1500.00, '2026-01-05 09:12:00'),
  (2, 'Budi Santoso',  'budi@example.com',  1, 3200.50, '2026-01-08 14:03:00'),
  (3, 'Citra Dewi',    'citra@example.com', 0,  800.00, '2026-02-01 10:45:00'),
  (4, 'Dian Pratama',  'dian@example.com',  1, 5000.00, '2026-02-14 16:30:00'),
  (5, 'Eka Putri',     'eka@example.com',   1, 2750.75, '2026-03-03 08:20:00');

INSERT INTO orders (id, customer_id, amount, status, ordered_at) VALUES
  (101, 1,  250.00, 'paid',      '2026-03-10 11:00:00'),
  (102, 1,  120.50, 'paid',      '2026-03-12 13:15:00'),
  (103, 2,  980.00, 'paid',      '2026-03-15 09:40:00'),
  (104, 3,   45.25, 'refunded',  '2026-03-18 17:05:00'),
  (105, 4, 1500.00, 'paid',      '2026-03-20 10:10:00'),
  (106, 4,  300.00, 'pending',   '2026-03-22 12:00:00'),
  (107, 5,  675.75, 'paid',      '2026-03-25 15:30:00'),
  (108, 2,  210.00, 'pending',   '2026-03-28 08:55:00');
