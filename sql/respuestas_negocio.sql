-- Challenge 1

SELECT o.seller_id,
       c.nombre,
       c.apellido,
       c.email
FROM Customer AS c
LEFT JOIN Orders AS o ON c.customer_id = o.seller_id AND (o.created_at BETWEEN '2020-01-01' AND '2020-01-31')
WHERE DATE_FORMAT(c.birthdate, '%m-%d') = DATE_FORMAT(CURDATE(), '%m-%d')
GROUP BY o.seller_id,
         c.nombre,
         c.apellido,
         c.email
HAVING COUNT(o.id) > 1500;

-- Challenge 2

WITH sales_data AS (
    SELECT 
        c.customer_id,
        c.nombre,
        c.apellido,
        EXTRACT(MONTH FROM o.created_at) AS month,
        SUM(o.id) AS quantity_orders,
        SUM(o.quantity) AS quantity_items,
        SUM(o.value) AS total_amount
    FROM Customer AS c
    LEFT JOIN Orders AS o ON c.customer_id = o.seller_id AND (o.created_at BETWEEN '2020-01-01' AND '2020-12-31')
    LEFT JOIN Items AS i ON o.item_id = i.id
    LEFT JOIN Category AS cat ON i.category_id = cat.id AND cat.path = 'Celulares'
    GROUP BY
        month,
        c.customer_id,
        c.nombre,
        c.apellido
)
SELECT 
    month,
    RANK() OVER(PARTITION BY month ORDER BY total_amount DESC) AS seller_rank
    customer_id,
    nombre,
    apellido,
    quantity_orders,
    quantity_items,
    total_amount,
FROM sales_data
WHERE seller_rank <= 5
ORDER BY month, seller_rank;

-- Challenge 3

CREATE TABLE HisotricalItemSnapshot (
    id INT AUTO_INCREMENT PRIMARY KEY,
    price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    snapshot_date DATE NOT NULL
);

-- Procedure to reprocess a single, specific date

CREATE PROCEDURE PopulateHistoricalItemSnapshot(IN var_date DATE)
BEGIN
    DELETE FROM HistoricalItemSnapshot WHERE snapshot_date = var_date;

    -- First, we'll order rows from the history table related to the
    -- day we want to reprocess
    WITH ordered AS (
        SELECT
            id,
            price,
            status,
            updated_at,
            ROW_NUMBER() OVER (PARTITION BY id, DATE(updated_at) ORDER BY updated_at DESC) AS row_number
        FROM ItemHistory
        WHERE DATE(updated_at) = var_date
    ),
    latest_data AS (
        -- We'll then select the latest snapshot. It's important to note that this SELECT
        -- statement will only retrieve rows updated during the specified date.
        SELECT 
            id,
            price,
            status,
            DATE(updated_at) AS snapshot_date
        FROM ordered
        WHERE row_number = 1;

        UNION ALL

        -- In this second SELECT statement, we'll retrieve the latest snapshot of the rows which
        -- were last updated before the specified date
        SELECT
            id,
            price,
            status,
            DATE(updated_at) AS snapshot_date
        FROM ItemHistory
        WHERE id NOT IN (SELECT id FROM ordered)
        AND DATE(updated_at) < var_date;
    )

    -- Finally, we'll MERGE the data into the table
    MERGE INTO HistoricalItemSnapshot (id, price, status, snapshot_date) AS HIS
    USING latest_data AS SRC
    ON (HIS.id = SRC.id AND HIS.snapshot_date = SRC.snapshot_date)
    WHEN NOT MATCHED THEN
        INSERT (id, price, status, snapshot_date)
        VALUES (SRC.id, SRC.price, SRC.status, SRC.snapshot_date)
END;

-- Standalone call
CALL PopulateHistoricalItemSnapshot('2024-03-03');

-- Or schedule
CREATE EVENT daily_snapshot
ON SCHEDULE EVERY 1 DAY
STARTS DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
DO
  CALL PopulateHistoricalItemSnapshot(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

-- Full query for reprocessing the whole dataset

DELETE FROM HistoricalItemSnapshot WHERE 1=1;

INSERT INTO HistoricalItemSnapshot (id, price, status, snapshot_date)
WITH ordered AS (
    SELECT
            id,
            price,
            status,
            updated_at,
            ROW_NUMBER() OVER (PARTITION BY id, DATE(updated_at) ORDER BY updated_at DESC) AS row_number
        FROM ItemHistory
)
SELECT 
    id,
    price,
    status,
    DATE(updated_at)
FROM ordered
WHERE row_number = 1;