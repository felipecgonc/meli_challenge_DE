-- Desafio 1

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

-- Desafio 2

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

-- Desafio 3

CREATE TABLE HisotricalItemSnapshot (
    id INT AUTO_INCREMENT PRIMARY KEY,
    price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    snapshot_date DATE NOT NULL
);

-- Procedure para (Re)Processar um só dia

CREATE PROCEDURE PopulateHistoricalItemSnapshot(IN var_date DATE)
BEGIN
    DELETE FROM HistoricalItemSnapshot WHERE snapshot_date = var_date;

    -- Primeiro, vamos ordenar os registros da tabela histórica
    -- referentes ao dia que se quer reprocessar
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
        -- Selecionamos então, o snapshot mais recente. Importante notar que esse primeiro
        -- SELECT buscará apenas registros que foram atualizados na data especificada.
        SELECT 
            id,
            price,
            status,
            DATE(updated_at) AS snapshot_date
        FROM ordered
        WHERE row_number = 1;

        UNION ALL

        -- No segundo SELECT, buscamos o snapshot mais recente dos registros cuja última
        -- atualização é anterior à data especificada.
        SELECT
            id,
            price,
            status,
            DATE(updated_at) AS snapshot_date
        FROM ItemHistory
        WHERE id NOT IN (SELECT id FROM ordered)
        AND DATE(updated_at) < var_date;
    )

    -- Por fim, fazemos o MERGE na tabela
    MERGE INTO HistoricalItemSnapshot (id, price, status, snapshot_date) AS HIS
    USING latest_data AS SRC
    ON HIS.id = SRC.id
    WHEN NOT MATCHED THEN
        INSERT (id, price, status, snapshot_date)
        VALUES (SRC.id, SRC.price, SRC.status, SRC.snapshot_date)
END;

CALL PopulateHistoricalItemSnapshot('2024-03-03');

-- Reprocessamento histórico da tabela

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
