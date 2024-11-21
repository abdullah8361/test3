WITH LastMonthSales AS (
    SELECT 
        product_id,
        SUM(quantity) AS total_quantity
    FROM 
        sales
    WHERE 
        sale_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
        AND sale_date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 
        product_id
),
TopFiveProducts AS (
    SELECT 
        product_id,
        total_quantity,
        RANK() OVER (ORDER BY total_quantity DESC) AS product_rank
    FROM 
        LastMonthSales
)
SELECT 
    p.product_id,
    p.product_name,
    t.total_quantity
FROM 
    TopFiveProducts t
JOIN 
    products p ON t.product_id = p.product_id
WHERE 
    t.product_rank <= 5;