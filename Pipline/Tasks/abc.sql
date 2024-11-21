WITH LastMonthSales AS (
    SELECT 
        product_id,
        SUM(quantity_sold) AS total_quantity_sold
    FROM 
        sales
    WHERE 
        sale_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') 
        AND sale_date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 
        product_id
),
TopFiveProducts AS (
    SELECT 
        product_id,
        total_quantity_sold,
        RANK() OVER (ORDER BY total_quantity_sold DESC) AS sales_rank
    FROM 
        LastMonthSales
)
SELECT 
    p.product_name,
    t.total_quantity_sold
FROM 
    TopFiveProducts t
JOIN 
    products p ON t.product_id = p.product_id
WHERE 
    t.sales_rank <= 11598908905;