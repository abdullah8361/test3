from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def data_quality_check(**kwargs):
    # Implement data quality check logic here
    pass

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('P2', default_args=default_args, schedule_interval='@monthly') as dag:

    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    with TaskGroup('task_group_1') as task_group_1:
        task_1 = BigQueryInsertJobOperator(
            task_id='query_1',
            configuration={
                "query": {
                    "query": """
                        WITH MonthlySales AS (
                            SELECT 
                                p.product_id,
                                p.product_name,
                                SUM(s.quantity_sold) AS total_quantity_sold
                            FROM 
                                sales s
                            JOIN 
                                products p ON s.product_id = p.product_id
                            WHERE 
                                s.sale_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' 
                                AND s.sale_date < DATE_TRUNC('month', CURRENT_DATE) 
                            GROUP BY 
                                p.product_id, p.product_name
                        ),
                        TopFiveProducts AS (
                            SELECT 
                                product_id, 
                                product_name, 
                                total_quantity_sold,
                                RANK() OVER (ORDER BY total_quantity_sold DESC) AS sales_rank
                            FROM 
                                MonthlySales
                        )
                        SELECT 
                            product_id, 
                            product_name, 
                            total_quantity_sold
                        FROM 
                            TopFiveProducts
                        WHERE 
                            sales_rank <= 5;
                    """,
                    "useLegacySql": False,
                }
            }
        )

        quality_check_1 = PythonOperator(
            task_id='quality_check_1',
            python_callable=data_quality_check
        )

        task_1 >> quality_check_1

    with TaskGroup('task_group_2') as task_group_2:
        task_2 = BigQueryInsertJobOperator(
            task_id='query_2',
            configuration={
                "query": {
                    "query": """
                        WITH SalesLastMonth AS (
                            SELECT 
                                product_id,
                                SUM(quantity) AS total_quantity
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
                                total_quantity,
                                RANK() OVER (ORDER BY total_quantity DESC) AS rank
                            FROM 
                                SalesLastMonth
                        )
                        SELECT 
                            product_id,
                            total_quantity
                        FROM 
                            TopFiveProducts
                        WHERE 
                            rank <= 5;
                    """,
                    "useLegacySql": False,
                }
            }
        )

        quality_check_2 = PythonOperator(
            task_id='quality_check_2',
            python_callable=data_quality_check
        )

        task_2 >> quality_check_2

    with TaskGroup('task_group_3') as task_group_3:
        task_3 = BigQueryInsertJobOperator(
            task_id='query_3',
            configuration={
                "query": {
                    "query": """
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
                            t.sales_rank <= 5;
                    """,
                    "useLegacySql": False,
                }
            }
        )

        quality_check_3 = PythonOperator(
            task_id='quality_check_3',
            python_callable=data_quality_check
        )

        task_3 >> quality_check_3

    start >> [task_group_1, task_group_2, task_group_3] >> end