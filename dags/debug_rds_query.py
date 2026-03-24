from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

def run_debug_query():
    import psycopg2
    conn = psycopg2.connect(
        host='proxy-db-viverdb.proxy-cb8m6qcy2cyh.sa-east-1.rds.amazonaws.com',
        port=5432,
        dbname='homolog',
        user='postgres',
        password='rM9$8wRW8V*&lxzD',
        sslmode='require',
        connect_timeout=10
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    
    print("=== tb_atendimento for id_recepcao IN (71660, 71659) ===")
    cur.execute("""
    SELECT a.id, a.id_recepcao, a.dathorainicio, a.codespecialidade, a.codtipoatend, 
           a.indescutainicial, a.indclassificacaorisco
    FROM public.tb_atendimento a
    WHERE a.id_recepcao IN (71660, 71659)
    ORDER BY a.id_recepcao, a.dathorainicio
    """)
    for row in cur.fetchall():
        print(row)
    
    print("=== distinct codtipoatend from 2025-09-09 ===")
    cur.execute("""
    SELECT a.codtipoatend, a.codespecialidade, COUNT(*)
    FROM public.tb_atendimento a
    WHERE a.id_recepcao IN (
        SELECT id FROM public.tb_recepcao WHERE dathorainicio::date = '2025-09-09'
    )
    GROUP BY a.codtipoatend, a.codespecialidade
    ORDER BY COUNT(*) DESC
    LIMIT 20
    """)
    for row in cur.fetchall():
        print(row)
    
    conn.close()
    print("done")

with DAG('debug_rds_query', schedule=None, start_date=datetime(2024, 1, 1, tzinfo=timezone.utc), tags=['debug']) as dag:
    PythonOperator(task_id='query', python_callable=run_debug_query)
