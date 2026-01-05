"""
DAG de Exemplo - Saúde Pública PI
Autor: Saúde PI
Data: 2026-01-05
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Configurações padrão
default_args = {
    'owner': 'saude-pi',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    dag_id='exemplo_dag',
    default_args=default_args,
    description='DAG de exemplo para teste do GitSync',
    schedule='0 8 * * *',  # Executa às 8h todos os dias
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['exemplo', 'saude-pi', 'teste'],
) as dag:

    # Task 1: Início
    inicio = EmptyOperator(
        task_id='inicio',
    )

    # Task 2: Verificação
    verificar = BashOperator(
        task_id='verificar_ambiente',
        bash_command='''
            echo "===================================="
            echo "DAG de Exemplo - Saúde Pública PI"
            echo "===================================="
            echo "Data/Hora: $(date)"
            echo "Hostname: $(hostname)"
            echo "Python: $(python --version)"
            echo "Usuário: $(whoami)"
            echo "Diretório: $(pwd)"
            echo "===================================="
        ''',
    )

    # Task 3: Processamento Python
    def processar_dados(**context):
        """Função de exemplo para processamento"""
        import json

        execution_date = context['execution_date']
        run_id = context['run_id']

        print("=" * 50)
        print("🚀 Iniciando processamento...")
        print(f"📅 Data de execução: {execution_date}")
        print(f"🆔 Run ID: {run_id}")
        print("=" * 50)

        # Simular processamento
        dados = {
            'status': 'sucesso',
            'timestamp': execution_date.isoformat(),
            'mensagem': 'GitSync funcionando perfeitamente!',
            'total_registros': 100,
        }

        print(f"✅ Dados processados:")
        print(json.dumps(dados, indent=2, ensure_ascii=False))

        return dados

    processar = PythonOperator(
        task_id='processar_dados',
        python_callable=processar_dados,
    )

    # Task 4: Finalização
    finalizar = BashOperator(
        task_id='finalizar',
        bash_command='echo "✅ Pipeline executado com sucesso via GitSync!"',
    )

    # Fluxo de execução
    inicio >> verificar >> processar >> finalizar


# Documentação da DAG
dag.doc_md = """
# DAG de Exemplo - GitSync

Esta DAG foi criada para testar a sincronização automática via GitSync.

## Como funciona:

1. **início**: Marca o início da execução
2. **verificar_ambiente**: Mostra informações do ambiente
3. **processar_dados**: Simula processamento de dados
4. **finalizar**: Finaliza o pipeline

## Sincronização:

Qualquer mudança commitada no repositório GitHub será sincronizada automaticamente
em até 60 segundos!

## Repositório:

https://github.com/cloudvivver/saude-publica-airflow-dags
"""
