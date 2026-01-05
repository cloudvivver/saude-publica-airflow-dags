# Airflow DAGs - Saúde Pública PI

Repositório de DAGs (Directed Acyclic Graphs) do Apache Airflow para a Saúde Pública do Piauí.

## 🔄 Sincronização Automática

Este repositório é sincronizado automaticamente com o cluster Kubernetes via **GitSync**:

- ⏱️ Intervalo: A cada **60 segundos**
- 🌿 Branch: `main`
- 📦 Destino: `/opt/airflow/dags/current/` no cluster
- 🔗 URL do Airflow: https://airflow.saude.pi.gov.br

## 📁 Estrutura do Repositório

```
saude-publica-airflow-dags/
├── dags/                    # DAGs do Airflow
│   ├── __init__.py         # Arquivo de inicialização (obrigatório)
│   ├── exemplo_dag.py      # DAG de exemplo
│   └── sigtap/             # DAGs organizadas por domínio
│       ├── __init__.py
│       └── importacao.py
├── plugins/                 # Plugins customizados (opcional)
│   └── __init__.py
├── tests/                   # Testes unitários (opcional)
│   └── test_dags.py
├── requirements.txt         # Dependências Python
├── .gitignore
└── README.md
```

## 🚀 Como Desenvolver DAGs

### 1. Clone o repositório

```bash
git clone https://github.com/cloudvivver/saude-publica-airflow-dags.git
cd saude-publica-airflow-dags
```

### 2. Crie sua DAG

Crie um arquivo Python em `dags/`:

```bash
vim dags/minha_dag.py
```

Exemplo de DAG:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'saude-pi',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'minha_dag',
    default_args=default_args,
    description='Minha primeira DAG',
    schedule='0 8 * * *',  # Diariamente às 8h
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['saude-pi'],
) as dag:

    task = BashOperator(
        task_id='hello',
        bash_command='echo "Hello from Airflow!"',
    )
```

### 3. Commit e Push

```bash
git add dags/minha_dag.py
git commit -m "feat: adicionar minha DAG"
git push origin main
```

### 4. Aguarde a Sincronização

- ⏱️ GitSync sincroniza em até **60 segundos**
- 🔍 Verifique no Airflow: https://airflow.saude.pi.gov.br
- ✅ Sua DAG aparecerá automaticamente na lista!

## 📋 Boas Práticas

### 1. Sempre use `catchup=False`

```python
with DAG(
    'minha_dag',
    catchup=False,  # Não executar runs passadas
    ...
) as dag:
```

### 2. Use tags para organização

```python
tags=['sigtap', 'importacao', 'producao']
```

### 3. Configure retries

```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}
```

### 4. Documente suas DAGs

```python
dag.doc_md = """
# Minha DAG

Descrição detalhada do que esta DAG faz...
"""
```

### 5. Organize por domínio

```
dags/
├── sigtap/
│   ├── __init__.py
│   └── importacao_sigtap.py
├── bi/
│   ├── __init__.py
│   └── dashboard_diario.py
└── etl/
    ├── __init__.py
    └── limpar_cache.py
```

## 🧪 Testando DAGs Localmente

### Validar sintaxe

```bash
python dags/minha_dag.py
```

### Testar com Airflow local (opcional)

```bash
# Instalar Airflow localmente
pip install apache-airflow==3.0.6

# Testar DAG
airflow dags test minha_dag 2026-01-05
```

## 📦 Dependências Python

Adicione dependências no `requirements.txt`:

```txt
pandas==2.1.4
requests==2.31.0
boto3==1.34.17
```

**Nota**: Após adicionar dependências, será necessário rebuildar a imagem do Airflow ou instalá-las via startup script.

## 🔍 Troubleshooting

### DAG não aparece no Airflow

1. **Verificar logs do GitSync**:
   ```bash
   kubectl logs -n saude-airflow -l component=gitsync -f
   ```

2. **Verificar se o arquivo está no pod**:
   ```bash
   kubectl exec -n saude-airflow deployment/airflow-scheduler -- \
     ls -la /opt/airflow/dags/current/dags/
   ```

3. **Verificar erros de importação** na UI do Airflow:
   - Acesse: https://airflow.saude.pi.gov.br
   - Menu: Admin > Import Errors

### DAG com erro de sintaxe

```bash
# Validar localmente
python dags/minha_dag.py

# Se não houver erro, o arquivo está OK!
```

### Forçar sincronização imediata

```bash
# Reiniciar pod do GitSync
kubectl delete pod -n saude-airflow -l component=gitsync
```

## 🛠️ Comandos Úteis

### Via kubectl

```bash
# Listar DAGs
kubectl exec -n saude-airflow deployment/airflow-scheduler -- \
  airflow dags list

# Testar DAG
kubectl exec -n saude-airflow deployment/airflow-scheduler -- \
  airflow dags test minha_dag 2026-01-05

# Ver logs do scheduler
kubectl logs -n saude-airflow -l component=scheduler -f
```

### Via Helper Script (no repositório k8s)

```bash
# No diretório: /home/cristiano/projetos/saude/k8s/saude-publica-airflow/

./scripts/airflow-helper.sh status
./scripts/airflow-helper.sh list-dags
./scripts/airflow-helper.sh trigger minha_dag
./scripts/airflow-helper.sh logs-scheduler
```

## 📚 Recursos

- **Airflow UI**: https://airflow.saude.pi.gov.br
- **Documentação Oficial**: https://airflow.apache.org/docs/apache-airflow/3.0.6/
- **Repositório K8s**: `/home/cristiano/projetos/saude/k8s/saude-publica-airflow/`
- **Guia de Desenvolvimento**: `DESENVOLVIMENTO_DAGS.md` (no repo k8s)

## 📝 Changelog

- **2026-01-05**: Setup inicial do repositório
  - Estrutura básica criada
  - DAG de exemplo adicionada
  - GitSync configurado

## 👥 Contribuindo

1. Crie uma branch para sua feature: `git checkout -b feature/minha-feature`
2. Desenvolva e teste sua DAG
3. Commit: `git commit -m "feat: adicionar nova DAG"`
4. Push: `git push origin feature/minha-feature`
5. Crie um Pull Request para `main`

## 📄 Licença

Uso interno - Secretaria de Saúde do Piauí
