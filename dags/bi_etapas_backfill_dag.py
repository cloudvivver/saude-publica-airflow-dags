"""
DAG de backfill histórico de etapas de atendimento para o ClickHouse.

Estratégia:
  - Uma task por (município × dia) via KubernetesPodOperator
  - O Pod Rails roda `rake bi_etapas:backfill_dia DATA={{ ds }}`
  - APP_HOST injetado explicitamente (cada município tem o seu)
  - DATABASE_USERNAME/PASSWORD sobrescritos com usuário read-only (airflow_bi)
  - max_active_runs=1 → um dia por vez por município, sem pressão no RDS
  - schedule=None → trigger via CLI ou UI

Execução via CLI (recomendado para backfill inicial):
  kubectl exec -it deployment/airflow-scheduler -n saude-airflow -- \\
    airflow dags backfill \\
      --start-date 2024-01-01 \\
      --end-date   2025-12-31 \\
      --max-jobs   2 \\
      bi_etapas_backfill_piripiri

Para adicionar novo município: copiar o bloco make_backfill_dag() no final.
O Secret airflow-bi-db deve existir no namespace do município (criado via kubectl create secret).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


DEFAULT_ARGS = {
    "owner": "saude-bi",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

POD_CONTAINER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "200m", "memory": "512Mi"},
    limits={"cpu": "500m", "memory": "1Gi"},
)

GATEWAY_URL = "http://saude-bi-gateway.saude-bi.svc.cluster.local:8080"
RAILS_IMAGE = "961341521437.dkr.ecr.sa-east-1.amazonaws.com/saude-publica-web:master-83"


def make_backfill_dag(
    dag_id: str,
    municipio: str,
    namespace: str,
    app_host: str,
    rails_image: str = RAILS_IMAGE,
    start_date: datetime = datetime(2024, 1, 1),
) -> DAG:
    """
    Fábrica de DAG de backfill.

    O Pod herda DATABASE_HOST, DATABASE_NAME do ConfigMap `env` do namespace,
    mas substitui DATABASE_USERNAME/PASSWORD pelo usuário read-only `airflow_bi`
    (Secret `airflow-bi-db`). APP_HOST é injetado explicitamente.
    """

    # Fontes de variáveis de ambiente (ConfigMap do município + Secret read-only)
    env_from = [
        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="env")),
        # Sobrescreve DATABASE_USERNAME e DATABASE_PASSWORD com usuário read-only
        k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="airflow-bi-db")),
    ]

    # Variáveis explícitas (sobrescrevem o ConfigMap quando há conflito)
    env_vars = [
        k8s.V1EnvVar(name="APP_HOST", value=app_host),
        k8s.V1EnvVar(name="ATENDIMENTO_EVENTS_GATEWAY_URL", value=f"{GATEWAY_URL}/events"),
        k8s.V1EnvVar(name="PUBLISH_ATENDIMENTO_EVENTS", value="true"),
        k8s.V1EnvVar(name="BACKFILL_SLEEP_MS", value="150"),
        k8s.V1EnvVar(name="BACKFILL_BATCH_SIZE", value="500"),
        # Rails precisa de RAILS_ENV
        k8s.V1EnvVar(name="RAILS_ENV", value="production"),
    ]

    with DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        description=f"Backfill BI etapas — {municipio}",
        schedule=None,
        start_date=start_date,
        catchup=True,
        max_active_runs=1,
        tags=["bi-etapas", "backfill", municipio],
    ) as dag:

        KubernetesPodOperator(
            task_id="backfill_dia",
            name=f"bi-etapas-backfill-{municipio}",
            namespace=namespace,
            image=rails_image,
            image_pull_policy="IfNotPresent",
            cmds=["bundle", "exec", "rake"],
            arguments=["bi_etapas:backfill_dia", "DATA={{ ds }}"],
            env_from=env_from,
            env_vars=env_vars,
            get_logs=True,
            is_delete_operator_pod=True,
            startup_timeout_seconds=300,
            container_resources=POD_CONTAINER_RESOURCES,
            on_finish_action="delete_pod",
        )

    return dag


# ---------------------------------------------------------------------------
# Municípios — APP_HOST extraído do ConfigMap env de cada namespace
# ---------------------------------------------------------------------------

dag_piripiri = make_backfill_dag(
    dag_id="bi_etapas_backfill_piripiri",
    municipio="piripiri",
    namespace="piripiri",
    app_host="cuidarpi.piripiri.saude.pi.gov.br",
    rails_image="961341521437.dkr.ecr.sa-east-1.amazonaws.com/saude-publica-web:master-82",
)

dag_bomjesus = make_backfill_dag(
    dag_id="bi_etapas_backfill_bomjesus",
    municipio="bomjesus",
    namespace="bomjesus",
    app_host="cuidarpi.bomjesus.saude.pi.gov.br",
)

dag_campomaior = make_backfill_dag(
    dag_id="bi_etapas_backfill_campomaior",
    municipio="campomaior",
    namespace="campomaior",
    app_host="cuidarpi.campomaior.saude.pi.gov.br",
)

dag_caps = make_backfill_dag(
    dag_id="bi_etapas_backfill_caps",
    municipio="caps",
    namespace="caps",
    app_host="cuidarpi.caps.saude.pi.gov.br",
)

dag_cetea = make_backfill_dag(
    dag_id="bi_etapas_backfill_cetea",
    municipio="cetea",
    namespace="cetea",
    app_host="cetea.saude.pi.gov.br",
)

dag_corrente = make_backfill_dag(
    dag_id="bi_etapas_backfill_corrente",
    municipio="corrente",
    namespace="corrente",
    app_host="cuidarpi.corrente.saude.pi.gov.br",
)

dag_floriano = make_backfill_dag(
    dag_id="bi_etapas_backfill_floriano",
    municipio="floriano",
    namespace="floriano",
    app_host="cuidarpi.floriano.saude.pi.gov.br",
)

dag_parnaiba = make_backfill_dag(
    dag_id="bi_etapas_backfill_parnaiba",
    municipio="parnaiba",
    namespace="parnaiba",
    app_host="cuidarpi.parnaiba.saude.pi.gov.br",
)

dag_picos = make_backfill_dag(
    dag_id="bi_etapas_backfill_picos",
    municipio="picos",
    namespace="picos",
    app_host="cuidarpi.picos.saude.pi.gov.br",
)

dag_saojoao = make_backfill_dag(
    dag_id="bi_etapas_backfill_saojoao",
    municipio="saojoao",
    namespace="saojoao",
    app_host="cuidarpi.saojoao.saude.pi.gov.br",
)

dag_saoraimundononato = make_backfill_dag(
    dag_id="bi_etapas_backfill_saoraimundononato",
    municipio="saoraimundononato",
    namespace="saoraimundononato",
    app_host="cuidarpi.saoraimundononato.saude.pi.gov.br",
)

# apresentacao e treinamento: sem DATABASE_NAME configurado → skip backfill
# cetea: verificar se tem dados históricos relevantes antes de rodar
