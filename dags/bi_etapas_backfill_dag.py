"""
DAG de backfill histórico de etapas de atendimento para o ClickHouse.

Estratégia:
  - PythonOperator puro: lê direto do RDS via psycopg2 (usuário airflow_bi read-only)
    e publica no gateway BI — sem dependência de imagem Rails.
  - Credenciais do banco: variáveis Airflow por município (bi_db_<municipio>_*)
    ou Secret k8s airflow-bi-db lido via env vars no scheduler.
  - max_active_runs=1 → um dia por vez por município, sem pressão no RDS
  - schedule=None → trigger via CLI ou UI

Execução via CLI:
  kubectl exec -it deployment/airflow-scheduler -n saude-airflow -- \\
    airflow dags trigger bi_etapas_backfill_piripiri --logical-date 2024-01-01

Para adicionar novo município: copiar o bloco make_backfill_dag() no final.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "saude-bi",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

GATEWAY_URL = "http://saude-bi-gateway.saude-bi.svc.cluster.local:8080"
BATCH_SIZE  = 500
SLEEP_MS    = 150

EVENTS_ORDER = [
    "RecepcaoIniciada",
    "RecepcaoFinalizada",
    "SenhaTotemEmitida",
    "ClassificacaoRiscoRegistrada",
    "AtendimentoIniciado",
    "AtendimentoFinalizado",
    "ObservacaoRegistrada",
    "AltaAmbulatorialRegistrada",
    "EvolucaoRegistrada",
    "RecepcaoInternacaoRegistrada",
    "AltaInternacaoRegistrada",
]


# ---------------------------------------------------------------------------
# Queries — SQL puro, espelho exato do BackfillDiaService Ruby
# ---------------------------------------------------------------------------

def _q_recepcao_iniciada(cur, dia_inicio, dia_fim, after_id, limit):
    # tb_recepcao não tem created_at — usa dathorainicio como data do evento
    cur.execute("""
        SELECT id, id AS id_recepcao, numprontuario, codmunicipio,
               codunidade, codsetor, dathorainicio
        FROM public.tb_recepcao
        WHERE dathorainicio BETWEEN %s AND %s AND id > %s
        ORDER BY id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_recepcao_finalizada(cur, dia_inicio, dia_fim, after_id, limit):
    cur.execute("""
        SELECT id, id AS id_recepcao, dathorafim
        FROM public.tb_recepcao
        WHERE dathorafim IS NOT NULL AND dathorafim BETWEEN %s AND %s AND id > %s
        ORDER BY id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_senha_totem_emitida(cur, dia_inicio, dia_fim, after_id, limit):
    # tb_recepcao.id_totem → sc_controladorfila.tb_servico_seq.id
    # data: svc.dathoragravacao (quando a senha foi gravada)
    cur.execute("""
        SELECT svc.id AS id, rec.id AS id_recepcao,
               rec.numprontuario, rec.codmunicipio, rec.codunidade, rec.codsetor,
               rec.dathorainicio, svc.dathoragravacao AS dathoragravacao_senha, svc.numseq
        FROM public.tb_recepcao rec
        INNER JOIN sc_controladorfila.tb_servico_seq svc ON svc.id = rec.id_totem
        WHERE svc.dathoragravacao BETWEEN %s AND %s AND svc.id > %s
        ORDER BY svc.id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_classificacao_risco_registrada(cur, dia_inicio, dia_fim, after_id, limit):
    cur.execute("""
        SELECT t.id, t.amb_recepcao_id AS id_recepcao,
               a.codmunicipio, a.codunidade, a.codsetor, t.created_at AS dathorainicio
        FROM public.amb_pat_triagem t
        INNER JOIN public.tb_atendimento a ON a.id = t.amb_atendimento_id
        WHERE t.created_at BETWEEN %s AND %s AND t.id > %s
        ORDER BY t.id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_atendimento_iniciado(cur, dia_inicio, dia_fim, after_id, limit):
    cur.execute("""
        SELECT id, id_recepcao, dathorainicio, codmunicipio, codunidade, codsetor,
               codprofissional, codespecialidade
        FROM public.tb_atendimento
        WHERE dathorainicio BETWEEN %s AND %s AND id > %s
        ORDER BY id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_atendimento_finalizado(cur, dia_inicio, dia_fim, after_id, limit):
    cur.execute("""
        SELECT id, id_recepcao, dathorafim, codprofissional, codespecialidade
        FROM public.tb_atendimento
        WHERE dathorafim IS NOT NULL AND dathorafim BETWEEN %s AND %s AND id > %s
        ORDER BY id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_observacao_registrada(cur, dia_inicio, dia_fim, after_id, limit):
    cur.execute("""
        SELECT a.id, a.id_recepcao, a.dathorafim AS data_observacao, a.id_motivofinalizacao
        FROM public.tb_atendimento a
        INNER JOIN public.tb_motivofinalizacaoatendimento m
               ON m.id = a.id_motivofinalizacao AND m.indmotivoobservacao = 'S'
        WHERE a.dathorafim IS NOT NULL AND a.dathorafim BETWEEN %s AND %s AND a.id > %s
        ORDER BY a.id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_alta_ambulatorial_registrada(cur, dia_inicio, dia_fim, after_id, limit):
    cur.execute("""
        SELECT a.id, a.id_recepcao, a.dathorafim AS data_alta_ambulatorial, a.id_motivofinalizacao
        FROM public.tb_atendimento a
        INNER JOIN public.tb_motivofinalizacaoatendimento m
               ON m.id = a.id_motivofinalizacao AND m.indmotivoobservacao = 'N'
        WHERE a.dathorafim IS NOT NULL AND a.dathorafim BETWEEN %s AND %s AND a.id > %s
        ORDER BY a.id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_evolucao_registrada(cur, dia_inicio, dia_fim, after_id, limit):
    cur.execute("""
        SELECT e.id, a.id_recepcao, e.data_evolucao,
               a.codmunicipio, a.codunidade, a.codsetor
        FROM public.upa_evolucao_atendimento e
        INNER JOIN public.tb_atendimento a ON a.id = e.amb_atendimento_id
        WHERE e.created_at BETWEEN %s AND %s AND e.id > %s
        ORDER BY e.id LIMIT %s
    """, (dia_inicio, dia_fim, after_id, limit))
    return cur.fetchall()


def _q_recepcao_internacao_registrada(cur, dia_inicio, dia_fim, after_id, limit):
    try:
        cur.execute("""
            SELECT id, id_recepcao, data_internacao
            FROM sc_hospital.hos_inh_recepcao_internacao
            WHERE data_internacao BETWEEN %s AND %s AND id > %s
            ORDER BY id LIMIT %s
        """, (dia_inicio, dia_fim, after_id, limit))
        return cur.fetchall()
    except Exception as e:
        cur.connection.rollback()
        log.warning("RecepcaoInternacaoRegistrada: tabela indisponível neste banco — %s", e)
        return []


def _q_alta_internacao_registrada(cur, dia_inicio, dia_fim, after_id, limit):
    return []


QUERY_FN = {
    "RecepcaoIniciada":             _q_recepcao_iniciada,
    "RecepcaoFinalizada":           _q_recepcao_finalizada,
    "SenhaTotemEmitida":            _q_senha_totem_emitida,
    "ClassificacaoRiscoRegistrada": _q_classificacao_risco_registrada,
    "AtendimentoIniciado":          _q_atendimento_iniciado,
    "AtendimentoFinalizado":        _q_atendimento_finalizado,
    "ObservacaoRegistrada":         _q_observacao_registrada,
    "AltaAmbulatorialRegistrada":   _q_alta_ambulatorial_registrada,
    "EvolucaoRegistrada":           _q_evolucao_registrada,
    "RecepcaoInternacaoRegistrada": _q_recepcao_internacao_registrada,
    "AltaInternacaoRegistrada":     _q_alta_internacao_registrada,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso(val):
    """Converte datetime/date para ISO8601 com timezone UTC, ou None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val.isoformat(timespec="milliseconds")
    if isinstance(val, date):
        return val.isoformat()
    return str(val)


def _coerce(v):
    """Converte tipos Python não-JSON-serializáveis para tipos básicos."""
    if v is None:
        return None
    if hasattr(v, 'isoformat'):
        return _iso(v)
    # psycopg2 retorna Decimal para NUMERIC — converte para int ou float
    if hasattr(v, 'is_integer'):  # Decimal
        return int(v) if v == int(v) else float(v)
    return v


def _row_to_dict(cursor_desc, row):
    return {cursor_desc[i].name: _coerce(v) for i, v in enumerate(row)}


def _post_batch(gateway_url, secret, events):
    body = json.dumps(events).encode()
    headers = {"Content-Type": "application/json"}
    if secret:
        sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        headers["X-Audit-Signature"] = sig
    req = urllib.request.Request(f"{gateway_url}/events/batch",
                                 data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status not in (200, 201, 202):
                log.warning("Gateway batch %s: %s", resp.status, resp.read()[:200])
    except Exception as e:
        log.error("post_batch falhou: %s", e)
        raise


def _backfill_dia(ds: str, municipio: str, cluster: str,
                  db_host: str, db_port: int, db_name: str,
                  db_user: str, db_password: str,
                  gateway_url: str, secret: str,
                  batch_size: int = BATCH_SIZE, sleep_ms: int = SLEEP_MS):
    """Lê um dia de eventos do RDS e publica no gateway BI."""
    dia = date.fromisoformat(ds)
    dia_inicio = datetime(dia.year, dia.month, dia.day, 0, 0, 0, tzinfo=timezone.utc)
    dia_fim    = datetime(dia.year, dia.month, dia.day, 23, 59, 59, 999999, tzinfo=timezone.utc)

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name,
        user=db_user, password=db_password,
        sslmode="require", connect_timeout=10,
    )
    conn.set_session(readonly=True, autocommit=True)

    stats = {}
    try:
        for event_type in EVENTS_ORDER:
            query_fn = QUERY_FN[event_type]
            after_id = 0
            count = 0
            t0 = time.monotonic()

            with conn.cursor() as cur:
                while True:
                    rows = query_fn(cur, dia_inicio, dia_fim, after_id, batch_size)
                    if not rows:
                        break

                    events = []
                    for row in rows:
                        d = _row_to_dict(cur.description, row)
                        events.append({
                            "event_id":     f"backfill:{event_type}:{d['id']}",
                            "event_type":   event_type,
                            "timestamp":    datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                            "aggregate_id": d.get("id_recepcao"),
                            "payload":      {k: v for k, v in d.items() if k != "id"}
                                            | {"cluster": cluster},
                        })

                    _post_batch(gateway_url, secret, events)
                    count += len(rows)
                    after_id = rows[-1][0]  # primeira coluna é sempre id (cursor)

                    if len(rows) < batch_size:
                        break
                    if sleep_ms > 0:
                        time.sleep(sleep_ms / 1000.0)

            elapsed = time.monotonic() - t0
            stats[event_type] = count
            log.info("[BackfillDia] %s %s: %d eventos em %.1fs", ds, event_type, count, elapsed)
    finally:
        conn.close()

    log.info("[BackfillDia] %s %s TOTAL: %s", ds, municipio, stats)
    return stats


# ---------------------------------------------------------------------------
# Fábrica de DAGs
# ---------------------------------------------------------------------------

def make_backfill_dag(
    dag_id: str,
    municipio: str,
    db_host: str,
    db_name: str,
    app_host: str,
    db_port: int = 5432,
    start_date: datetime = datetime(2024, 1, 1),
) -> DAG:
    """
    Cria um DAG de backfill para um município.

    Credenciais do banco lidas de Airflow Variables:
      bi_db_<municipio>_user     (default: airflow_bi)
      bi_db_<municipio>_password

    Ou das env vars do scheduler:
      BI_DB_PASSWORD  (fallback genérico)
    """
    from airflow.models import Variable

    def _task(**context):
        ds = context["ds"]
        secret = os.environ.get("AUDITORIA_GATEWAY_SECRET") or \
                 os.environ.get("ATENDIMENTO_EVENTS_GATEWAY_SECRET", "")
        db_user = Variable.get(f"bi_db_{municipio}_user", default_var="airflow_bi")
        db_pass = Variable.get(f"bi_db_{municipio}_password",
                               default_var=os.environ.get("BI_DB_PASSWORD", "airflow_bi_2024!"))
        _backfill_dia(
            ds=ds, municipio=municipio, cluster=municipio,
            db_host=db_host, db_port=db_port, db_name=db_name,
            db_user=db_user, db_password=db_pass,
            gateway_url=GATEWAY_URL,
            secret=secret,
        )

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
        PythonOperator(
            task_id="backfill_dia",
            python_callable=_task,
        )

    return dag


# ---------------------------------------------------------------------------
# Municípios
# ---------------------------------------------------------------------------

_RDS_HOST = "proxy-db-viverdb.proxy-cb8m6qcy2cyh.sa-east-1.rds.amazonaws.com"

dag_homolog = make_backfill_dag(
    dag_id="bi_etapas_backfill_homolog",
    municipio="homolog",
    db_host=_RDS_HOST,
    db_name="saude_devel_pi",
    app_host="homolog.saude.pi.gov.br",
)

dag_piripiri = make_backfill_dag(
    dag_id="bi_etapas_backfill_piripiri",
    municipio="piripiri",
    db_host=_RDS_HOST,
    db_name="cuidar_piripiri_pi",
    app_host="cuidarpi.piripiri.saude.pi.gov.br",
)

dag_bomjesus = make_backfill_dag(
    dag_id="bi_etapas_backfill_bomjesus",
    municipio="bomjesus",
    db_host=_RDS_HOST,
    db_name="cuidar_bomjesus_pi",
    app_host="cuidarpi.bomjesus.saude.pi.gov.br",
)

dag_campomaior = make_backfill_dag(
    dag_id="bi_etapas_backfill_campomaior",
    municipio="campomaior",
    db_host=_RDS_HOST,
    db_name="cuidar_campomaior_pi",
    app_host="cuidarpi.campomaior.saude.pi.gov.br",
)

dag_caps = make_backfill_dag(
    dag_id="bi_etapas_backfill_caps",
    municipio="caps",
    db_host=_RDS_HOST,
    db_name="cuidar_caps_pi",
    app_host="cuidarpi.caps.saude.pi.gov.br",
)

dag_cetea = make_backfill_dag(
    dag_id="bi_etapas_backfill_cetea",
    municipio="cetea",
    db_host=_RDS_HOST,
    db_name="cetea_pi",
    app_host="cetea.saude.pi.gov.br",
)

dag_corrente = make_backfill_dag(
    dag_id="bi_etapas_backfill_corrente",
    municipio="corrente",
    db_host=_RDS_HOST,
    db_name="cuidar_corrente_pi",
    app_host="cuidarpi.corrente.saude.pi.gov.br",
)

dag_floriano = make_backfill_dag(
    dag_id="bi_etapas_backfill_floriano",
    municipio="floriano",
    db_host=_RDS_HOST,
    db_name="cuidar_floriano_pi",
    app_host="cuidarpi.floriano.saude.pi.gov.br",
)

dag_parnaiba = make_backfill_dag(
    dag_id="bi_etapas_backfill_parnaiba",
    municipio="parnaiba",
    db_host=_RDS_HOST,
    db_name="cuidar_parnaiba_pi",
    app_host="cuidarpi.parnaiba.saude.pi.gov.br",
)

dag_picos = make_backfill_dag(
    dag_id="bi_etapas_backfill_picos",
    municipio="picos",
    db_host=_RDS_HOST,
    db_name="cuidar_picos_pi",
    app_host="cuidarpi.picos.saude.pi.gov.br",
)

dag_saojoao = make_backfill_dag(
    dag_id="bi_etapas_backfill_saojoao",
    municipio="saojoao",
    db_host=_RDS_HOST,
    db_name="cuidar_saojoao_pi",
    app_host="cuidarpi.saojoao.saude.pi.gov.br",
)

dag_saoraimundononato = make_backfill_dag(
    dag_id="bi_etapas_backfill_saoraimundononato",
    municipio="saoraimundononato",
    db_host=_RDS_HOST,
    db_name="cuidar_saoraimundononato_pi",
    app_host="cuidarpi.saoraimundononato.saude.pi.gov.br",
)
