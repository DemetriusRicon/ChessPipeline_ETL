"""Chess.com ETL DAG – extracts monthly games to Bronze layer."""

from __future__ import annotations
import json
import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

# PYTHONPATH inclui /opt/airflow/src (configurado no Dockerfile)
sys.path.insert(0, "/opt/airflow/src")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "antigravity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_get_profile(**context) -> dict:
    """Extrai perfil do jogador e pusha para XCom."""
    from extractors.chess_com_extractor import ChessComExtractor

    username = Variable.get("chess_com_username", default_var="demetriusricon")
    logger.info("Buscando perfil de '%s' na Chess.com...", username)

    with ChessComExtractor(username=username) as extractor:
        profile = extractor.get_player_profile()

    logger.info("Perfil recebido: %s", json.dumps(profile, default=str)[:200])
    context["ti"].xcom_push(key="chess_com_username", value=username)
    return profile


def task_get_archives(**context) -> list[str]:
    """Retorna lista de archive URLs filtrada pelo período de execução."""
    from extractors.chess_com_extractor import ChessComExtractor
    from utils.validation import resolve_period

    username = context["ti"].xcom_pull(key="chess_com_username") or Variable.get(
        "chess_com_username", default_var="demetriusricon"
    )

    start_dt, end_dt = resolve_period(context.get("params", {}), context["logical_date"])

    with ChessComExtractor(username=username) as extractor:
        archives = extractor.get_archives()

    if not archives:
        logger.warning("Nenhum arquivo mensal encontrado para '%s'.", username)
        return []

    # Filter archives by the period months
    start_month = start_dt.format("YYYY/MM")
    end_month = end_dt.format("YYYY/MM")

    filtered_archives = []
    for archive_url in archives:
        # e.g., "https://api.chess.com/pub/player/hikaru/games/2025/01"
        parts = archive_url.rstrip("/").split("/")
        if len(parts) >= 2:
            arch_month = f"{parts[-2]}/{parts[-1]}"
            if start_month <= arch_month <= end_month:
                filtered_archives.append(archive_url)

    logger.info("Período: %s a %s. %d archives selecionados para extração.", 
                start_month, end_month, len(filtered_archives))
    context["ti"].xcom_push(key="archives", value=filtered_archives)
    return filtered_archives


def task_extract_and_save(**context) -> list[str]:
    """Extrai partidas de cada archive e salva em Parquet na Bronze Layer."""
    from extractors.chess_com_extractor import ChessComExtractor
    from loaders.parquet_loader import save_chess_com_games

    username = context["ti"].xcom_pull(key="chess_com_username") or "demetriusricon"
    archives = context["ti"].xcom_pull(key="archives") or []

    if not archives:
        logger.warning("Nenhum archive para processar.")
        return []

    bronze_path = Variable.get("bronze_base_path", default_var="/opt/airflow/data/bronze")
    saved_paths = []

    with ChessComExtractor(username=username) as extractor:
        for archive_url in archives:
            df = extractor.extract_monthly_games(archive_url)
            if df.empty:
                continue

            parts = archive_url.rstrip("/").split("/")
            source_month = f"{parts[-2]}-{parts[-1]}"

            out_path = save_chess_com_games(
                df=df,
                source_month=source_month,
                base_path=bronze_path,
                username=username,
            )
            if out_path:
                saved_paths.append(str(out_path))

    logger.info("Chess.com: %d arquivos Parquet salvos.", len(saved_paths))
    context["ti"].xcom_push(key="saved_parquet_paths", value=saved_paths)
    return saved_paths


def task_run_dbt(**context) -> None:
    """Executa dbt run para os modelos Bronze do Chess.com."""
    import subprocess
    import json
    from utils.validation import resolve_period

    dbt_project_dir = "/opt/airflow/dbt_chess"
    logger.info("Executando dbt run (modelos bronze chess_com)...")

    # Pass the period as dbt vars
    start_dt, end_dt = resolve_period(context.get("params", {}), context["logical_date"])
    dbt_vars = {
        "start_date": start_dt.format("YYYY-MM-DD"),
        "end_date": end_dt.format("YYYY-MM-DD")
    }

    result = subprocess.run(
        [
            "dbt", "run", "--select", "bronze.stg_chess_com_games", 
            "--project-dir", dbt_project_dir,
            "--vars", json.dumps(dbt_vars)
        ],
        capture_output=True,
        text=True,
    )
    logger.info("dbt stdout:\n%s", result.stdout)
    if result.returncode != 0:
        logger.error("dbt stderr:\n%s", result.stderr)
        raise RuntimeError(f"dbt run falhou (exit code {result.returncode})")
    logger.info("✅ dbt run Chess.com concluído.")


with DAG(
    dag_id="chess_com_etl",
    description="ETL mensal: Chess.com → Bronze (Parquet)",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 1 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["chess", "etl", "bronze", "chess_com"],
    params={
        "start_date": Param(None, type=["null", "string"], format="date", description="Start Date (YYYY-MM-DD) for manual runs. Default is last week."),
        "end_date": Param(None, type=["null", "string"], format="date", description="End Date (YYYY-MM-DD) for manual runs. Default is last week."),
    },
) as dag:

    get_profile = PythonOperator(
        task_id="get_player_profile",
        python_callable=task_get_profile,
    )

    get_archives = PythonOperator(
        task_id="get_archives",
        python_callable=task_get_archives,
    )

    extract_and_save = PythonOperator(
        task_id="extract_and_save_parquet",
        python_callable=task_extract_and_save,
    )

    run_dbt = PythonOperator(
        task_id="run_dbt_bronze",
        python_callable=task_run_dbt,
    )

    get_profile >> get_archives >> extract_and_save >> run_dbt
