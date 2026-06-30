"""Lichess ETL DAG – extracts games to Bronze layer."""

from __future__ import annotations
import logging
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param

sys.path.insert(0, "/opt/airflow/src")
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "Demetrius Ricon",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="lichess_etl",
    description="ETL - Lichess",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["chess", "etl", "bronze", "lichess"],
    params={
        "start_date": Param(None, type=["null", "string"], format="date", description="Start Date (YYYY-MM-DD) for manual runs. Default is last week."),
        "end_date": Param(None, type=["null", "string"], format="date", description="End Date (YYYY-MM-DD) for manual runs. Default is last week."),
    },
)
def lichess_etl_dag():

    # Recebe context para acessar e configurar variáveis e retorna dict com dados de validação (token e username) para as tasks seguintes.
    @task(task_id="validate_token")
    def validate_token(**context) -> dict:
        """Validate that the Lichess API token is set in Airflow Variables."""
        token = Variable.get("lichess_api_token", default_var=None)
        if not token:
            raise ValueError("Lichess API token not set in Airflow Variable 'lichess_api_token'.")
        logger.info("Token Lichess presente – modo produção ativo.")
        
        username = Variable.get("lichess_username", default_var="Demetrius01")
        # Push username for downstream tasks
        context["ti"].xcom_push(
            key="lichess_username",
            value=username,
        )
        return {"token": token, "username": username}

    # Recebe o dict de validação (para garantir dependência e dados do user) e o context, retornando a contagem de jogos extraídos.
    @task(task_id="extract_stream_parquet")
    def extract_stream(token_info: dict, **context) -> int:
        """Extract Lichess games for a given period and save as Parquet.

        The period can be provided via DAG run configuration parameters ``start_date`` and ``end_date``
        (ISO 8601 strings). If not provided (scheduled runs), the default period is the previous week
        relative to the DAG's logical execution date.
        """
        from extractors.lichess_extractor import LichessExtractor
        from loaders.parquet_loader import save_lichess_games
        from utils.validation import resolve_period

        username = (
            token_info.get("username")
            or context["ti"].xcom_pull(key="lichess_username")
            or Variable.get("lichess_username", default_var="Demetrius01")
        )
        bronze_path = Variable.get("bronze_base_path", default_var="/opt/airflow/data/bronze")

        # Resolve period
        start_dt, end_dt = resolve_period(context.get("params", {}), context["logical_date"])

        if start_dt > end_dt:
            raise ValueError("start_date must be before or equal to end_date")

        since_ms = int(start_dt.timestamp() * 1000)
        until_ms = int(end_dt.timestamp() * 1000)
        source_month = start_dt.strftime("%Y-%m")

        logger.info(
            "Extracting Lichess games for user '%s' | Period: %s → %s",
            username,
            start_dt.to_iso8601_string(),
            end_dt.to_iso8601_string(),
        )
        with LichessExtractor(username=username) as extractor:
            df = extractor.extract_games(since_ms=since_ms, until_ms=until_ms)
        if df.empty:
            logger.info("No games found in the specified period.")
            return 0
        out_path = save_lichess_games(
            df=df,
            source_month=source_month,
            base_path=bronze_path,
            username=username,
        )
        count = len(df)
        logger.info("%d Lichess games saved to %s", count, out_path)
        context["ti"].xcom_push(key="games_count", value=count)
        context["ti"].xcom_push(key="source_month", value=source_month)
        return count

    # Recebe a contagem de novos jogos extraídos (para pular se for zero) e o context, e não retorna nada (None) ao rodar o dbt.
    @task(task_id="run_dbt_bronze")
    def run_dbt(games_count: int, **context) -> None:
        """Execute dbt run for Lichess bronze models."""
        import subprocess
        import json
        from utils.validation import resolve_period

        if games_count == 0:
            logger.info("Sem partidas novas — pulando dbt run.")
            return

        # Pass the period as dbt vars
        start_dt, end_dt = resolve_period(context.get("params", {}), context["logical_date"])
        dbt_vars = {
            "start_date": start_dt.format("YYYY-MM-DD"),
            "end_date": end_dt.format("YYYY-MM-DD")
        }

        dbt_project_dir = "/opt/airflow/dbt_chess"
        logger.info("Executando dbt run (modelos bronze lichess)...")
        result = subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "bronze.stg_lichess_games",
                "--project-dir",
                dbt_project_dir,
                "--profiles-dir",
                dbt_project_dir,
                "--vars",
                json.dumps(dbt_vars)
            ],
            capture_output=True,
            text=True,
        )
        logger.info("dbt stdout:\n%s", result.stdout)
        if result.returncode != 0:
            logger.error("dbt stderr:\n%s", result.stderr)
            raise RuntimeError(f"dbt run falhou (exit code {result.returncode})")
        logger.info("dbt run Lichess concluído.")

    # Cadeia de dependências usando TaskFlow API
    token_info = validate_token()
    games_count = extract_stream(token_info)
    run_dbt(games_count)


lichess_etl_dag()
