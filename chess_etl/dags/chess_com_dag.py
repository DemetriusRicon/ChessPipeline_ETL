"""Chess.com ETL DAG – extracts monthly games to Bronze layer."""

from __future__ import annotations
import json
import logging
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param

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


@dag(
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
)
def chess_com_etl_dag():

    # Recebe context para acessar a logical_date/params e retorna a lista de URLs dos archives filtrados para as tasks seguintes.
    @task(task_id="get_archives")
    def get_archives(**context) -> list[str]:
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

        # Filtra pelo mês
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

    # Recebe a lista de URLs dos archives a extrair (de get_archives) e o context,
    # retornando os caminhos dos arquivos Parquet salvos.
    @task(task_id="extract_and_save_parquet")
    def extract_and_save(archives: list[str], **context) -> list[str]:
        """Extrai partidas e salva em Parquet na Bronze Layer."""
        from extractors.chess_com_extractor import ChessComExtractor
        from loaders.parquet_loader import save_chess_com_games

        username = context["ti"].xcom_pull(key="chess_com_username") or "demetriusricon"

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

    # Recebe context para resolver o período (start_date/end_date) no dbt e não retorna nada (None) pois apenas executa o comando CLI.
    @task(task_id="run_dbt_bronze")
    def run_dbt(**context) -> None:
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
        logger.info("dbt run Chess.com concluído.")

    #Ordem de execução
    archives_list = get_archives()
    saved_files = extract_and_save(archives_list)
    dbt_execution = run_dbt()
    
    saved_files >> dbt_execution


chess_com_etl_dag()
