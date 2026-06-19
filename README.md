# ♟️ Chess ETL Pipeline

Pipeline ETL para extração de partidas de xadrez das APIs **Chess.com** e **Lichess.org**, com persistência na camada **Bronze (Parquet)**, orquestração via **Apache Airflow** e validação via **dbt + DuckDB**.

---

## Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 2.9 (Docker) |
| Extração HTTP | Python + `httpx` (HTTP/2 + gzip) |
| Bronze Storage | Parquet (PyArrow + snappy) |
| Transformação | dbt-core + dbt-duckdb |
| Backend Airflow | PostgreSQL 15 |
| Containerização | Docker Compose |

---

## Estrutura

```
chess_etl/
├── dags/                        # DAGs do Airflow
│   ├── chess_com_dag.py         # ETL mensal Chess.com
│   └── lichess_dag.py           # ETL diário Lichess
├── src/
│   ├── extractors/
│   │   ├── chess_com_extractor.py
│   │   └── lichess_extractor.py
│   ├── loaders/
│   │   └── parquet_loader.py
│   └── utils/
│       ├── http_client.py       # HTTP/2, ETag, retry
│       └── rate_limiter.py      # Serial rate limiter
├── dbt_chess/                   # Projeto dbt
│   ├── models/bronze/
│   │   ├── stg_chess_com_games.sql
│   │   └── stg_lichess_games.sql
│   └── dbt_project.yml
├── tests/                       # Testes unitários
├── data/bronze/                 # Bronze Layer (gerado)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## Setup

### 1. Clonar e configurar variáveis de ambiente

```bash
cp .env.example .env
```

Edite `.env` com seu token Lichess (opcional para início):
```
LICHESS_API_TOKEN=seu_token_aqui    # Deixe vazio para usar token mock
LICHESS_USERNAME=Demetrius01
CHESS_COM_USERNAME=demetriusricon
```

### 2. Subir o ambiente

```bash
docker compose up --build -d
```

> O container `airflow-init` cria automaticamente as **Airflow Variables** com os valores do `.env`.

### 3. Acessar o Airflow UI

**URL:** http://localhost:8080  
**Login:** `admin` / `admin`

---

## Token Lichess — 3 camadas de resolução

O sistema resolve o token automaticamente em ordem de prioridade. Eu fiz esse processo pois da pra baixar informações de usuario sem a necessidade usar um toker de API. O Lichess é muito mais completo que o Chess.com .

```
1. Airflow Variable 'lichess_api_token'   ← PRODUÇÃO (maior prioridade)
2. Variável de ambiente LICHESS_API_TOKEN ← DESENVOLVIMENTO LOCAL
3. MOCK_LICHESS_TOKEN_REPLACE_ME          ← TESTES (sem credenciais)
```

### Para configurar o token real via Airflow UI:

1. Acesse **Admin > Variables** em http://localhost:8080/variable/list/
2. Edite a variável `lichess_api_token`
3. Cole seu token da Lichess (obtido em https://lichess.org/account/oauth/token)
4. Clique em **Save**

> A DAG detectará o token real automaticamente na próxima execução — sem necessidade de reiniciar containers.

---

## DAGs

### `chess_com_etl`
- **Schedule:** `0 3 1 * *` (1º de cada mês às 03:00)
- **Fluxo:** `get_player_profile → get_archives → extract_and_save_parquet → run_dbt_bronze`
- **Output:** `data/bronze/chess_com/demetriusricon/YYYY/MM/games.parquet`

### `lichess_etl`
- **Schedule:** `0 4 * * *` (diariamente às 04:00)
- **Fluxo:** `validate_token → extract_stream_parquet → run_dbt_bronze`
- **Output:** `data/bronze/lichess/demetrius01/YYYY/MM/games.parquet`

---

## Airflow Variables

| Variable | Valor Padrão | Descrição |
|---|---|---|
| `lichess_api_token` | `MOCK_LICHESS_TOKEN_REPLACE_ME` | Token da API Lichess |
| `lichess_username` | `Demetrius01` | Username Lichess |
| `chess_com_username` | `demetriusricon` | Username Chess.com |
| `bronze_base_path` | `/opt/airflow/data/bronze` | Caminho Bronze Layer |
| `historical_months` | `3` | Meses históricos (Chess.com) |

---

## Rodar Testes

```bash
# Instalar dependências localmente
pip install -r requirements.txt

# Rodar testes unitários
pytest tests/ -v

# Rodar testes dbt (requer dbt instalado)
cd dbt_chess && dbt test
```

---

## Bronze Layer — Schemas Parquet

### `chess_com_games`
| Campo | Tipo |
|---|---|
| `game_id` | string |
| `url` | string |
| `pgn` | string |
| `time_control` | string |
| `end_time` | timestamp[UTC] |
| `is_rated` | boolean |
| `time_class` | string |
| `rules` | string |
| `white_username` | string |
| `white_rating` | int64 |
| `white_result` | string |
| `black_username` | string |
| `black_rating` | int64 |
| `black_result` | string |
| `ingestion_ts` | timestamp[UTC] |
| `source_month` | string |

### `lichess_games`
| Campo | Tipo |
|---|---|
| `game_id` | string |
| `rated` | boolean |
| `variant` | string |
| `speed` | string |
| `perf` | string |
| `created_at` | timestamp[UTC] |
| `last_move_at` | timestamp[UTC] |
| `status` | string |
| `white_id` | string |
| `white_rating` | int64 |
| `white_result` | string |
| `black_id` | string |
| `black_rating` | int64 |
| `black_result` | string |
| `moves` | string |
| `clock_initial` | int64 |
| `clock_increment` | int64 |
| `ingestion_ts` | timestamp[UTC] |