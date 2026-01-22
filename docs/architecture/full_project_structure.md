## Full Project Structure for Spotify Data Pipeline

```
SPOTIFY-DATA-PIPELINE/
├── __pycache__/
├── .devcontainer/
├── api/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── cache.py
│   ├── database.py
│   ├── main.py
│   └── models.py
├── dags/
│   ├── data_quality_monitoring.py         # Monitoring DAG
│   └── spotify_pipeline_basic.py          # Main production DAG
├── dbt/
│   ├── analyses/
│   ├── dbt_packages/
│   ├── dbt_utils/
│   ├── logs/
│   ├── macros/
│   │   ├── .gitkeep
│   │   └── get_custom_schema.sql
│   ├── models/
│   │   ├── bronze/
│   │   │   └── sources.yml
│   │   ├── gold/
│   │   │   ├── artist_affinity.sql
│   │   │   ├── daily_user_stats.sql
│   │   │   ├── device_usage.sql
│   │   │   ├── schema.yml
│   │   │   ├── top_artists.sql
│   │   │   ├── top_tracks.sql
│   │   │   ├── track_cooccurrence.sql
│   │   │   └── track_recommendations.sql
│   │   └── silver/
│   │       ├── silver_plays.sql
│   │       └── silver_plays.yml
│   ├── seeds/
│   ├── snapshots/
│   ├── target/
│   ├── tests/
│   ├── .user.yml
│   ├── dbt_project.yml
│   ├── package-lock.yml
│   ├── packages.yml
│   └── profiles.yml
├── docs/
│   ├── airflow/
│   │   ├── airflow_architecture.md
│   │   ├── airflow_setup.md
│   │   └── troubleshooting.md
│   ├── api/
│   │   └── README.md
│   ├── architecture/
│   │   └── system_architecture.png
│   ├── dbt/
│   │   ├── erds/
│   │   │   └── gold_layer_erd.png
│   │   ├── lineage/
│   │   │   └── spotify_dbt_lineage_graph.png
│   │   └── screenshots/
│   │       ├── dag_graph_monitoring.png
│   │       ├── dag_graph_production.png
│   │       ├── email_alert_example.png
│   │       └── successful_dag_run.png
│   └── setup.md
├── logs/
├── plugins/
├── scripts/
│   └── dashboard.py
├── sql/
│   ├── ddl/
│   │   └── spotify_DDL.sql
│   └── validation/
│       ├── api_validation_queries.sql
│       ├── data_validation_silver.sql
│       ├── poller_validation_queries.sql
│       └── recommendation_validation_queries.sql
├── src/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── event_simulator.py                 # Synthetic event generator
│   ├── kafka_consumer_background.py
│   ├── kafka_consumer.py                  # Kafka to S3 consumer
│   ├── kafka_producer.py
│   ├── simulator_last_date.txt            # Date tracker for sequential data
│   ├── spotify_client.py
│   ├── spotify_historical_backfill.py
│   └── spotify_poller.py
├── streamlit_dashboard/
│   ├── __pycache__/
│   ├── .streamlit/
│   │   └── secrets.toml                   # Snowflake credentials (not committed)
│   ├── app.py                             # Main dashboard application
│   ├── requirements.txt                   # Dashboard dependencies
│   └── utils.py                           # Snowflake connection helpers
├── systemd/
│   ├── recommendation-api.service
│   └── spotify-poller.service
├── tests/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── test_kafka.py
│   ├── test_parquet_validation.py
│   ├── test_s3.py
│   ├── test_snowflake_external_table.py
│   └── test_snowflake.py
├── venv/
├── .cache
├── .env                                   # Environment variables (not committed)
├── .gitignore                             # Git ignore rules
├── config.py                              # Centralized configuration
├── docker-compose.yml                     # Airflow containerization
├── Dockerfile                             # Custom Airflow image
├── prd.md
├── project.md
├── README.md                              # This file
└── requirements.txt                       # Python dependencies
```