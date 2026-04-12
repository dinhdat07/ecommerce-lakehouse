import importlib

from common import constants as constants_module
from common import config as config_module


def test_load_config_reads_server_profile_fields(monkeypatch):
    overrides = {
        "DEPLOYMENT_PROFILE": "server",
        "STREAM_MODE": "server",
        "KAFKA_TOPIC_PARTITIONS": "12",
        "KAFKA_TOPIC_REPLICATION_FACTOR": "3",
        "KAFKA_TOPIC_MIN_INSYNC_REPLICAS": "2",
        "STREAM_TIMEOUT_SECONDS": "0",
        "STREAM_CHECKPOINT_LOCATION": "/srv/checkpoints/streaming/job",
        "STREAM_PROGRESS_LOG_PATH": "/srv/logs/streaming/progress.jsonl",
        "REPLAY_REPEAT_INPUT": "true",
        "REPLAY_RUNTIME_SECONDS": "600",
        "S3_ENDPOINT": "https://minio.example.com",
        "S3_PATH_STYLE_ACCESS": "false",
        "S3_BUCKET_WAREHOUSE": "warehouse-prod",
        "ICEBERG_CATALOG_URI": "jdbc:postgresql://metadata:5432/iceberg",
        "ICEBERG_WAREHOUSE": "s3://warehouse-prod",
        "BENCHMARK_OUTPUT_ROOT": "/srv/benchmarks",
    }
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)

    importlib.reload(constants_module)
    reloaded_config = importlib.reload(config_module)
    cfg = reloaded_config.load_config()

    try:
        assert cfg.deployment_profile == "server"
        assert cfg.stream_mode == "server"
        assert cfg.kafka_topic_partitions == 12
        assert cfg.kafka_topic_replication_factor == 3
        assert cfg.kafka_topic_min_insync_replicas == 2
        assert cfg.stream_timeout_seconds == 0
        assert cfg.stream_checkpoint_location == "/srv/checkpoints/streaming/job"
        assert cfg.stream_progress_log_path == "/srv/logs/streaming/progress.jsonl"
        assert cfg.replay_repeat_input is True
        assert cfg.replay_runtime_seconds == 600
        assert cfg.s3_endpoint == "https://minio.example.com"
        assert cfg.s3_path_style_access is False
        assert cfg.s3_bucket_warehouse == "warehouse-prod"
        assert cfg.iceberg_catalog_uri == "jdbc:postgresql://metadata:5432/iceberg"
        assert cfg.iceberg_warehouse == "s3://warehouse-prod"
        assert str(cfg.benchmark_output_root) == "/srv/benchmarks"
    finally:
        monkeypatch.undo()
        importlib.reload(constants_module)
        importlib.reload(config_module)
