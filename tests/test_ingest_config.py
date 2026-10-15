from simex.ingest.config import Settings


def test_defaults():
    settings = Settings.from_env({})
    assert settings.pairs == ("BTC-USD", "ETH-USD", "SOL-USD")
    assert settings.kafka_bootstrap_servers == "127.0.0.1:9092"
    assert settings.metrics_port == 9101
    assert settings.topic_partitions == 3


def test_overrides_and_blank_values():
    settings = Settings.from_env(
        {
            "SIMEX_PAIRS": " btc-usd , sol-usd,",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "INGEST_METRICS_PORT": "9200",
            "INGEST_STALE_AFTER_SECONDS": "5",
            "INGEST_BACKOFF_MAX_SECONDS": "",
        }
    )
    assert settings.pairs == ("BTC-USD", "SOL-USD")
    assert settings.kafka_bootstrap_servers == "kafka:29092"
    assert settings.metrics_port == 9200
    assert settings.stale_after_seconds == 5.0
    assert settings.backoff_max_seconds == 60.0
