import pytest

from simex.ingest.config import Settings


def test_defaults():
    settings = Settings.from_env({})
    assert settings.pairs == ("BTC-USD", "ETH-USD", "SOL-USD")
    assert settings.kafka_bootstrap_servers == "127.0.0.1:9092"
    assert settings.metrics_port == 9101
    assert settings.topic_partitions == 3
    assert settings.venue == "gemini" and settings.coinbase_research is False
    assert settings.stale_after_seconds == 5.0 and settings.ping_interval_seconds == 2.0
    assert settings.ws_url == ""  # the adapter's default URL
    settings.check()


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


def test_coinbase_needs_the_research_flag():
    settings = Settings.from_env({"SIMEX_VENUE": "coinbase", "COINBASE_WS_URL": "wss://x"})
    assert settings.venue == "coinbase" and settings.ws_url == "wss://x"
    with pytest.raises(ValueError, match="research only"):
        settings.check()
    Settings.from_env({"SIMEX_VENUE": "Coinbase", "SIMEX_COINBASE_RESEARCH": "1"}).check()


def test_venue_urls_and_bad_venue():
    gemini = Settings.from_env({"GEMINI_WS_URL": "wss://g", "COINBASE_WS_URL": "wss://c"})
    assert gemini.ws_url == "wss://g"
    with pytest.raises(ValueError, match="SIMEX_VENUE"):
        Settings.from_env({"SIMEX_VENUE": "kraken"}).check()
