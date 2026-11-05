"""Environment loading for executable services."""

from __future__ import annotations

import logging

from dotenv import load_dotenv


def load_environment(log: logging.Logger) -> None:
    """Load the local .env without overriding explicitly exported variables."""
    if load_dotenv(override=False):
        log.info("loaded environment from .env; explicit environment variables take precedence")
