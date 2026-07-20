import logging
import os
from pathlib import Path

from schemas.app_settings import AppSettings

logger = logging.getLogger(__name__)


def _load_settings() -> AppSettings:
    env = os.getenv("APP_ENV", "development")
    env_file = Path(__file__).resolve().parent.parent / f".env.{env}"
    return AppSettings(_env_file=env_file)


settings: AppSettings = _load_settings()
