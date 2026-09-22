"""Configuration centralisée, chargée depuis l'environnement / .env."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    bsd_api_key: str
    bsd_base_url: str = "https://sports.bzzoiro.com/api/v2"

    database_url: str = "sqlite:///./bsd_ai.db"

    bsd_league_ids: str = ""  # CSV de league_id, ex: "85,203"
    max_concurrent_live: int = 50

    @property
    def league_ids(self) -> list[int]:
        return [int(x) for x in self.bsd_league_ids.split(",") if x.strip()]


settings = Settings()  # type: ignore[call-arg]  # lit BSD_API_KEY etc. depuis l'env
