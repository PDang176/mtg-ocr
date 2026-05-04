import os

from dotenv import load_dotenv, find_dotenv
from dataclasses import dataclass, field

load_dotenv(find_dotenv())

@dataclass
class Config:
    # ── Database Configuration ──────────────────────────────────────────────────────────
    db_host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    db_port: str = field(default_factory=lambda: os.getenv("DB_PORT", "5432"))
    db_name: str = field(default_factory=lambda: os.getenv("DB_NAME", "postgres"))
    db_user: str = field(default_factory=lambda: os.getenv("DB_USER", "postgres"))
    db_password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", "postgres"))
    # ────────────────────────────────────────────────────────────────────────────────────

    @property
    def db_config(self):
        return {
            "host": self.db_host,
            "port": self.db_port,
            "database": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
        }