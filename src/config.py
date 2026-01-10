"""
Configuration management using Pydantic Settings.

Loads configuration from environment variables and .env files.
Provides type-safe access to all configuration values.
"""

from pathlib import Path
from functools import lru_cache
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    Environment variables can be set directly or via a .env file.
    All settings have sensible defaults for local development.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # ==========================================================================
    # Project Paths
    # ==========================================================================
    
    project_root: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent,
        description="Root directory of the project",
    )
    
    data_dir: Path = Field(
        default=Path("data"),
        description="Base directory for all data files",
    )
    
    @property
    def raw_data_dir(self) -> Path:
        """Directory for raw downloaded data."""
        return self.data_dir / "raw"
    
    @property
    def processed_data_dir(self) -> Path:
        """Directory for processed/transformed data."""
        return self.data_dir / "processed"
    
    @property
    def output_dir(self) -> Path:
        """Directory for output files (reports, exports)."""
        return self.data_dir / "output"
    
    # ==========================================================================
    # OpenSanctions Configuration
    # ==========================================================================
    
    opensanctions_base_url: str = Field(
        default="https://data.opensanctions.org/datasets/latest",
        description="Base URL for OpenSanctions data downloads",
    )
    
    opensanctions_dataset: str = Field(
        default="sanctions",
        description="Which dataset to download: 'default', 'sanctions', or 'peps'",
    )
    
    # ==========================================================================
    # OpenCorporates Configuration
    # ==========================================================================
    
    opencorporates_api_key: str | None = Field(
        default=None,
        description="API key for OpenCorporates (optional, increases rate limits)",
    )
    
    opencorporates_base_url: str = Field(
        default="https://api.opencorporates.com/v0.4",
        description="Base URL for OpenCorporates API",
    )
    
    # ==========================================================================
    # UK Companies House Configuration
    # ==========================================================================
    
    uk_companies_house_api_key: str | None = Field(
        default=None,
        description="API key for UK Companies House (required for that data source)",
    )
    
    uk_companies_house_base_url: str = Field(
        default="https://api.company-information.service.gov.uk",
        description="Base URL for UK Companies House API",
    )
    
    # ==========================================================================
    # Entity Resolution Configuration
    # ==========================================================================
    
    name_match_threshold: int = Field(
        default=85,
        ge=0,
        le=100,
        description="Minimum fuzzy match score (0-100) for name matching",
    )
    
    address_match_threshold: int = Field(
        default=80,
        ge=0,
        le=100,
        description="Minimum fuzzy match score (0-100) for address matching",
    )
    
    # ==========================================================================
    # Database Configuration
    # ==========================================================================
    
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_user: str = Field(default="postgres")
    postgres_password: str = Field(default="postgres")
    postgres_database: str = Field(default="sanctions_network")
    
    @property
    def postgres_url(self) -> str:
        """PostgreSQL connection URL."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        )
    
    neo4j_uri: str = Field(default="bolt://localhost:7687")
    neo4j_user: str = Field(default="neo4j")
    neo4j_password: str = Field(default="password")
    
    # ==========================================================================
    # Network Analysis Configuration
    # ==========================================================================
    
    max_path_depth: int = Field(
        default=4,
        ge=1,
        le=10,
        description="Maximum depth for path finding between entities",
    )
    
    high_risk_jurisdictions: list[str] = Field(
        default=[
            "vg",  # British Virgin Islands
            "ky",  # Cayman Islands
            "sc",  # Seychelles
            "pa",  # Panama
            "bz",  # Belize
            "ws",  # Samoa
            "mh",  # Marshall Islands
        ],
        description="ISO 3166-1 alpha-2 codes for high-risk jurisdictions",
    )
    
    # ==========================================================================
    # HTTP Client Configuration
    # ==========================================================================
    
    http_timeout: float = Field(
        default=120.0,
        description="HTTP request timeout in seconds",
    )
    
    http_max_retries: int = Field(
        default=3,
        description="Maximum number of retry attempts for failed requests",
    )
    
    rate_limit_delay: float = Field(
        default=1.0,
        description="Delay between API requests (seconds) for rate limiting",
    )
    
    # ==========================================================================
    # Logging Configuration
    # ==========================================================================
    
    log_level: str = Field(
        default="INFO",
        description="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    
    log_format: str = Field(
        default="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        description="Log message format string",
    )
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Ensure log level is valid."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper_v = v.upper()
        if upper_v not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return upper_v
    
    def ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        for directory in [
            self.raw_data_dir / "opensanctions",
            self.raw_data_dir / "corporate",
            self.processed_data_dir,
            self.output_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings.
    
    Uses lru_cache to ensure settings are only loaded once.
    Call get_settings.cache_clear() to reload if needed.
    """
    return Settings()


# Convenience export
settings = get_settings()
