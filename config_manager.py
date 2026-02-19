"""
Configuration Manager for Retail Data Warehouse
Loads and validates configuration from YAML files with Pydantic
Supports multiple environments (dev, staging, prod)
Author: [Your Name]
Date: February 2026
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class Environment(str, Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class DatabaseConfig(BaseModel):
    """Database connection configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "retail_dw"
    username: str = "postgres"
    password: str = Field(default="", description="Use environment variable")
    pool_size: int = 5
    max_overflow: int = 10
    echo: bool = False


class IncrementalConfig(BaseModel):
    """Incremental loading configuration"""
    enabled: bool = True
    timestamp_column: str = "InvoiceDate"
    watermark_table: str = "etl_watermarks"


class QualityChecksConfig(BaseModel):
    """Data quality checks configuration"""
    enabled: bool = True
    fail_on_error: bool = False
    anomaly_z_threshold: float = 3.0
    duplicate_check: bool = True
    referential_integrity: bool = True
    schema_validation: bool = True


class RejectionConfig(BaseModel):
    """Rejection handling configuration"""
    log_rejected_records: bool = True
    rejection_table: str = "etl_rejected_records"
    max_rejection_percentage: float = 10.0


class ETLConfig(BaseModel):
    """ETL process configuration"""
    incremental: IncrementalConfig = IncrementalConfig()
    batch_size: int = 5000
    chunk_size: int = 1000
    max_retries: int = 3
    retry_delay_seconds: int = 300
    exponential_backoff: bool = True
    quality_checks: QualityChecksConfig = QualityChecksConfig()
    rejection: RejectionConfig = RejectionConfig()


class EmailConfig(BaseModel):
    """Email notification configuration"""
    enabled: bool = False
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str = ""
    sender_password: str = Field(default="", description="Use environment variable")
    recipient_emails: List[str] = []
    send_on_success: bool = False
    send_on_failure: bool = True
    send_on_warning: bool = True


class SchedulerConfig(BaseModel):
    """Scheduler configuration"""
    enabled: bool = True
    cron_expression: str = "0 2 * * *"
    timezone: str = "Asia/Kolkata"
    email: EmailConfig = EmailConfig()


class DataSourceConfig(BaseModel):
    """Data source configuration"""
    type: str = "kagglehub"
    dataset: Optional[str] = None
    path: Optional[str] = None
    cache_enabled: bool = True
    encoding: str = "utf-8"


class LoggingFileConfig(BaseModel):
    """File logging configuration"""
    enabled: bool = True
    directory: str = "logs"
    filename_pattern: str = "{process}_{date}.log"
    max_bytes: int = 10485760  # 10 MB
    backup_count: int = 5


class LoggingConsoleConfig(BaseModel):
    """Console logging configuration"""
    enabled: bool = True
    colored: bool = True


class LoggingConfig(BaseModel):
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: LoggingFileConfig = LoggingFileConfig()
    console: LoggingConsoleConfig = LoggingConsoleConfig()

    @validator('level')
    def validate_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of {valid_levels}")
        return v.upper()


class AlertsConfig(BaseModel):
    """Monitoring alerts configuration"""
    slow_query_threshold_seconds: int = 30
    large_batch_threshold: int = 100000
    low_quality_threshold: float = 0.8


class MonitoringConfig(BaseModel):
    """Performance monitoring configuration"""
    enabled: bool = True
    track_execution_time: bool = True
    track_record_counts: bool = True
    track_data_quality_metrics: bool = True
    alerts: AlertsConfig = AlertsConfig()


class QualityReportsConfig(BaseModel):
    """Quality reports configuration"""
    enabled: bool = True
    generate_html: bool = True
    output_directory: str = "reports"
    save_to_database: bool = True
    retention_days: int = 90


class PerformanceReportsConfig(BaseModel):
    """Performance reports configuration"""
    enabled: bool = True
    generate_daily_summary: bool = True
    generate_weekly_summary: bool = True


class ReportingConfig(BaseModel):
    """Reporting configuration"""
    quality_reports: QualityReportsConfig = QualityReportsConfig()
    performance_reports: PerformanceReportsConfig = PerformanceReportsConfig()


class FeaturesConfig(BaseModel):
    """Feature flags"""
    incremental_loading: bool = True
    advanced_quality_checks: bool = True
    email_notifications: bool = False
    html_dashboards: bool = True
    performance_monitoring: bool = True
    auto_schema_validation: bool = True
    duplicate_prevention: bool = True


class PathsConfig(BaseModel):
    """Paths and directories"""
    data_directory: str = "data"
    logs_directory: str = "logs"
    reports_directory: str = "reports"
    temp_directory: str = "temp"
    backup_directory: str = "backups"


class BusinessRulesConfig(BaseModel):
    """Custom business rules"""
    min_transaction_amount: float = 0.01
    max_transaction_amount: float = 100000.00
    max_quantity_per_transaction: int = 10000
    min_transaction_date: str = "2010-01-01"
    max_transaction_date: str = "2025-12-31"
    allowed_countries: List[str] = []
    excluded_categories: List[str] = []


class Config(BaseModel):
    """Main configuration model"""
    environment: Environment = Environment.DEVELOPMENT
    database: DatabaseConfig = DatabaseConfig()
    etl: ETLConfig = ETLConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    data_sources: Dict[str, DataSourceConfig] = {}
    logging: LoggingConfig = LoggingConfig()
    schemas: Dict[str, Dict[str, str]] = {}
    monitoring: MonitoringConfig = MonitoringConfig()
    reporting: ReportingConfig = ReportingConfig()
    features: FeaturesConfig = FeaturesConfig()
    paths: PathsConfig = PathsConfig()
    business_rules: BusinessRulesConfig = BusinessRulesConfig()

    class Config:
        use_enum_values = True


class ConfigManager:
    """
    Configuration Manager
    Loads, validates, and provides access to configuration
    """

    _instance = None
    _config: Optional[Config] = None

    def __new__(cls):
        """Singleton pattern - only one config instance"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def load_config(self, config_file: str = "config.yaml", env: Optional[str] = None):
        """
        Load configuration from YAML file

        Args:
            config_file (str): Path to config file
            env (str): Environment override (reads from ENV var if not provided)
        """
        # Determine environment
        if env is None:
            env = os.getenv('APP_ENV', 'development')

        # Load YAML file
        config_path = Path(config_file)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")

        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        # Override environment if specified
        if env:
            config_data['environment'] = env

        # Load environment-specific overrides if they exist
        env_config_file = config_path.parent / f"config.{env}.yaml"
        if env_config_file.exists():
            with open(env_config_file, 'r') as f:
                env_config_data = yaml.safe_load(f)
                config_data = self._deep_merge(config_data, env_config_data)

        # Load environment variables for sensitive data
        config_data = self._load_env_variables(config_data)

        # Validate and create config object
        self._config = Config(**config_data)

        print(f"✅ Configuration loaded: {self._config.environment}")

        return self._config

    def _deep_merge(self, base: dict, override: dict) -> dict:
        """Deep merge two dictionaries"""
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def _load_env_variables(self, config_data: dict) -> dict:
        """Load sensitive values from environment variables"""

        # Database password
        db_password = os.getenv('DB_PASSWORD')
        if db_password:
            config_data['database']['password'] = db_password

        # Email password
        email_password = os.getenv('EMAIL_PASSWORD')
        if email_password:
            if 'scheduler' in config_data and 'email' in config_data['scheduler']:
                config_data['scheduler']['email']['sender_password'] = email_password

        return config_data

    @property
    def config(self) -> Config:
        """Get current configuration"""
        if self._config is None:
            self.load_config()
        return self._config

    def get_database_url(self) -> str:
        """Get SQLAlchemy database URL"""
        db = self.config.database
        return f"postgresql://{db.username}:{db.password}@{db.host}:{db.port}/{db.database}"

    def is_production(self) -> bool:
        """Check if running in production"""
        return self.config.environment == Environment.PRODUCTION

    def is_development(self) -> bool:
        """Check if running in development"""
        return self.config.environment == Environment.DEVELOPMENT

    def reload(self):
        """Reload configuration"""
        self._config = None
        return self.load_config()


# Global config manager instance
config_manager = ConfigManager()


# Convenience function
def get_config() -> Config:
    """Get configuration instance"""
    return config_manager.config


if __name__ == "__main__":
    # Test configuration loading
    print("="*70)
    print("🧪 TESTING CONFIGURATION MANAGER")
    print("="*70)

    config = config_manager.load_config()

    print(f"\nEnvironment: {config.environment}")
    print(f"Database: {config.database.host}:{config.database.port}/{config.database.database}")
    print(f"Incremental Loading: {config.etl.incremental.enabled}")
    print(f"Quality Checks: {config.etl.quality_checks.enabled}")
    print(f"Email Notifications: {config.scheduler.email.enabled}")
    print(f"Log Level: {config.logging.level}")

    print(f"\nDatabase URL: {config_manager.get_database_url()}")
    print(f"Is Production: {config_manager.is_production()}")
    print(f"Is Development: {config_manager.is_development()}")

    print("\n✅ Configuration loaded successfully!")
