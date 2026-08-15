"""Environment-specific configuration for the rental_analytics dbt pipeline.

Which environment is active is controlled by the RENTAL_ANALYTICS_ENV Airflow
Variable (falls back to "dev" if unset), so the same DAG code runs unchanged
against dev or prod -- only the resolved BigQuery dataset / dbt target /
notification behavior differ.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EnvConfig:
    env: str
    bq_dataset: str
    dbt_target: str
    notify_enabled: bool


_ENV_CONFIGS: dict[str, EnvConfig] = {
    "dev": EnvConfig(
        env="dev",
        bq_dataset="dev_rentals_analytics",
        dbt_target="dev",
        notify_enabled=False,
    ),
    "prod": EnvConfig(
        env="prod",
        bq_dataset="rentals_analytics",
        dbt_target="prod",
        notify_enabled=True,
    ),
}


def get_env_config() -> EnvConfig:
    """Resolve the active EnvConfig from the RENTAL_ANALYTICS_ENV Airflow Variable."""
    from airflow.models import Variable

    env = Variable.get("RENTAL_ANALYTICS_ENV", default_var="dev").strip().lower()
    return _ENV_CONFIGS.get(env, _ENV_CONFIGS["dev"])
