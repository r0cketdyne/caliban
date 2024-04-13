#!/usr/bin/python
"""
Caliban has built-in (alpha) support for configuring containers for easy
metrics tracking via MLFlow. This module provides functions useful for
configuring a container for this behavior.
"""

import datetime
import json
import os
from contextlib import contextmanager
from typing import Dict, Any, List, Optional

import caliban.util as u

CLOUD_SQL_WRAPPER_SCRIPT = "cloud_sql_proxy.py"
LAUNCHER_SCRIPT = "caliban_launcher.py"
RESOURCE_DIR = "/.resources"
LAUNCHER_CONFIG_FILE = "caliban_launcher_cfg.json"
LAUNCHER_CONFIG_PATH = os.path.join(RESOURCE_DIR, LAUNCHER_CONFIG_FILE)
GPU_ENABLED_TAG = "gpu_enabled"
TPU_ENABLED_TAG = "tpu_enabled"
JOB_NAME_TAG = "job_name"
DOCKER_IMAGE_TAG = "docker_image"
PLATFORM_TAG = "platform"


def cloud_sql_proxy_path() -> Optional[str]:
    """Returns an absolute path to the cloud_sql_proxy python wrapper that we
    inject into containers."""
    return u.resource(CLOUD_SQL_WRAPPER_SCRIPT)


def launcher_path() -> Optional[str]:
    """Returns an absolute path to the caliban_launcher python script that we
    inject into containers."""
    return u.resource(LAUNCHER_SCRIPT)


def _default_launcher_config() -> Dict[str, Any]:
    return {
        "services": [],
        "env": {},
    }


def _create_mlflow_config(mlflow_cfg: Optional[Dict[str, Any]] = None,
                          uv_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Generates mlflow configuration dict for the launcher script."""
    if mlflow_cfg is None:
        return _default_launcher_config()

    uv_cfg = uv_cfg or {}
    uv_mlflow_cfg = uv_cfg.get("mlflow") or {}

    user = mlflow_cfg["user"]
    pw = mlflow_cfg["password"]
    db = mlflow_cfg["db"]
    project = mlflow_cfg["project"]
    region = mlflow_cfg["region"]
    artifact_root = mlflow_cfg["artifact_root"]
    debug = mlflow_cfg.get("debug", False)
    pubsub_project = uv_mlflow_cfg.get("pubsub_project", project)
    pubsub_topic = uv_mlflow_cfg.get("pubsub_topic", "mlflow")

    socket_path = "/tmp/cloudsql"
    proxy_path = os.path.join(os.sep, "usr", "bin", "cloud_sql_proxy")

    proxy_config = {
        "proxy": proxy_path,
        "path": socket_path,
        "project": project,
        "region": region,
        "db": db,
        "creds": "~/.config/gcloud/application_default_credentials.json",
        "debug": debug,
    }

    proxy_cmd = [
        "python",
        os.path.join(RESOURCE_DIR, CLOUD_SQL_WRAPPER_SCRIPT),
        json.dumps(proxy_config),
    ]

    tracking_uri = (
        f"postgresql+pg8000://{user}:{pw}@/{db}?unix_sock={socket_path}/"
        f"{project}:{region}:{db}/.s.PGSQL.5432"
    )

    return {
        "services": [proxy_cmd],
        "env": {
            "MLFLOW_TRACKING_URI": tracking_uri,
            "MLFLOW_ARTIFACT_ROOT": artifact_root,
            "UV_MLFLOW_PUBSUB_PROJECT": pubsub_project,
            "UV_MLFLOW_PUBSUB_TOPIC": pubsub_topic,
        },
    }


@contextmanager
def launcher_config_file(path: str,
                         caliban_config: Optional[Dict[str, Any]] = None):
    """Creates a configuration file for the caliban launcher script."""
    caliban_config = caliban_config or {}

    config = _default_launcher_config()
    config_file_path = os.path.join(path, LAUNCHER_CONFIG_FILE)

    mlflow_config = _create_mlflow_config(
        mlflow_cfg=caliban_config.get("mlflow_config"), uv_cfg=caliban_config.get("uv")
    )

    config["services"] += mlflow_config["services"]
    config["env"].update(mlflow_config["env"])

    with open(config_file_path, "w") as f:
        json.dump(config, f, indent=2)

    try:
        yield config_file_path
    finally:
        if os.path.exists(config_file_path):
            os.remove(config_file_path)


def _mlflow_job_name(index: int, user: str = None) -> str:
    user = user or u.current_user()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    return f"{user}-{timestamp}-{index}"


def mlflow_args(caliban_config: Dict[str, Any],
                experiment_name: str,
                index: int,
                tags: Dict[str, Any]) -> List[str]:
    """Returns mlflow args for caliban launcher."""
    if caliban_config.get("mlflow_config") is None:
        return []

    env = {f"ENVVAR_{k}": v for k, v in tags.items()}
    env["MLFLOW_EXPERIMENT_NAME"] = experiment_name
    env["MLFLOW_RUN_NAME"] = _mlflow_job_name(index=index)

    return ["--caliban_config", json.dumps({"env": env})]
