from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from infra.airflow.start_standalone import configure_simple_auth


def test_airflow_image_includes_langfuse_runtime_dependency() -> None:
    requirements = Path("infra/airflow/requirements.txt").read_text(encoding="utf-8")

    assert "langfuse>=4,<5" in requirements.splitlines()


def test_configure_simple_auth_writes_password_file_and_environment(tmp_path) -> None:
    path = configure_simple_auth(
        airflow_home=tmp_path,
        username="local-admin",
        password="local-password",
    )
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "local-admin": "local-password"
    }
    assert path.stat().st_mode & 0o777 == 0o600
    assert os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"] == (
        "local-admin:admin"
    )
    assert os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"] == str(
        path
    )


@pytest.mark.parametrize("username,password", [("", "pw"), ("admin", "")])
def test_configure_simple_auth_rejects_empty_credentials(
    tmp_path, username, password
) -> None:
    with pytest.raises(ValueError):
        configure_simple_auth(
            airflow_home=tmp_path,
            username=username,
            password=password,
        )
