from __future__ import annotations

import json
import os
from pathlib import Path


def configure_simple_auth(*, airflow_home: Path, username: str, password: str) -> Path:
    username = username.strip()
    if not username:
        raise ValueError("AIRFLOW_USERNAME must not be empty")
    if not password:
        raise ValueError("AIRFLOW_PASSWORD must not be empty")

    password_file = airflow_home / "simple_auth_manager_passwords.json.generated"
    password_file.parent.mkdir(parents=True, exist_ok=True)
    temporary = password_file.with_suffix(password_file.suffix + ".tmp")
    temporary.write_text(
        json.dumps({username: password}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.chmod(0o600)
    temporary.replace(password_file)
    password_file.chmod(0o600)

    os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"] = f"{username}:admin"
    os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"] = str(
        password_file
    )
    return password_file


def main() -> None:
    airflow_home = Path(os.environ.get("AIRFLOW_HOME", "/home/airflow/.airflow"))
    configure_simple_auth(
        airflow_home=airflow_home,
        username=os.environ.get("AIRFLOW_USERNAME", "admin"),
        password=os.environ.get("AIRFLOW_PASSWORD", "news_pipeline_airflow_dev"),
    )
    os.execvp("airflow", ["airflow", "standalone"])


if __name__ == "__main__":
    main()
