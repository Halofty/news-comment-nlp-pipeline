from __future__ import annotations

import os
from pathlib import Path


def configure_java_home() -> str | None:
    """Use JAVA_HOME or the project-local Java 17 installation when available."""
    configured = os.environ.get("JAVA_HOME")
    if configured:
        return configured

    project_root = Path(__file__).resolve().parents[1]
    local_java_home = (
        project_root
        / ".tools"
        / "java17-root"
        / "usr"
        / "lib"
        / "jvm"
        / "java-17-openjdk-amd64"
    )
    if not (local_java_home / "bin" / "java").is_file():
        return None

    os.environ["JAVA_HOME"] = str(local_java_home)
    os.environ["PATH"] = (
        f"{local_java_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
    )
    return str(local_java_home)
