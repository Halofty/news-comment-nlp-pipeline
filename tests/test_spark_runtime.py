from pathlib import Path

from spark_jobs.runtime import configure_java_home


def test_configure_java_home_respects_existing_value(monkeypatch) -> None:
    monkeypatch.setenv("JAVA_HOME", "/custom/java")

    assert configure_java_home() == "/custom/java"


def test_configure_java_home_detects_local_installation(monkeypatch) -> None:
    monkeypatch.delenv("JAVA_HOME", raising=False)

    configured = configure_java_home()

    local_java = Path(".tools/java17-root/usr/lib/jvm/java-17-openjdk-amd64")
    if local_java.is_dir():
        assert configured == str(local_java.resolve())
