from __future__ import annotations

import pytest

from spark_jobs.minio_roundtrip import build_s3a_config


def test_s3a_config_uses_path_style_and_http_for_local_minio() -> None:
    config = build_s3a_config(
        endpoint="http://minio:9000/", access_key="key", secret_key="secret"
    )
    assert config["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert config["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert config["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert config["spark.hadoop.fs.s3a.access.key"] == "key"


def test_s3a_config_requires_credentials() -> None:
    with pytest.raises(ValueError):
        build_s3a_config(endpoint="http://minio:9000", access_key="", secret_key="")
