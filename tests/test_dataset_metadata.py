from __future__ import annotations

import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

CATALOG_PATH = Path("analysis/datasets/dataset-catalog.yaml")
CATALOG_SCHEMA_PATH = Path("analysis/datasets/dataset-catalog.schema.json")


def load_catalog() -> dict:
    return yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_dataset_catalog_schema_is_valid() -> None:
    Draft202012Validator.check_schema(load_json(CATALOG_SCHEMA_PATH))


def test_dataset_catalog_matches_schema() -> None:
    validator = Draft202012Validator(
        load_json(CATALOG_SCHEMA_PATH), format_checker=FormatChecker()
    )
    validator.validate(load_catalog())


def test_catalog_ids_and_contract_are_unique_and_current() -> None:
    catalog = load_catalog()
    datasets = catalog["datasets"]

    assert catalog["contract"]["version"] == 1
    assert {dataset["id"] for dataset in datasets} == {
        "gdelt-doc-news",
        "pushshift-reddit-comments",
    }
    assert len({dataset["id"] for dataset in datasets}) == len(datasets)
    assert len({dataset["source_type"] for dataset in datasets}) == len(datasets)


def test_catalog_local_references_exist() -> None:
    catalog = load_catalog()
    paths = [
        catalog["contract"]["documentation_path"],
        catalog["contract"]["schema_path"],
    ]
    for dataset in catalog["datasets"]:
        paths.extend(
            [
                dataset["specification_path"],
                dataset["validation"]["profile_path"],
                dataset["reproduction"]["collector"],
            ]
        )

    missing = [path for path in paths if not Path(path).is_file()]
    assert missing == []


def test_profile_status_matches_catalog_and_counts_are_consistent() -> None:
    for dataset in load_catalog()["datasets"]:
        profile = load_json(Path(dataset["validation"]["profile_path"]))
        counts = profile["counts"]

        assert profile["dataset_id"] == dataset["id"]
        assert profile["status"] == dataset["validation"]["status"]
        assert profile["validated_at"] == dataset["validation"]["checked_at"]

        if profile["status"] == "passed":
            assert counts["schema_passed"] + counts["schema_errors"] == counts["events"]
            assert counts["duplicate_event_ids"] <= counts["events"]
            assert dataset["project_scope"]["sample_size"] == counts["events"]
        else:
            assert counts["events"] is None


def test_public_profiles_do_not_contain_raw_records() -> None:
    forbidden_keys = {"text", "body", "author", "url", "event_id", "id"}
    for path in Path("analysis/reports").glob("*.json"):
        profile = load_json(path)

        def visit(value: object) -> None:
            if isinstance(value, dict):
                assert forbidden_keys.isdisjoint(value)
                for nested in value.values():
                    visit(nested)
            elif isinstance(value, list):
                for nested in value:
                    visit(nested)

        visit(profile)

