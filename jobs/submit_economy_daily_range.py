from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from jobs.openai_batch import run as run_openai_batch


TERMINAL_RETRYABLE = {"failed", "expired", "cancelled"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit one independent OpenAI Batch per economy-and-society day"
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--response-root", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--start-day", type=int, required=True)
    parser.add_argument("--end-day", type=int, required=True)
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--prompt-version", default="economy-society-daily-v1")
    parser.add_argument("--summary-output", type=Path, required=True)
    return parser


def submit_range(args: argparse.Namespace) -> dict[str, object]:
    if not 1 <= args.start_day <= args.end_day <= 31:
        raise ValueError("day range must be within 1..31")
    submitted: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []
    for day in range(args.start_day, args.end_day + 1):
        period = f"{args.year:04d}-{args.month:02d}-{day:02d}"
        artifact = args.artifact_root / period
        response = args.response_root / period
        state_path = response / "batch-state.json"
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8"))
            status = str(state.get("status") or "unknown")
            if state.get("id") and status not in TERMINAL_RETRYABLE:
                skipped.append(
                    {"period": period, "batch_id": str(state["id"]), "status": status}
                )
                continue
        value = run_openai_batch(
            argparse.Namespace(
                command="submit",
                request_file=artifact / "requests.jsonl",
                preflight_report=artifact / "preflight.json",
                state_output=state_path,
                internal_batch_id=f"economy-social-{period}",
                model=args.model,
                prompt_version=args.prompt_version,
            )
        )
        submitted.append(
            {"period": period, "batch_id": str(value["id"]), "status": str(value["status"])}
        )
    result: dict[str, object] = {
        "submitted_count": len(submitted),
        "skipped_count": len(skipped),
        "submitted": submitted,
        "skipped": skipped,
    }
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)
    args.summary_output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(args.summary_output)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = submit_range(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
