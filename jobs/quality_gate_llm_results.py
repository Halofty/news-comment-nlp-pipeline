from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from llm_analysis.quality import apply_quality_gate


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize and validate LLM topic and keyword labels"
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = apply_quality_gate(
        input_path=args.input, output_path=args.output, report_path=args.report
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

