from __future__ import annotations

import argparse

from equities_pnl_engine.services import (
    run_batch_pnl_job,
    run_streaming_trade_processor,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Equities PnL Engine CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("run-batch-pnl", help="Run a sample batch PnL job")
    subparsers.add_parser(
        "run-streaming-processor",
        help="Run the streaming trade lifecycle processor (Kafka)",
    )

    args = parser.parse_args()

    if args.command == "run-batch-pnl":
        run_batch_pnl_job()
    elif args.command == "run-streaming-processor":
        run_streaming_trade_processor()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

