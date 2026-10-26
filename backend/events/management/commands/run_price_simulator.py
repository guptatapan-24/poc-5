"""Django management command to run the stock price simulator."""

from django.core.management.base import BaseCommand
from event_pipeline.price_simulator import run_standalone_simulator


class Command(BaseCommand):
    help = "Runs the stock price simulator publishing updates to Kafka 'stock-prices'"

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval",
            type=float,
            default=1.0,
            help="Interval between price updates in seconds (default: 1.0)",
        )
        parser.add_argument(
            "--mode",
            type=str,
            default="deterministic",
            choices=["deterministic", "random"],
            help="Simulation mode: 'deterministic' (canonical AAPL sequence) or 'random'",
        )
        parser.add_argument(
            "--symbols",
            nargs="+",
            default=["AAPL"],
            help="Stock symbols to simulate (e.g. --symbols AAPL MSFT GOOGL)",
        )
        parser.add_argument(
            "--iterations",
            type=int,
            default=None,
            help="Optional max number of price cycles to generate before stopping",
        )

    def handle(self, *args, **options):
        interval = options["interval"]
        mode = options["mode"]
        symbols = options["symbols"]
        iterations = options["iterations"]

        self.stdout.write(
            self.style.SUCCESS(
                f"Starting Price Simulator [interval={interval}s, mode={mode}, symbols={symbols}, iterations={iterations}]"
            )
        )
        run_standalone_simulator(
            interval=interval,
            mode=mode,
            symbols=symbols,
            iterations=iterations,
        )
