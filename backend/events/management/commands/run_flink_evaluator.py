"""Django management command to run the Apache Flink and Flink CEP stream evaluator."""

from django.core.management.base import BaseCommand
from event_pipeline.flink_evaluator import run_standalone_flink


class Command(BaseCommand):
    help = "Runs the Apache Flink and Flink CEP stream evaluator"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting Apache Flink and Flink CEP Stream Evaluator..."))
        run_standalone_flink()
