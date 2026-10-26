"""Django management command to run the threshold evaluator."""

from django.core.management.base import BaseCommand
from event_pipeline.threshold_evaluator import run_standalone_evaluator


class Command(BaseCommand):
    help = "Runs the threshold evaluator consuming 'stock-prices' and publishing to 'stock-events'"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting Threshold Evaluator service..."))
        run_standalone_evaluator()
