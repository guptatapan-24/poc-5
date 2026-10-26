"""Django management command to run the live event consumer."""

from django.core.management.base import BaseCommand
from event_pipeline.live_consumer import run_standalone_live_consumer


class Command(BaseCommand):
    help = "Runs the live Kafka event consumer forwarding 'stock-events' to Django Channels / WebSockets"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting Live Event Consumer service..."))
        run_standalone_live_consumer()
