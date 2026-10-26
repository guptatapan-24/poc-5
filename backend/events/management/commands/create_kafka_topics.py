"""Django management command to programmatically create Kafka topics if they do not exist."""

import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Creates required Kafka topics (stock-prices, stock-events) with defined partitions and retention"

    def handle(self, *args, **options):
        bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
        self.stdout.write(f"Connecting to Kafka at {bootstrap_servers}...")

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="topic-creator-admin",
                request_timeout_ms=10000,
            )

            topics_to_create = [
                NewTopic(
                    name=settings.KAFKA_TOPIC_PRICES,
                    num_partitions=2,
                    replication_factor=1,
                    topic_configs={"retention.ms": "604800000"},
                ),
                NewTopic(
                    name=settings.KAFKA_TOPIC_EVENTS,
                    num_partitions=2,
                    replication_factor=1,
                    topic_configs={"retention.ms": "604800000"},
                ),
            ]

            existing_topics = set(admin_client.list_topics())
            created_count = 0

            for topic in topics_to_create:
                if topic.name in existing_topics:
                    self.stdout.write(self.style.WARNING(f"Topic '{topic.name}' already exists."))
                else:
                    try:
                        admin_client.create_topics(new_topics=[topic], validate_only=False)
                        self.stdout.write(self.style.SUCCESS(f"Created topic '{topic.name}' successfully."))
                        created_count += 1
                    except TopicAlreadyExistsError:
                        self.stdout.write(self.style.WARNING(f"Topic '{topic.name}' already exists."))

            admin_client.close()
            self.stdout.write(self.style.SUCCESS(f"Finished topic creation. Total created: {created_count}"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to connect or create topics in Kafka: {e}"))
