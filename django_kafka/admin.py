import logging

from confluent_kafka.admin import AdminClient, NewTopic
from django.conf import settings

# Configura o logger específico para a sua biblioteca
logger = logging.getLogger(__name__)


class SetupDjangoKafka:
    def create_topics(self, adm: AdminClient, topics) -> None:
        """Create topics"""

        new_topics = [
            NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics
        ]
        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
        fs = adm.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                # print("Topic {} created".format(topic))
                logger.info("Topic {} created".format(topic))
            except Exception as e:
                # print("Failed to create topic {}: {}".format(topic, e))
                logger.error("Failed to create topic {}: {}".format(topic, e))

    def setup(self) -> None:
        adminClient: AdminClient = AdminClient(
            {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVER}
        )

        topics = [key for key, _ in settings.KAFKA_TOPICS.items()]

        self.create_topics(adminClient, topics)
