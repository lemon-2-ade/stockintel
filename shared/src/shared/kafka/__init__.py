"""Kafka topology, client configuration and event (de)serialisation.

Nothing here imports ``confluent_kafka`` at module import time, so contracts
can be used (and tested) without librdkafka installed.
"""

from shared.kafka.serde import EventDeserializationError, EventSerde, JsonEventSerde
from shared.kafka.topics import TOPICS, Topic, TopicSpec, spec_for

__all__ = [
    "TOPICS",
    "EventDeserializationError",
    "EventSerde",
    "JsonEventSerde",
    "Topic",
    "TopicSpec",
    "spec_for",
]
