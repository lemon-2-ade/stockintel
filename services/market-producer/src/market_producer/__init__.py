"""Market data producer service.

Turns a :class:`~market_producer.providers.base.MarketDataProvider` (the GBM
simulator or a historical replay) into validated ``market.bar`` events on the
``market.raw`` Kafka topic.
"""
