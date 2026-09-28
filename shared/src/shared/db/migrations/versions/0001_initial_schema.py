"""Initial schema: market data, indicators, anomalies, predictions, MLOps and watchlists.

Revision ID: 0001
Revises:
Create Date: 2026-09-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "anomalies",
        sa.Column("anomaly_id", sa.UUID(), nullable=False),
        sa.Column("symbol", sa.String(length=15), nullable=False),
        sa.Column("bar_interval", sa.String(length=4), nullable=False),
        sa.Column("ts", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("anomaly_type", sa.String(length=32), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("observed_value", sa.Double(), nullable=False),
        sa.Column("expected_value", sa.Double(), nullable=True),
        sa.Column("score", sa.Double(), nullable=False),
        sa.Column("threshold", sa.Double(), nullable=True),
        sa.Column("detector", sa.String(length=64), nullable=False),
        sa.Column("detector_version", sa.String(length=32), nullable=False),
        sa.Column("source_event_id", sa.UUID(), nullable=False),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "anomaly_type IN ('price_spike', 'price_drop', 'return_zscore', 'volume_spike', 'multivariate_outlier')",
            name=op.f("ck_anomalies_anomaly_type"),
        ),
        sa.CheckConstraint(
            "severity IN ('low', 'medium', 'high', 'critical')", name=op.f("ck_anomalies_severity")
        ),
        sa.PrimaryKeyConstraint("anomaly_id", name=op.f("pk_anomalies")),
        sa.UniqueConstraint(
            "source_event_id",
            "detector",
            "anomaly_type",
            name=op.f("uq_anomalies_source_event_id_detector_anomaly_type"),
        ),
    )
    op.create_index(
        "ix_anomalies_symbol_ts",
        "anomalies",
        ["symbol", sa.literal_column("ts DESC")],
        unique=False,
    )
    op.create_index("ix_anomalies_ts", "anomalies", [sa.literal_column("ts DESC")], unique=False)
    op.create_table(
        "bar_indicators",
        sa.Column("symbol", sa.String(length=15), nullable=False),
        sa.Column("bar_interval", sa.String(length=4), nullable=False),
        sa.Column("ts", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("indicators", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("indicator_version", sa.String(length=32), nullable=False),
        sa.Column("source_event_id", sa.UUID(), nullable=False),
        sa.Column(
            "computed_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("symbol", "bar_interval", "ts", name=op.f("pk_bar_indicators")),
    )
    op.create_table(
        "market_bars",
        sa.Column("symbol", sa.String(length=15), nullable=False),
        sa.Column("bar_interval", sa.String(length=4), nullable=False),
        sa.Column(
            "ts", postgresql.TIMESTAMP(timezone=True), nullable=False, comment="Bar open time (UTC)"
        ),
        sa.Column("open", sa.Double(), nullable=False),
        sa.Column("high", sa.Double(), nullable=False),
        sa.Column("low", sa.Double(), nullable=False),
        sa.Column("close", sa.Double(), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column(
            "ingested_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "bar_interval IN ('1s', '5s', '1m', '5m', '15m', '1h', '1d')",
            name=op.f("ck_market_bars_bar_interval"),
        ),
        sa.CheckConstraint(
            "high >= GREATEST(open, close, low) AND low <= LEAST(open, close)",
            name=op.f("ck_market_bars_ohlc"),
        ),
        sa.CheckConstraint(
            "open > 0 AND high > 0 AND low > 0 AND close > 0",
            name=op.f("ck_market_bars_positive_prices"),
        ),
        sa.CheckConstraint("volume >= 0", name=op.f("ck_market_bars_non_negative_volume")),
        sa.PrimaryKeyConstraint("symbol", "bar_interval", "ts", name=op.f("pk_market_bars")),
        sa.UniqueConstraint("event_id", name=op.f("uq_market_bars_event_id")),
    )
    op.create_index(
        "ix_market_bars_ts_brin", "market_bars", ["ts"], unique=False, postgresql_using="brin"
    )
    op.create_table(
        "model_deployments",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("model_version", sa.String(length=64), nullable=False),
        sa.Column("from_stage", sa.String(length=16), nullable=True),
        sa.Column("to_stage", sa.String(length=16), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column(
            "metrics",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "from_stage IS NULL OR from_stage IN ('candidate', 'challenger', 'champion', 'archived')",
            name=op.f("ck_model_deployments_from_stage"),
        ),
        sa.CheckConstraint(
            "to_stage IN ('candidate', 'challenger', 'champion', 'archived')",
            name=op.f("ck_model_deployments_to_stage"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_deployments")),
    )
    op.create_index(
        "ix_model_deployments_model_created",
        "model_deployments",
        ["model_name", sa.literal_column("created_at DESC")],
        unique=False,
    )
    op.create_table(
        "monitoring_reports",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("report_type", sa.String(length=32), nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=True),
        sa.Column("model_version", sa.String(length=64), nullable=True),
        sa.Column("window_start", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("window_end", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("metric_name", sa.String(length=64), nullable=False),
        sa.Column("feature_name", sa.String(length=128), nullable=True),
        sa.Column("value", sa.Double(), nullable=False),
        sa.Column("threshold", sa.Double(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "report_type IN ('data_quality', 'data_drift', 'prediction_drift', 'performance', 'operational')",
            name=op.f("ck_monitoring_reports_report_type"),
        ),
        sa.CheckConstraint(
            "status IN ('ok', 'warning', 'alert')", name=op.f("ck_monitoring_reports_status")
        ),
        sa.CheckConstraint("window_end > window_start", name=op.f("ck_monitoring_reports_window")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_monitoring_reports")),
    )
    op.create_index(
        "ix_monitoring_reports_type_created",
        "monitoring_reports",
        ["report_type", sa.literal_column("created_at DESC")],
        unique=False,
    )
    op.create_table(
        "predictions",
        sa.Column(
            "prediction_id", sa.UUID(), nullable=False, comment="Deterministic uuid5, see schemas"
        ),
        sa.Column("symbol", sa.String(length=15), nullable=False),
        sa.Column("bar_interval", sa.String(length=4), nullable=False),
        sa.Column(
            "ts",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            comment="As-of time of the features",
        ),
        sa.Column("target_ts", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("horizon_bars", sa.Integer(), nullable=False),
        sa.Column("task", sa.String(length=16), nullable=False),
        sa.Column("predicted_direction", sa.String(length=8), nullable=True),
        sa.Column("predicted_return", sa.Double(), nullable=True),
        sa.Column("class_probabilities", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("confidence", sa.Double(), nullable=True),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("model_version", sa.String(length=64), nullable=False),
        sa.Column("feature_set_version", sa.String(length=64), nullable=False),
        sa.Column(
            "features",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Model input snapshot, used for drift monitoring",
        ),
        sa.Column("inference_latency_ms", sa.Double(), nullable=True),
        sa.Column("source_event_id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "predicted_direction IS NULL OR predicted_direction IN ('up', 'down', 'neutral')",
            name=op.f("ck_predictions_predicted_direction"),
        ),
        sa.CheckConstraint("task IN ('direction', 'return')", name=op.f("ck_predictions_task")),
        sa.CheckConstraint(
            "confidence IS NULL OR confidence BETWEEN 0 AND 1",
            name=op.f("ck_predictions_confidence"),
        ),
        sa.CheckConstraint("horizon_bars >= 1", name=op.f("ck_predictions_positive_horizon")),
        sa.CheckConstraint("target_ts > ts", name=op.f("ck_predictions_target_after_asof")),
        sa.PrimaryKeyConstraint("prediction_id", name=op.f("pk_predictions")),
    )
    op.create_index(
        "ix_predictions_model_ts",
        "predictions",
        ["model_name", "model_version", "ts"],
        unique=False,
    )
    op.create_index(
        "ix_predictions_symbol_ts",
        "predictions",
        ["symbol", sa.literal_column("ts DESC")],
        unique=False,
    )
    op.create_index("ix_predictions_target_ts", "predictions", ["target_ts"], unique=False)
    op.create_table(
        "symbols",
        sa.Column("symbol", sa.String(length=15), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("exchange", sa.String(length=32), nullable=True),
        sa.Column("currency", sa.String(length=3), server_default=sa.text("'USD'"), nullable=False),
        sa.Column("sector", sa.String(length=64), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("symbol", name=op.f("pk_symbols")),
    )
    op.create_table(
        "prediction_outcomes",
        sa.Column("prediction_id", sa.UUID(), nullable=False),
        sa.Column("actual_return", sa.Double(), nullable=False),
        sa.Column("actual_direction", sa.String(length=8), nullable=False),
        sa.Column("is_correct", sa.Boolean(), nullable=True),
        sa.Column("abs_error", sa.Double(), nullable=True),
        sa.Column(
            "resolved_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "actual_direction IN ('up', 'down', 'neutral')",
            name=op.f("ck_prediction_outcomes_actual_direction"),
        ),
        sa.ForeignKeyConstraint(
            ["prediction_id"],
            ["predictions.prediction_id"],
            name=op.f("fk_prediction_outcomes_prediction_id_predictions"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("prediction_id", name=op.f("pk_prediction_outcomes")),
    )
    op.create_index(
        "ix_prediction_outcomes_resolved_at", "prediction_outcomes", ["resolved_at"], unique=False
    )
    op.create_table(
        "watchlist_items",
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=15), nullable=False),
        sa.Column("position", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "added_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["symbol"],
            ["symbols.symbol"],
            name=op.f("fk_watchlist_items_symbol_symbols"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("user_id", "symbol", name=op.f("pk_watchlist_items")),
    )


def downgrade() -> None:
    op.drop_table("watchlist_items")
    op.drop_index("ix_prediction_outcomes_resolved_at", table_name="prediction_outcomes")
    op.drop_table("prediction_outcomes")
    op.drop_table("symbols")
    op.drop_index("ix_predictions_target_ts", table_name="predictions")
    op.drop_index("ix_predictions_symbol_ts", table_name="predictions")
    op.drop_index("ix_predictions_model_ts", table_name="predictions")
    op.drop_table("predictions")
    op.drop_index("ix_monitoring_reports_type_created", table_name="monitoring_reports")
    op.drop_table("monitoring_reports")
    op.drop_index("ix_model_deployments_model_created", table_name="model_deployments")
    op.drop_table("model_deployments")
    op.drop_index("ix_market_bars_ts_brin", table_name="market_bars", postgresql_using="brin")
    op.drop_table("market_bars")
    op.drop_table("bar_indicators")
    op.drop_index("ix_anomalies_ts", table_name="anomalies")
    op.drop_index("ix_anomalies_symbol_ts", table_name="anomalies")
    op.drop_table("anomalies")
