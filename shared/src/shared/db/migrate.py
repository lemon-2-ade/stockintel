"""Programmatic Alembic entry point.

Migrations ship *inside* this package next to the models they describe, so
the same wheel/image that contains the models can migrate the database - no
separate ``alembic.ini`` to keep in sync.

Usage::

    python -m shared.db.migrate upgrade [head]
    python -m shared.db.migrate downgrade <rev>
    python -m shared.db.migrate revision -m "message"   # autogenerate
    python -m shared.db.migrate current
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

from shared.config import LogSettings, PostgresSettings
from shared.observability.logs import configure_logging

MIGRATIONS_DIR = Path(__file__).with_name("migrations")


def alembic_config(settings: PostgresSettings | None = None) -> Config:
    settings = settings or PostgresSettings()
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    # ConfigParser interpolation: a literal % in the password must be doubled.
    config.set_main_option("sqlalchemy.url", settings.sqlalchemy_url().replace("%", "%%"))
    config.set_main_option("file_template", "%%(rev)s_%%(slug)s")
    return config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Database migrations")
    sub = parser.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("upgrade")
    up.add_argument("revision", nargs="?", default="head")
    down = sub.add_parser("downgrade")
    down.add_argument("revision")
    rev = sub.add_parser("revision")
    rev.add_argument("-m", "--message", required=True)
    sub.add_parser("current")
    args = parser.parse_args(argv)

    log_settings = LogSettings()
    configure_logging("db-migrate", level=log_settings.level, fmt=log_settings.format)
    config = alembic_config()
    match args.cmd:
        case "upgrade":
            command.upgrade(config, args.revision)
        case "downgrade":
            command.downgrade(config, args.revision)
        case "revision":
            command.revision(config, message=args.message, autogenerate=True)
        case "current":
            command.current(config, verbose=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
