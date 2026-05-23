"""
migrate.py — minimal Flyway-style migration runner for Postgres.

Discovers files named  Vxxx__description.sql  under ./migrations,
sorts them by version, and applies any that haven't already been
recorded in schema_migrations. Each migration runs inside its own
transaction with its checksum recorded after success.

Usage:
    python scripts/migrate.py              # apply all pending
    python scripts/migrate.py --status     # show applied / pending
    python scripts/migrate.py --dry-run    # print what would run

Configuration via environment variables (or a local .env file):
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate")


MIGRATION_FILENAME_RE = re.compile(r"^V(\d{3,})__([a-z0-9_]+)\.sql$")


@dataclass(frozen=True)
class Migration:
    version: str
    description: str
    path: Path
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def discover_migrations(migrations_dir: Path) -> list[Migration]:
    migrations = []
    for path in sorted(migrations_dir.glob("V*.sql")):
        match = MIGRATION_FILENAME_RE.match(path.name)
        if not match:
            log.warning("Skipping non-conforming filename: %s", path.name)
            continue
        version, description = match.group(1), match.group(2)
        migrations.append(Migration(
            version=version,
            description=description,
            path=path,
            sql=path.read_text(encoding="utf-8"),
        ))
    return migrations


def get_applied(conn) -> dict[str, str]:
    """Return {version: checksum} for migrations already in schema_migrations.
    Empty dict if the table doesn't exist yet (i.e. before V001 has run)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'schema_migrations'
            )
        """)
        exists = cur.fetchone()[0]
        if not exists:
            return {}
        cur.execute("""
            SELECT version, checksum FROM schema_migrations WHERE success = TRUE
        """)
        return {v: c for v, c in cur.fetchall()}


def apply_migration(conn, migration: Migration) -> None:
    """Run a single migration. Records success in schema_migrations as part
    of the same transaction — if the migration fails, the row is never
    written, and the next run will retry from this version."""
    log.info("Applying V%s — %s", migration.version, migration.description)
    start = time.perf_counter()
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(migration.sql)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            # Skip the audit insert for V001 itself — the table doesn't
            # exist yet at the start of the V001 statement, so we insert
            # after the CREATE TABLE has run within the same transaction.
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO schema_migrations
                        (version, description, script_filename, checksum,
                         execution_time_ms, success)
                    VALUES (%s, %s, %s, %s, %s, TRUE)
                """, (
                    migration.version, migration.description,
                    migration.path.name, migration.checksum, elapsed_ms,
                ))
        log.info("  ✓ V%s applied in %d ms", migration.version, elapsed_ms)
    except Exception as e:
        log.error("  ✗ V%s FAILED: %s", migration.version, e)
        raise


def verify_checksums(applied: dict[str, str], discovered: list[Migration]) -> None:
    """Flyway-style integrity check. If a migration file's contents changed
    after being applied, that's a foot-gun we want to catch early."""
    for m in discovered:
        if m.version in applied and applied[m.version] != m.checksum:
            raise RuntimeError(
                f"Checksum mismatch for V{m.version} ({m.path.name}). "
                f"The file changed after being applied. Either revert the "
                f"file, or write a new V{int(max(applied) or 0) + 1}__... "
                f"migration with your change."
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", action="store_true",
                        help="Show applied/pending without running anything")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be applied, but don't connect or run")
    parser.add_argument("--migrations-dir", type=Path,
                        default=Path(__file__).parent.parent / "migrations")
    args = parser.parse_args()

    discovered = discover_migrations(args.migrations_dir)
    if not discovered:
        log.error("No migrations found in %s", args.migrations_dir)
        sys.exit(1)

    if args.dry_run:
        log.info("Discovered %d migration(s):", len(discovered))
        for m in discovered:
            log.info("  V%s — %s", m.version, m.description)
        return

    dsn = (
        f"host={os.environ.get('PGHOST', 'localhost')} "
        f"port={os.environ.get('PGPORT', '5432')} "
        f"dbname={os.environ.get('PGDATABASE', 'amanotes')} "
        f"user={os.environ.get('PGUSER', 'postgres')} "
        f"password={os.environ.get('PGPASSWORD', 'postgres')}"
    )

    with psycopg.connect(dsn) as conn:
        applied = get_applied(conn)
        verify_checksums(applied, discovered)

        if args.status:
            log.info("Applied: %d, Discovered: %d", len(applied), len(discovered))
            for m in discovered:
                state = "applied" if m.version in applied else "PENDING"
                log.info("  V%s [%s] — %s", m.version, state, m.description)
            return

        pending = [m for m in discovered if m.version not in applied]
        if not pending:
            log.info("Nothing to do — all %d migration(s) already applied.",
                     len(discovered))
            return

        log.info("Applying %d pending migration(s)...", len(pending))
        for m in pending:
            apply_migration(conn, m)
        log.info("Done. %d migration(s) applied.", len(pending))


if __name__ == "__main__":
    main()
