import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from sqlalchemy import text


LOGGER = logging.getLogger(__name__)


def _migration_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.glob("*.sql") if path.name[:4].isdigit())


def _sqlite_path(database_url: str) -> Path | None:
    prefix = "sqlite:///"
    if not str(database_url or "").startswith(prefix):
        return None
    raw = str(database_url)[len(prefix):]
    return Path(raw).resolve() if raw else None


def _backup_database(database_url: str, backup_directory: Path) -> Path | None:
    database = _sqlite_path(database_url)
    if database is None or not database.exists() or database.stat().st_size == 0:
        return None
    backup_directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    target = backup_directory / f"{database.name}.before-migrations-{stamp}.bak"
    # API de backup do SQLite inclui páginas ainda presentes no WAL e produz
    # uma cópia consistente mesmo com leituras concorrentes do site.
    source = sqlite3.connect(str(database), timeout=30)
    destination = sqlite3.connect(str(target), timeout=30)
    try:
        source.backup(destination)
    finally:
        destination.close()
        source.close()
    return target


def _statements(sql: str) -> list[str]:
    # As migrations do projeto são DDL simples e não contêm triggers nem
    # strings com ponto-e-vírgula. Manter este parser deliberadamente restrito
    # evita executar formatos inesperados.
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def run_schema_migrations(engine, migrations_directory: str | os.PathLike | None = None) -> list[str]:
    root = Path(migrations_directory or Path(__file__).resolve().parents[2] / "migrations")
    files = _migration_files(root)
    if not files:
        return []

    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE IF NOT EXISTS schema_migration ("
            "version VARCHAR(160) PRIMARY KEY, applied_at DATETIME NOT NULL, checksum VARCHAR(64))"
        ))
        applied = {row[0] for row in connection.execute(text("SELECT version FROM schema_migration"))}
    pending = [path for path in files if path.name not in applied]
    if not pending:
        return []

    database_url = str(engine.url)
    backup = _backup_database(database_url, root.parent / "backup")
    if _sqlite_path(database_url) is not None and backup is None:
        database = _sqlite_path(database_url)
        if database is not None and database.exists():
            raise RuntimeError("Não foi possível criar o backup obrigatório antes das migrations.")
    if backup:
        LOGGER.info("Backup antes das migrations criado em %s", backup)

    completed = []
    import hashlib
    for path in pending:
        sql = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        with engine.begin() as connection:
            for statement in _statements(sql):
                try:
                    connection.execute(text(statement))
                except Exception as exc:
                    # db.create_all() pode ter criado a coluna nova em bancos
                    # recém-inicializados antes da migration ser registrada.
                    duplicate_column = "duplicate column name" in str(exc).casefold()
                    if not duplicate_column:
                        raise
            connection.execute(
                text("INSERT INTO schema_migration(version,applied_at,checksum) VALUES (:version,CURRENT_TIMESTAMP,:checksum)"),
                {"version": path.name, "checksum": checksum},
            )
        completed.append(path.name)
        LOGGER.info("Migration aplicada: %s", path.name)
    return completed
