"""
ILAM Portal — Service Trino
Gère les requêtes SQL sur Iceberg via Trino.
"""
import time
import trino
import logging
from typing import List, Dict, Any
from app.config import settings
from app.models.schemas import TableInfo, TablePreview, QueryResult

logger = logging.getLogger(__name__)

SCHEMAS = ["bronze", "silver", "gold"]


def get_connection():
    return trino.dbapi.connect(
        host=settings.TRINO_HOST,
        port=settings.TRINO_PORT,
        user="portal",
        catalog="iceberg",
        request_timeout=60,
    )


def get_all_tables() -> List[TableInfo]:
    """Liste toutes les tables Iceberg avec leur count."""
    conn   = get_connection()
    cursor = conn.cursor()
    tables = []

    for schema in SCHEMAS:
        try:
            cursor.execute(f"""
                SELECT table_name
                FROM iceberg.information_schema.tables
                WHERE table_schema = '{schema}'
                ORDER BY table_name
            """)
            schema_tables = [row[0] for row in cursor.fetchall()]

            for table in schema_tables:
                try:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM iceberg.{schema}.{table}")
                    count = cursor.fetchone()[0]
                    tables.append(TableInfo(
                        schema_name=schema,
                        table_name=table,
                        row_count=count,
                    ))
                except Exception as e:
                    logger.warning(f"Count error {schema}.{table}: {e}")
                    tables.append(TableInfo(
                        schema_name=schema,
                        table_name=table,
                        row_count=0,
                    ))
        except Exception as e:
            logger.error(f"Schema {schema} error: {e}")

    return tables


def get_table_preview(
    schema: str, table: str, limit: int = 50
) -> TablePreview:
    """Retourne un aperçu des données d'une table."""
    conn   = get_connection()
    cursor = conn.cursor()

    # Colonnes
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM iceberg.information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name   = '{table}'
        ORDER BY ordinal_position
    """)
    columns_info = cursor.fetchall()
    columns      = [row[0] for row in columns_info]

    # Données
    cursor.execute(
        f"SELECT * FROM iceberg.{schema}.{table} LIMIT {limit}")
    rows = cursor.fetchall()
    rows = [[str(v) if v is not None else None
             for v in row] for row in rows]

    # Count total
    cursor.execute(
        f"SELECT COUNT(*) FROM iceberg.{schema}.{table}")
    total = cursor.fetchone()[0]

    return TablePreview(
        table_name=f"{schema}.{table}",
        columns=columns,
        rows=rows,
        total_rows=total,
    )


def execute_query(sql: str, limit: int = 100) -> QueryResult:
    """Exécute une requête SQL sur Trino."""
    start = time.time()
    try:
        conn   = get_connection()
        cursor = conn.cursor()

        # Sécurité basique — empêcher les requêtes destructives
        sql_upper = sql.strip().upper()
        forbidden = ["DROP", "DELETE", "TRUNCATE",
                     "INSERT", "UPDATE", "ALTER"]
        for kw in forbidden:
            if sql_upper.startswith(kw):
                return QueryResult(
                    columns=[],
                    rows=[],
                    row_count=0,
                    duration_ms=0,
                    error=f"Requête {kw} non autorisée dans le Portal.",
                )

        # Ajouter LIMIT si absent
        if "LIMIT" not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        cursor.execute(sql)
        rows    = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        rows    = [[str(v) if v is not None else None
                    for v in row] for row in rows]

        duration = (time.time() - start) * 1000
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            duration_ms=round(duration, 2),
        )

    except Exception as e:
        duration = (time.time() - start) * 1000
        logger.error(f"Query error: {e}")
        return QueryResult(
            columns=[],
            rows=[],
            row_count=0,
            duration_ms=round(duration, 2),
            error=str(e),
        )
