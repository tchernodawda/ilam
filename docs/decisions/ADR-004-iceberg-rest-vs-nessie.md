# ADR-004 — tabulario/iceberg-rest over Project Nessie

**Date:** 2025
**Status:** Accepted (Development environment only)
**Deciders:** Ilam core team

## Context

The Ilam platform requires an Iceberg catalog to manage table metadata,
enabling Trino and other engines to discover and query Iceberg tables stored
in MinIO. Two candidates were evaluated: Project Nessie and the
tabulario/iceberg-rest server.

## Problem encountered

Trino 435 with the Iceberg REST connector communicates via the standard
Iceberg REST Catalog API (OpenAPI spec). Project Nessie exposes its own
versioned API (`/api/v1`, `/api/v2`) which is **not directly compatible**
with the Iceberg REST Catalog protocol expected by Trino 435.

Attempting to use Nessie as a REST catalog with Trino 435 produced:

RESTException: Unable to process —
No content to map due to end-of-input

## Alternatives evaluated

| Criterion | tabulario/iceberg-rest | Project Nessie | Hive Metastore |
|-----------|----------------------|----------------|----------------|
| Trino 435 compatibility | Native | Partial (protocol mismatch) | Via Hive connector |
| Setup complexity | Minimal | Medium | High |
| Git-like versioning | No | Yes | No |
| Persistence (default) | In-memory (SQLite) | In-memory or PostgreSQL | PostgreSQL |
| Production-ready | Dev/staging | Yes | Yes |
| Extra dependencies | None | PostgreSQL recommended | PostgreSQL |

## Decision

Use **tabulario/iceberg-rest** for the development environment.

## Rationale

Immediate native compatibility with Trino 435 REST connector, zero external
dependencies, minimal configuration. Sufficient for development, testing,
and PFE demonstration purposes.

## Known limitation — metadata persistence

The default configuration uses SQLite in-memory storage. Table metadata
is lost on container restart. Mitigation: `make init-warehouse` must be
run after every restart to recreate table definitions in the catalog.

A volume-based SQLite persistence was attempted:

```yaml
environment:
  CATALOG_URI: jdbc:sqlite:/catalog/iceberg_catalog.db
volumes:
  - iceberg_catalog:/catalog
```

This failed with `Permission denied` because Docker creates volumes owned
by root, while the Java process in the container runs as a non-root user.

## Consequences

- All Iceberg table metadata must be recreated after each container restart
  via `make init-warehouse`
- Data (Parquet files in MinIO) persists across restarts via the
  `ilam-minio-data` Docker volume — only the catalog index is lost
- For a Sonatel production deployment, Project Nessie with a PostgreSQL
  backend would replace this component, providing full metadata persistence
  and Git-like table versioning

## Production recommendation
Development (PFE)              Production (Sonatel)
──────────────────────         ──────────────────────────
tabulario/iceberg-rest         Project Nessie

SQLite in-memory             + PostgreSQL backend
make init-warehouse          + Native persistence
on each restart                across all restarts