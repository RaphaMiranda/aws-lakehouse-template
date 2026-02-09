# Architecture Decisions

## Silver Layer: Table Format & History Strategy

### Table Format: Apache Iceberg
We chose **Apache Iceberg** for the Silver (and Gold) layers for the following reasons:
1.  **ACID Transactions**: Guarantees consistency during concurrent writes/reads.
2.  **Schema Evolution**: Supports adding/renaming/deleting columns without expensive rewriting.
3.  **Partition Evolution**: Can update partition schemes without rewriting data.
4.  **Time Travel**: Allows querying data as of a specific point in time (Snapshots).

### History Tracking & SCD Type 2
**Requirement**: Guarantee ACID transactions, consistency, and historical tracking.

**Options**:
1.  **Traditional SCD Type 2**: Explicit `valid_from`, `valid_to`, `is_current` columns managed by the ETL logic.
2.  **Modern Lakehouse (Iceberg)**: Use Iceberg's native history/snapshot capabilities for auditing and "Time Travel", but keep the main view as "Current State" (SCD Type 1) via MERGE (Upsert).

**Decision**: **Hybrid / Upsert (SCD Type 1)** for the main table, relying on Iceberg History for audit.
-   **Reasoning**: For most Data Engineering use cases, having a queryable "Current State" is the primary requirement. Managing explicit SCD2 columns in data lakes can lead to explosion of small files or complex compaction needs.
-   Iceberg supports `MERGE INTO` which acts as an Upsert (Update matching keys, Insert new).
-   If "Point-in-Time" analysis is strictly required for business logic, Iceberg's `FOR SYSTEM_TIME_AS_OF` syntax can be used.
-   *Note*: If strict regulatory requirements demand explicit SCD2 columns for BI tools that don't support Iceberg Time Travel, we would add those columns. For this template, we prioritize simplicity and modern patterns.

## Glue vs Athena for ETL
We chose **AWS Glue (Spark)** for the core ETL (Bronze -> Silver):
-   **Scalability**: Handles large-scale processing better than Athena CTAS for complex transformations.
-   **Control**: Full Spark capabilities (Broadcasting, Caching).
-   **Integration**: Native support for Iceberg and Glue Data Catalog.

Athena is reserved for ad-hoc querying and the Gold Layer (if simple SQL aggregations are sufficient), but we will state Glue as the primary engine for consistency.
