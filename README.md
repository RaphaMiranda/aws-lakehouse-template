# AWS Data Lakehouse Template (CDK + Iceberg)

This repository contains a production-ready template for building a Data Lakehouse on AWS using AWS CDK (Python). It implements a Medallion Architecture (Bronze, Silver, Gold) with Apache Iceberg and AWS Glue.

## Architecture

![Architecture](docs/architecture_placeholder.png)

- **Bronze Layer**: Raw data ingestion via AWS Lambda (from external API) to S3 (JSON).
- **Silver Layer**: Data transformation and cleaning via AWS Glue (Spark) to S3 (Apache Iceberg). Implements Merge/Upsert Logic.
- **Gold Layer**: Business aggregation via AWS Glue to S3 (Iceberg).
- **Orchestration**: AWS Step Functions coordinates the pipeline (Lambda -> Silver -> Gold).
- **Storage**: S3 with dedicated buckets for each layer.
- **Catalog**: AWS Glue Data Catalog.

## Directory Structure

```
├── app.py                  # CDK App entry point
├── cdk.json
├── config/                 # Environment configurations
├── data/                   # Local data for testing (gitignored)
├── docs/                   # Documentation
├── etl/                    # ETL Logic
│   ├── extract/            # Bronze Ingestion (Lambda)
│   ├── transform/          # Silver Transformation (Glue)
│   └── load/               # Gold Aggregation (Glue)
├── infra/                  # Infrastructure Code
│   ├── stacks/
│   ├── constructs/         # Reusable Components
│   └── shared/             # Shared resources
├── pipelines/              # Orchestration Definitions (Step Functions)
├── src/                    # Shared Utilities
└── tests/                  # Unit and Integration Tests
```

## Prerequisites

- AWS CLI installed and configured.
- Node.js (for CDK CLI).
- Python 3.9+ installed.
- Docker (optional, but recommended if building complex Lambda layers).

## Deployment

1.  **Install Dependencies**:
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # Windows: .venv\Scripts\activate
    pip install -r requirements.txt
    ```

2.  **Bootstrap CDK** (First time only):
    ```bash
    cdk bootstrap
    ```

3.  **Synthesize Template**:
    ```bash
    cdk synth
    ```

4.  **Deploy**:
    ```bash
    cdk deploy
    ```

## Development

- **Infrastructure**: Edit `infra/` constructs.
- **ETL Logic**: Edit scripts in `etl/`.
- **Pipelines**: Edit orchestration in `pipelines/`.
- **Tests**: Run `pytest` to execute unit tests.
