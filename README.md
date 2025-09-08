# AEF Export

Export AEF embeddings from Earth Engine to BigQuery / GCS.

## Installation

```bash
uv sync
```

Active the uv environment with `source .venv/bin/activate`, or prefix commands with `uv run`:

```shell
aef-export --help
```

## Setup

Configure your Google Cloud project and authenticate with Earth Engine:

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
earthengine authenticate
```

## Usage

Export coverage data to a BigQuery table:

```bash
aef-export coverage BQ_DATASET_NAME BQ_TABLE_NAME
```
