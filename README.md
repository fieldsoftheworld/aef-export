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
aef-export coverage <BQ_DATASET_NAME> <BQ_TABLE_NAME>
```

Export a single image to GCS, an example image ID is `GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL/xs6bvzj41inm2e1cc`.  It is recommended to export embeddings in their quantized form (int8) to reduce storage costs.

```bash
aef-export image <IMAGE_ID> <GCS_BUCKET_NAME> <GCS_KEY_PREFIX> --quantize
```
