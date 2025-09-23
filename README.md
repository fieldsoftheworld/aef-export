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

## AOI Export

First run the `aef-export coverage` command to generate a BQ table which serves as an index for the AEF dataset.  Then export AEF data to the specified GCS bucket (as COG) for a given area of interest.  This will
query the BQ table and submit a earth engine batch task to export each AEF image intersecting the provided geojson.  It currently exports ALL years (PRs are welcome!).

```bash
aef-export aoi <GEOJSON_FILEPATH> <BQ_DATASET_NAME> <BQ_TABLE_NAME> <GCS_BUCKET_NAME>
```

This command autogenerates a sqlite database which may be used to track the status of the export.  Use the `db update-task-status` command to update the state of the mysql database at some cadence, for example:

```bash
watch -n 1800 aef-export db update-task-status
```

And use the `db summarize` command to query the sqlite database for job status:

```bash
aef-export db summarize | jq

[
  {
    "status": "PENDING",
    "count": 557,
    "eecu_seconds": null
  },
  {
    "status": "RUNNING",
    "count": 2,
    "eecu_seconds": null
  },
  {
    "status": "SUCCEEDED",
    "count": 1,
    "eecu_seconds": 3503.357177734375,
    "compute_cost": 0.3892619086371528,
    "avg_runtime_minutes": 14.47057945
  }
]
```




