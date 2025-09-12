import ee

from aef_export.utils import set_workload_tag
from aef_export.sqlite import init_database, Row, insert_row
from aef_export.coverage import query_coverage


def _quantize_embeddings(image: ee.Image) -> ee.Image:
    """Apply quantization to embedding values for efficient storage.

    Transforms floating-point embedding values to 8-bit signed integers using
    a power-law transformation followed by scaling and clamping as described
    by the AEF paper. This reduces storage requirements while preserving relative
    magnitudes of each vector.

    Args:
        image: Earth Engine Image containing embedding values to quantize.

    Returns:
        Earth Engine Image with quantized embedding values as int8.
    """
    power = 2.0
    scale = 127.5
    min_value = -127
    max_value = 127

    sat = image.abs().pow(ee.Number(1.0).divide(power)).multiply(image.signum())
    snapped = sat.multiply(scale).round()
    image = snapped.clamp(min_value, max_value).int8()
    return image


def export_image(
    image_id: str, gcs_bucket_name: str, gcs_key_prefix: str, quantize: bool = False
) -> str:
    """Export an Earth Engine Image to Google Cloud Storage.

    Exports an Earth Engine image to Google Cloud Storage as a Cloud
    Optimized GeoTIFF. Optionally applies quantization to reduce file size.
    Uses workload tags for Earth Engine quota management.

    Args:
        image_id: Earth Engine image id to export.
        gcs_bucket_name: Google Cloud Storage bucket name for the export.
        gcs_key_prefix: GCS object key prefix for the exported file.
        quantize: Whether to apply quantization to the image values. Defaults to False.

    Returns:
        Earth Engine task ID for the export operation.

    Example:
        >>> task_id = export_image(
        ...     "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL/xs6bvzj41inm2e1cc",
        ...     "my-bucket",
        ...     "my-key-prefix",
        ...     quantize=True
        ... )
    """
    image = ee.Image(image_id)
    if quantize:
        image = _quantize_embeddings(image)

    with set_workload_tag("export-image"):
        short_uuid = image_id.split("/")[-1]
        task = ee.batch.Export.image.toCloudStorage(
            image=image,
            description=f"export-image-{short_uuid}",
            bucket=gcs_bucket_name,
            fileNamePrefix=gcs_key_prefix,
            maxPixels=2e10,
            formatOptions={"cloudOptimized": True},
        )
        task.start()

    return task.id


def export_aoi(
    geojson_geometry: dict,
    bq_dataset_name: str,
    bq_table_name: str,
    gcs_bucket_name: str,
    job_name: str,
    limit: int | None = None,
):
    init_database()

    rows_to_export = query_coverage(
        geojson_geometry, bq_dataset_name, bq_table_name, limit
    )
    for row in rows_to_export:
        # Build the key prefix.
        system_id = row["system_id"]
        year = row["year"]
        utm_zone = row["utm_zone"]
        key_prefix = "/".join([job_name, year, utm_zone, system_id.split("/")[-1]])

        # Start the export.
        # TODO: Protect against 3000+ tasks in the queue.
        task_id = export_image(system_id, gcs_bucket_name, key_prefix, quantize=True)
        print("Submitting task: ", task_id)

        # Insert record of this row into sqlite
        row = Row(
            task_id="task_id",
            job_name=job_name,
            eecu_seconds=None,
            runtime_seconds=None,
            status="queued",
            image_id=system_id,
            year=year,
            s3_path=f"s3://{gcs_bucket_name}/{key_prefix}",
        )
        insert_row(row)
