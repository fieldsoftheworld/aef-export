import ee
import uuid

from aef_export.utils import set_workload_tag


def image_to_feature(img: ee.Image) -> ee.Feature:
    """Convert an Earth Engine Image to a Feature with coverage metadata.

    Transforms an ee.Image into an ee.Feature by extracting all image properties
    and converting temporal metadata to human-readable date formats. The geometry
    is transformed to EPSG:4326 coordinate system.

    Args:
        img: Earth Engine Image to convert to a Feature.

    Returns:
        Earth Engine Feature containing the image geometry and processed properties.
        Start and end dates are formatted as YYYY-MM-dd strings.
    """
    keys = img.propertyNames()
    values = keys.map(lambda k: img.get(k))

    properties = ee.Dictionary.fromLists(keys, values)
    properties = properties.set(
        "start_date", ee.Date(img.get("system:time_start")).format("YYYY-MM-dd")
    )
    properties = properties.set(
        "end_date", ee.Date(img.get("system:time_end")).format("YYYY-MM-dd")
    )
    properties = properties.remove(["system:band_names", "system:bands"])

    geom = img.geometry().transform("EPSG:4326", 1)
    return ee.Feature(geom, properties)


def export_image_collection(
    gcp_project_name: str,
    bq_dataset_name: str,
    bq_table_name: str,
    img_collection_name: str,
) -> str:
    """Export Earth Engine ImageCollection coverage data to BigQuery.

    Processes an Earth Engine ImageCollection by converting each image to a feature
    with coverage metadata, then exports the resulting FeatureCollection to BigQuery.
    Uses workload tags for Earth Engine quota management.

    Args:
        gcp_project_name: Google Cloud Project ID for the BigQuery destination.
        bq_dataset_name: BigQuery dataset name where the table will be created.
        bq_table_name: BigQuery table name for the exported data.
        img_collection_name: Earth Engine ImageCollection asset ID to process.

    Returns:
        Earth Engine task ID for the export operation.

    Example:
        >>> task_id = export_image_collection(
        ...     "my-project",
        ...     "aef",
        ...     "embedding_coverage",
        ...     "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"
        ... )
    """
    collection = ee.ImageCollection(img_collection_name)
    fc = collection.map(image_to_feature)

    with set_workload_tag("image-collection-coverage"):
        short_uuid = str(uuid.uuid4())[:8]
        task = ee.batch.Export.table.toBigQuery(
            collection=fc,
            table=f"{gcp_project_name}.{bq_dataset_name}.{bq_table_name}",
            description=f"image-collection-coverage-{short_uuid}",
            overwrite=True,
        )
        task.start()

    return task.id
