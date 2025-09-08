import click

from aef_export.embeddings import export_image
from aef_export.coverage import export_image_collection
from aef_export.settings import get_settings
from aef_export.utils import initialize_ee


@click.group()
def app():
    """Export AEF embeddings from earth engine."""
    pass


@app.command()
@click.argument("bq_dataset_name")
@click.argument("bq_table_name")
def coverage(bq_dataset_name: str, bq_table_name: str):
    """Export Earth Engine image collection coverage data to BigQuery.

    Processes the configured Earth Engine image collection by converting each image
    to a feature with coverage metadata, then exports to the specified BigQuery table.
    """
    settings = get_settings()

    initialize_ee(settings.google_cloud_project)
    task_id = export_image_collection(
        settings.google_cloud_project,
        bq_dataset_name,
        bq_table_name,
        img_collection_name=settings.image_collection_name,
    )
    click.echo(f"Task id: {task_id}")


@app.command()
@click.argument("image_id")
@click.argument("gcs_bucket_name")
@click.argument("gcs_key_prefix")
@click.option("--quantize", is_flag=True, default=False)
def image(
    image_id: str, gcs_bucket_name: str, gcs_key_prefix: str, quantize: bool = False
):
    """Export a single Earth Engine image to GCS.

    Exports the specified Earth Engine Image asset to Google Cloud Storage as a
    Cloud Optimized GeoTIFF. Optionally applies quantization to reduce file size.
    """
    settings = get_settings()

    if not gcs_key_prefix.endswith("/"):
        gcs_key_prefix += "/"

    initialize_ee(settings.google_cloud_project)
    task_id = export_image(image_id, gcs_bucket_name, gcs_key_prefix, quantize)
    click.echo(f"Task id: {task_id}")
