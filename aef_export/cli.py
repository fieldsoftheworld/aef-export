import click

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
