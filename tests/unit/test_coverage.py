from unittest.mock import MagicMock, patch, call

from aef_export.coverage import image_to_feature, export_image_collection


@patch("aef_export.coverage.ee")
def test_image_to_feature_transforms_image_properties(mock_ee):
    # Mock the Earth Engine objects
    mock_img = MagicMock()
    mock_keys = MagicMock()
    mock_values = MagicMock()
    mock_properties = MagicMock()
    mock_geom = MagicMock()
    mock_feature = MagicMock()

    # Configure the mock chain
    mock_img.propertyNames.return_value = mock_keys
    mock_keys.map.return_value = mock_values
    mock_ee.Dictionary.fromLists.return_value = mock_properties
    mock_img.get.side_effect = ["2020-01-01T00:00:00", "2020-01-02T00:00:00"]
    mock_ee.Date.return_value.format.side_effect = ["2020-01-01", "2020-01-02"]
    mock_properties.set.return_value = mock_properties
    mock_properties.remove.return_value = mock_properties
    mock_img.geometry.return_value.transform.return_value = mock_geom
    mock_ee.Feature.return_value = mock_feature

    # Call the function
    result = image_to_feature(mock_img)

    # Verify the calls
    mock_img.propertyNames.assert_called_once()
    mock_keys.map.assert_called_once()
    mock_ee.Dictionary.fromLists.assert_called_once_with(mock_keys, mock_values)
    mock_properties.set.assert_has_calls(
        [call("start_date", "2020-01-01"), call("end_date", "2020-01-02")]
    )
    mock_properties.remove.assert_called_once_with(
        ["system:band_names", "system:bands"]
    )
    mock_img.geometry.assert_called_once()
    mock_img.geometry().transform.assert_called_once_with("EPSG:4326", 1)
    mock_ee.Feature.assert_called_once_with(mock_geom, mock_properties)

    assert result == mock_feature


@patch("aef_export.coverage.uuid.uuid4")
@patch("aef_export.coverage.set_workload_tag")
@patch("aef_export.coverage.ee")
def test_export_image_collection_creates_export_task(
    mock_ee, mock_workload_tag, mock_uuid
):
    # Setup mocks
    mock_collection = MagicMock()
    mock_fc = MagicMock()
    mock_task = MagicMock()
    mock_task.id = "test_task_id"

    mock_ee.ImageCollection.return_value = mock_collection
    mock_collection.map.return_value = mock_fc
    mock_ee.batch.Export.table.toBigQuery.return_value = mock_task
    mock_uuid.return_value = MagicMock()
    mock_uuid.return_value.__str__ = MagicMock(
        return_value="abcd1234-5678-90ef-ghij-klmnopqrstuv"
    )

    # Call the function
    result = export_image_collection(
        gcp_project_name="test-project",
        bq_dataset_name="test_dataset",
        bq_table_name="test_table",
        img_collection_name="TEST/COLLECTION",
    )

    # Verify the calls
    mock_ee.ImageCollection.assert_called_once_with("TEST/COLLECTION")
    mock_collection.map.assert_called_once()
    mock_workload_tag.assert_called_once_with("image-collection-coverage")
    mock_ee.batch.Export.table.toBigQuery.assert_called_once_with(
        collection=mock_fc,
        table="test-project.test_dataset.test_table",
        description="image-collection-coverage-abcd1234",
        overwrite=True,
    )
    mock_task.start.assert_called_once()

    assert result == "test_task_id"


@patch("aef_export.coverage.uuid.uuid4")
@patch("aef_export.coverage.set_workload_tag")
@patch("aef_export.coverage.ee")
def test_export_image_collection_uses_workload_tag_context(
    mock_ee, mock_workload_tag, mock_uuid
):
    # Setup mocks
    mock_collection = MagicMock()
    mock_fc = MagicMock()
    mock_task = MagicMock()
    mock_task.id = "test_task_id"

    mock_ee.ImageCollection.return_value = mock_collection
    mock_collection.map.return_value = mock_fc
    mock_ee.batch.Export.table.toBigQuery.return_value = mock_task
    mock_uuid.return_value = MagicMock()
    mock_uuid.return_value.__str__ = MagicMock(
        return_value="12345678-90ab-cdef-1234-567890abcdef"
    )

    # Call the function
    export_image_collection(
        gcp_project_name="test-project",
        bq_dataset_name="test_dataset",
        bq_table_name="test_table",
        img_collection_name="TEST/COLLECTION",
    )

    # Verify workload tag context manager is used
    mock_workload_tag.assert_called_once_with("image-collection-coverage")
    mock_workload_tag.return_value.__enter__.assert_called_once()
    mock_workload_tag.return_value.__exit__.assert_called_once()
