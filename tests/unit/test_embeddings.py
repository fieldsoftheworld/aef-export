from unittest.mock import MagicMock, patch

from aef_export.embeddings import _quantize_embeddings, export_image


@patch("aef_export.embeddings.ee")
def test_quantize_embeddings_applies_correct_transformations(mock_ee):
    # Mock the Earth Engine objects
    mock_image = MagicMock()
    mock_abs = MagicMock()
    mock_pow = MagicMock()
    mock_sat = MagicMock()
    mock_signum = MagicMock()
    mock_scaled = MagicMock()
    mock_snapped = MagicMock()
    mock_clamped = MagicMock()
    mock_result = MagicMock()

    # Configure the mock chain for: image.abs().pow(power).multiply(signum)
    mock_image.abs.return_value = mock_abs
    mock_abs.pow.return_value = mock_pow
    mock_pow.multiply.return_value = mock_sat
    mock_image.signum.return_value = mock_signum

    # Configure the mock chain for: sat.multiply(scale).round()
    mock_sat.multiply.return_value = mock_scaled
    mock_scaled.round.return_value = mock_snapped

    # Configure the mock chain for: snapped.clamp(min, max).int8()
    mock_snapped.clamp.return_value = mock_clamped
    mock_clamped.int8.return_value = mock_result

    # Configure ee.Number mock
    mock_number = MagicMock()
    mock_number.divide.return_value = 0.5
    mock_ee.Number.return_value = mock_number

    # Call the function
    result = _quantize_embeddings(mock_image)

    # Verify the transformations
    mock_image.abs.assert_called_once()
    mock_ee.Number.assert_called_once_with(1.0)
    mock_number.divide.assert_called_once_with(2.0)
    mock_abs.pow.assert_called_once_with(0.5)
    mock_image.signum.assert_called_once()
    mock_pow.multiply.assert_called_once_with(mock_signum)
    mock_sat.multiply.assert_called_once_with(127.5)
    mock_scaled.round.assert_called_once()
    mock_snapped.clamp.assert_called_once_with(-127, 127)
    mock_clamped.int8.assert_called_once()

    assert result == mock_result


@patch("aef_export.embeddings.set_workload_tag")
@patch("aef_export.embeddings.ee")
@patch("aef_export.embeddings._quantize_embeddings")
def test_export_image_without_quantization(mock_quantize, mock_ee, mock_workload_tag):
    # Setup mocks
    mock_image = MagicMock()
    mock_task = MagicMock()
    mock_task.id = "test_task_id"

    mock_ee.Image.return_value = mock_image
    mock_ee.batch.Export.image.toCloudStorage.return_value = mock_task

    # Call the function without quantization
    export_image(
        image_id="PROJECTS/test/assets/test_image_12345",
        gcs_bucket_name="test-bucket",
        gcs_key_prefix="test/prefix",
    )

    # Verify _quantize_embeddings is not called
    mock_quantize.assert_not_called()


@patch("aef_export.embeddings._quantize_embeddings")
@patch("aef_export.embeddings.set_workload_tag")
@patch("aef_export.embeddings.ee")
def test_export_image_with_quantization(mock_ee, mock_workload_tag, mock_quantize):
    # Setup mocks
    mock_original_image = MagicMock()
    mock_quantized_image = MagicMock()
    mock_task = MagicMock()
    mock_task.id = "quantized_task_id"

    mock_ee.Image.return_value = mock_original_image
    mock_quantize.return_value = mock_quantized_image
    mock_ee.batch.Export.image.toCloudStorage.return_value = mock_task

    # Call the function with quantization
    result = export_image(
        image_id="PROJECTS/test/assets/embeddings_67890",
        gcs_bucket_name="embeddings-bucket",
        gcs_key_prefix="quantized/embeddings",
        quantize=True,
    )

    # Verify the calls
    mock_ee.Image.assert_called_once_with("PROJECTS/test/assets/embeddings_67890")
    mock_quantize.assert_called_once_with(mock_original_image)
    mock_workload_tag.assert_called_once_with("export-image")
    mock_ee.batch.Export.image.toCloudStorage.assert_called_once_with(
        image=mock_quantized_image,
        description="export-image-embeddings_67890",
        bucket="embeddings-bucket",
        fileNamePrefix="quantized/embeddings",
        maxPixels=2e10,
        formatOptions={"cloudOptimized": True},
    )
    mock_task.start.assert_called_once()

    assert result == "quantized_task_id"


@patch("aef_export.embeddings.set_workload_tag")
@patch("aef_export.embeddings.ee")
def test_export_image_uses_workload_tag_context(mock_ee, mock_workload_tag):
    # Setup mocks
    mock_image = MagicMock()
    mock_task = MagicMock()
    mock_task.id = "context_task_id"

    mock_ee.Image.return_value = mock_image
    mock_ee.batch.Export.image.toCloudStorage.return_value = mock_task

    # Call the function
    export_image(
        image_id="PROJECTS/test/assets/context_test",
        gcs_bucket_name="context-bucket",
        gcs_key_prefix="context/test",
    )

    # Verify workload tag context manager is used
    mock_workload_tag.assert_called_once_with("export-image")
    mock_workload_tag.return_value.__enter__.assert_called_once()
    mock_workload_tag.return_value.__exit__.assert_called_once()
