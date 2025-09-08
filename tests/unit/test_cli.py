from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from aef_export.cli import coverage, image


@patch("aef_export.cli.export_image_collection")
@patch("aef_export.cli.initialize_ee")
@patch("aef_export.cli.get_settings")
def test_coverage_command_success(
    mock_get_settings, mock_initialize_ee, mock_export_image_collection
):
    # Setup mocks
    mock_settings = MagicMock()
    mock_settings.google_cloud_project = "test-project"
    mock_settings.image_collection_name = "TEST/COLLECTION"
    mock_get_settings.return_value = mock_settings
    mock_export_image_collection.return_value = "task_123"

    runner = CliRunner()
    result = runner.invoke(coverage, ["test_dataset", "test_table"])

    # Verify the calls
    mock_get_settings.assert_called_once()
    mock_initialize_ee.assert_called_once_with("test-project")
    mock_export_image_collection.assert_called_once_with(
        "test-project",
        "test_dataset",
        "test_table",
        img_collection_name="TEST/COLLECTION",
    )

    # Verify the output and exit code
    assert result.exit_code == 0
    assert "Task id: task_123" in result.output


def test_coverage_command_missing_arguments():
    runner = CliRunner()

    # Test with no arguments
    result = runner.invoke(coverage, [])
    assert result.exit_code == 2
    assert "Missing argument" in result.output

    # Test with only one argument
    result = runner.invoke(coverage, ["test_dataset"])
    assert result.exit_code == 2
    assert "Missing argument" in result.output


@patch("aef_export.cli.export_image")
@patch("aef_export.cli.initialize_ee")
@patch("aef_export.cli.get_settings")
def test_image_command_success(
    mock_get_settings, mock_initialize_ee, mock_export_image
):
    # Setup mocks
    mock_settings = MagicMock()
    mock_settings.google_cloud_project = "test-project"
    mock_get_settings.return_value = mock_settings
    mock_export_image.return_value = "image_task_123"

    runner = CliRunner()
    result = runner.invoke(
        image, ["PROJECTS/test/assets/test_image", "test-bucket", "test/prefix"]
    )

    # Verify the calls
    mock_get_settings.assert_called_once()
    mock_initialize_ee.assert_called_once_with("test-project")
    mock_export_image.assert_called_once_with(
        "PROJECTS/test/assets/test_image", "test-bucket", "test/prefix/", False
    )

    # Verify the output and exit code
    assert result.exit_code == 0
    assert "Task id: image_task_123" in result.output


@patch("aef_export.cli.export_image")
@patch("aef_export.cli.initialize_ee")
@patch("aef_export.cli.get_settings")
def test_image_command_with_quantize_flag(
    mock_get_settings, mock_initialize_ee, mock_export_image
):
    # Setup mocks
    mock_settings = MagicMock()
    mock_settings.google_cloud_project = "test-project"
    mock_get_settings.return_value = mock_settings
    mock_export_image.return_value = "quantized_task_456"

    runner = CliRunner()
    result = runner.invoke(
        image,
        [
            "PROJECTS/test/assets/embedding_image",
            "embedding-bucket",
            "quantized/prefix",
            "--quantize",
        ],
    )

    # Verify the calls
    mock_get_settings.assert_called_once()
    mock_initialize_ee.assert_called_once_with("test-project")
    mock_export_image.assert_called_once_with(
        "PROJECTS/test/assets/embedding_image",
        "embedding-bucket",
        "quantized/prefix/",
        True,
    )

    # Verify the output and exit code
    assert result.exit_code == 0
    assert "Task id: quantized_task_456" in result.output
