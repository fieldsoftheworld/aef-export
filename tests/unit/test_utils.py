from unittest.mock import patch

from aef_export.utils import set_workload_tag, initialize_ee


@patch("aef_export.utils.ee")
def test_set_workload_tag_sets_and_resets_tag(mock_ee):
    # Test the context manager sets and resets workload tag
    with set_workload_tag("test-tag"):
        mock_ee.data.setWorkloadTag.assert_called_once_with("test-tag")

    mock_ee.data.resetWorkloadTag.assert_called_once()


@patch("aef_export.utils.ee")
def test_set_workload_tag_resets_on_exception(mock_ee):
    # Test that workload tag is reset even when exception occurs
    try:
        with set_workload_tag("test-tag"):
            raise ValueError("test exception")
    except ValueError:
        pass

    mock_ee.data.setWorkloadTag.assert_called_once_with("test-tag")
    mock_ee.data.resetWorkloadTag.assert_called_once()


@patch("aef_export.utils.ee")
def test_initialize_ee_calls_authenticate_and_initialize(mock_ee):
    # Test that initialize_ee calls both Authenticate and Initialize
    initialize_ee("test-project")

    mock_ee.Authenticate.assert_called_once()
    mock_ee.Initialize.assert_called_once_with(project="test-project")
