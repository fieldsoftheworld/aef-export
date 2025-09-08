from contextlib import contextmanager

import ee


@contextmanager
def set_workload_tag(tag_name: str):
    """Context manager for setting and resetting Earth Engine workload tags.

    Args:
        tag_name: The workload tag name to set during the context.

    Usage:
        with workload_tag_context('export-jobs'):
            ee.batch.Export.image.toAsset(composite).start()
    """
    ee.data.setWorkloadTag(tag_name)
    try:
        yield
    finally:
        ee.data.resetWorkloadTag()


def initialize_ee(project_name: str):
    ee.Authenticate()
    ee.Initialize(project=project_name)
