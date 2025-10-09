import ee
import io
from functools import lru_cache
import concurrent.futures

import affine
import numpy as np
from pyproj import Transformer
from shapely.geometry import Polygon
import utm
import geopandas as gpd
from google.cloud import storage

from aef_export.embeddings import _quantize_embeddings


@lru_cache()
def get_gcs_client():
    return storage.Client()


@lru_cache()
def _get_transformer(out_epsg: str, in_epsg: str = "EPSG:4326") -> Transformer:
    """Get and cache a pyproj transformer."""
    return Transformer.from_crs(in_epsg, out_epsg, always_xy=True)


def _utm_zone_from_latlon(lat: float, lon: float) -> str:
    _, _, zone_number, _ = utm.from_latlon(lat, lon)

    if lat < 0:
        return f"EPSG:327{zone_number}"
    else:
        return f"EPSG:326{zone_number}"


def _fetch_array(
    year: int,
    geom: Polygon,
    spatial_res_meters: int = 10,
    width: int | None = None,
    height: int | None = None,
) -> np.ndarray:
    # Find the right EE image
    images = (
        ee.ImageCollection("GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL")
        .filterBounds(ee.Geometry(geom.__geo_interface__))
        .filterDate(ee.Date(f"{year}-01-01"), ee.Date(f"{year + 1}-01-01"))
    )
    image = images.first()
    image = _quantize_embeddings(image)

    # Build an export request
    centroid = geom.centroid
    utm_zone = _utm_zone_from_latlon(centroid.y, centroid.x)
    transformer = _get_transformer(utm_zone)
    xmin, ymin, xmax, ymax = transformer.transform_bounds(*geom.bounds)

    if not width:
        width = int((xmax - xmin) / spatial_res_meters)

    if not height:
        height = int((ymax - ymin) / spatial_res_meters)

    transform = affine.Affine.translation(xmin, ymax) * affine.Affine.scale(
        (xmax - xmin) / width, (ymin - ymax) / height
    )

    request = {
        "expression": image,
        "fileFormat": "NUMPY_NDARRAY",
        "grid": {
            "dimensions": {"width": width, "height": height},
            "affineTransform": {
                "scaleX": transform.a,
                "shearX": transform.b,
                "translateX": transform.c,
                "shearY": transform.d,
                "scaleY": transform.e,
                "translateY": transform.f,
            },
            "crsCode": utm_zone,
        },
    }

    # Send the export request
    arr = ee.data.computePixels(request)
    restructured_arr = arr.view((arr.dtype[0], len(arr.dtype.names)))
    return restructured_arr


def _upload_numpy_array_to_gcs(
    bucket_name: str, destination_blob_name: str, numpy_array: np.ndarray
) -> str:
    """Uploads a NumPy array to a GCS bucket.

    Args:
        bucket_name (str): The name of your GCS bucket.
        destination_blob_name (str): The name of the blob in the bucket (e.g., 'my_array.npy').
        numpy_array (np.ndarray): The NumPy array to upload.
    """
    bucket = get_gcs_client().bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Serialize the NumPy array to bytes
    buffer = io.BytesIO()
    np.save(buffer, numpy_array)
    buffer.seek(0)  # Rewind the buffer to the beginning

    # Upload the bytes to GCS
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    dest_path = f"gs://{bucket_name}/{destination_blob_name}"
    print(f"Uploaded to {dest_path}")

    return dest_path


def _fetch_and_upload_embeddings(
    geom: Polygon,
    country: str,
    aoi_id: str,
    year: int,
    bucket_name: str,
    width: int | None = None,
    height: int | None = None,
) -> str:
    embeddings = _fetch_array(year, geom, width=width, height=height)
    key = f"chips/{country}/{year}/{aoi_id}.npy"
    return _upload_numpy_array_to_gcs(bucket_name, key, embeddings)


def export_labels_for_year(
    gdf: gpd.GeoDataFrame,
    year: int,
    bucket_name: str,
    max_workers: int | None = None,
    width: int | None = None,
    height: int | None = None,
) -> gpd.GeoDataFrame:
    with concurrent.futures.ThreadPoolExecutor(max_workers) as exec:
        tasks = {}
        for row in gdf.itertuples():
            future = exec.submit(
                _fetch_and_upload_embeddings,
                row.geometry,
                row.country,
                row.aoi_id,
                year,
                bucket_name,
                width,
                height,
            )
            tasks[future] = row.Index

        for future in concurrent.futures.as_completed(tasks):
            try:
                gcs_path = future.result()
            except Exception as exc:
                print(exc)
                continue

            gdf.loc[tasks[future], "gcs_path"] = gcs_path

    return gdf
