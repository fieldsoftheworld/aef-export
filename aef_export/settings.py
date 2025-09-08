import functools

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    google_cloud_project: str
    image_collection_name: str = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"


@functools.lru_cache()
def get_settings():
    return Settings()
