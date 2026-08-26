"""Initialize one exact S3-compatible bucket without production auto-creation."""

from __future__ import annotations

import sys
from typing import Literal, Protocol, cast
from urllib.parse import urlsplit

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus.settings import Environment

InitResult = Literal["created", "existing"]


class ObjectStoreInitSettings(BaseSettings):
    """Least-privilege settings for the one-shot bucket initializer."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    environment: Environment = Field(validation_alias="NEXUS_ENV")
    endpoint: str = Field(min_length=1, validation_alias="OBJECT_STORE_ENDPOINT")
    region: str = Field(min_length=1, validation_alias="OBJECT_STORE_REGION")
    bucket: str = Field(
        min_length=3,
        max_length=63,
        pattern=r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])$",
        validation_alias="OBJECT_STORE_BUCKET",
    )
    access_key_id: SecretStr = Field(validation_alias="OBJECT_STORE_ACCESS_KEY_ID")
    secret_access_key: SecretStr = Field(validation_alias="OBJECT_STORE_SECRET_ACCESS_KEY")

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        stripped = value.strip()
        parsed = urlsplit(stripped)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("object-store endpoint must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("object-store endpoint must not include credentials or URL extras")
        return stripped.rstrip("/")

    @field_validator("region")
    @classmethod
    def validate_region(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("object-store region must be non-empty")
        return stripped

    @field_validator("access_key_id", "secret_access_key")
    @classmethod
    def validate_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("object-store credential must be non-empty")
        return value

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return []


class ObjectStoreInitClient(Protocol):
    def head_bucket(self, **kwargs: object) -> object: ...

    def create_bucket(self, **kwargs: object) -> object: ...


def ensure_bucket(
    settings: ObjectStoreInitSettings,
    *,
    client: ObjectStoreInitClient | None = None,
) -> InitResult:
    """HEAD one exact bucket; create only outside production, then verify."""
    s3 = client or _client(settings)
    try:
        s3.head_bucket(Bucket=settings.bucket)
        return "existing"
    except ClientError as error:
        if not _is_missing(error):
            raise
    if settings.environment is Environment.PRODUCTION:
        raise RuntimeError("production object-store bucket is missing")

    create: dict[str, object] = {"Bucket": settings.bucket}
    if settings.region != "us-east-1":
        create["CreateBucketConfiguration"] = {
            "LocationConstraint": settings.region,
        }
    try:
        s3.create_bucket(**create)
    except ClientError as error:
        if not _is_replayable_create(error):
            raise
        s3.head_bucket(Bucket=settings.bucket)
        return "existing"
    s3.head_bucket(Bucket=settings.bucket)
    return "created"


def main() -> int:
    """Console entrypoint with non-sensitive, stable outcome text."""
    try:
        # BaseSettings supplies required values from the environment at runtime.
        result = ensure_bucket(ObjectStoreInitSettings())  # type: ignore[call-arg]
    except Exception:
        print("object-store initialization failed", file=sys.stderr)
        return 1
    print(f"object-store initialization {result}")
    return 0


def _client(settings: ObjectStoreInitSettings) -> ObjectStoreInitClient:
    return cast(
        ObjectStoreInitClient,
        boto3.client(
            "s3",
            endpoint_url=settings.endpoint,
            region_name=settings.region,
            aws_access_key_id=settings.access_key_id.get_secret_value(),
            aws_secret_access_key=settings.secret_access_key.get_secret_value(),
            config=Config(
                connect_timeout=2,
                read_timeout=5,
                retries={"max_attempts": 1},
                s3={"addressing_style": "path"},
            ),
        ),
    )


def _is_missing(error: ClientError) -> bool:
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    document = error.response.get("Error", {})
    code = document.get("Code") if isinstance(document, dict) else None
    return status == 404 or code in {"404", "NoSuchBucket", "NotFound"}


def _is_replayable_create(error: ClientError) -> bool:
    document = error.response.get("Error", {})
    code = document.get("Code") if isinstance(document, dict) else None
    return code in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}
