from __future__ import annotations

import hashlib
import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def guess_mime(path: Path, *, default: str = "application/octet-stream") -> str:
    mt, _enc = mimetypes.guess_type(str(path))
    return mt or default


@dataclass(frozen=True)
class R2Config:
    bucket: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    region: str = "auto"
    key_prefix: str = ""

    @staticmethod
    def from_env() -> "R2Config":
        bucket = (os.environ.get("R2_BUCKET") or "").strip()
        endpoint_url = (os.environ.get("R2_ENDPOINT_URL") or "").strip()
        access_key_id = (os.environ.get("R2_ACCESS_KEY_ID") or "").strip()
        secret_access_key = os.environ.get("R2_SECRET_ACCESS_KEY") or ""
        region = (os.environ.get("R2_REGION") or "auto").strip() or "auto"
        key_prefix = (os.environ.get("R2_KEY_PREFIX") or "").strip()
        if not bucket or not endpoint_url or not access_key_id or not secret_access_key:
            raise RuntimeError(
                "Missing R2 config. Set R2_BUCKET, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY."
            )
        return R2Config(
            bucket=bucket,
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            region=region,
            key_prefix=key_prefix,
        )


class R2Uploader:
    def __init__(self, cfg: R2Config) -> None:
        self.cfg = cfg
        self._client = None

    def connect(self) -> None:
        if self._client is not None:
            return
        import boto3  # type: ignore
        from botocore.config import Config as BotoConfig  # type: ignore

        # R2 is S3-compatible; path-style keeps things predictable.
        boto_cfg = BotoConfig(
            region_name=self.cfg.region,
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "path"},
        )
        self._client = boto3.client(
            "s3",
            endpoint_url=self.cfg.endpoint_url,
            aws_access_key_id=self.cfg.access_key_id,
            aws_secret_access_key=self.cfg.secret_access_key,
            config=boto_cfg,
        )

    @property
    def client(self):
        if self._client is None:
            self.connect()
        return self._client

    def _full_key(self, key: str) -> str:
        key_norm = (key or "").lstrip("/")
        pfx = (self.cfg.key_prefix or "").strip().strip("/")
        if not pfx:
            return key_norm
        if key_norm.startswith(f"{pfx}/"):
            return key_norm
        return f"{pfx}/{key_norm}"

    def put_bytes(self, *, key: str, data: bytes, content_type: str | None = None) -> Tuple[str, str]:
        self.connect()
        full_key = self._full_key(key)
        kwargs = {"Bucket": self.cfg.bucket, "Key": full_key, "Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        resp = self.client.put_object(**kwargs)
        etag = (resp.get("ETag") or "").strip('"')
        return full_key, etag

    def put_file(self, *, key: str, path: Path, content_type: str | None = None) -> Tuple[str, str]:
        self.connect()
        full_key = self._full_key(key)
        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        with path.open("rb") as f:
            resp = self.client.put_object(Bucket=self.cfg.bucket, Key=full_key, Body=f, **extra)
        return full_key, (resp.get("ETag") or "").strip('"')
