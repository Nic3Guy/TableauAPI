"""Storage integration for local and S3 storage."""

import csv
import gzip
import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from tableauapi.core.metadata import MetadataProcessor, ServerMetadata
from tableauapi.utils.exceptions import StorageError


class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    def save_metadata(
        self, metadata: ServerMetadata, path: str, format: str = "json"
    ) -> str:
        """Save metadata to storage."""
        pass

    @abstractmethod
    def load_metadata(self, path: str) -> ServerMetadata:
        """Load metadata from storage."""
        pass

    @abstractmethod
    def list_metadata_files(self, prefix: str = "") -> List[str]:
        """List metadata files."""
        pass

    @abstractmethod
    def delete_metadata(self, path: str) -> bool:
        """Delete metadata file."""
        pass


class LocalStorage(StorageBackend):
    """Local filesystem storage backend."""

    def __init__(self, base_path: str = "./tableau_metadata"):
        """Initialize local storage."""
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.processor = MetadataProcessor()

    def save_metadata(
        self, metadata: ServerMetadata, path: str, format: str = "json"
    ) -> str:
        """Save metadata to local filesystem."""
        try:
            file_path = self.base_path / path
            file_path.parent.mkdir(parents=True, exist_ok=True)

            if format.lower() == "json":
                content = self.processor.metadata_to_json(metadata)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)

            elif format.lower() == "json.gz":
                content = self.processor.metadata_to_json(metadata)
                with gzip.open(file_path, "wt", encoding="utf-8") as f:
                    f.write(content)

            elif format.lower() == "csv":
                self._save_as_csv(metadata, file_path)

            else:
                raise StorageError(f"Unsupported format: {format}")

            return str(file_path)

        except Exception as e:
            raise StorageError(f"Failed to save metadata locally: {str(e)}")

    def load_metadata(self, path: str) -> ServerMetadata:
        """Load metadata from local filesystem."""
        try:
            file_path = self.base_path / path

            if not file_path.exists():
                raise StorageError(f"Metadata file not found: {file_path}")

            if path.endswith(".gz"):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    content = f.read()
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

            # Parse JSON and reconstruct metadata objects
            data = json.loads(content)
            return self._dict_to_metadata(data)

        except Exception as e:
            raise StorageError(f"Failed to load metadata locally: {str(e)}")

    def list_metadata_files(self, prefix: str = "") -> List[str]:
        """List metadata files in local storage."""
        try:
            search_path = self.base_path / prefix if prefix else self.base_path
            files = []

            if search_path.is_dir():
                for file_path in search_path.rglob("*.json*"):
                    relative_path = file_path.relative_to(self.base_path)
                    files.append(str(relative_path))

            return sorted(files)

        except Exception as e:
            raise StorageError(f"Failed to list metadata files: {str(e)}")

    def delete_metadata(self, path: str) -> bool:
        """Delete metadata file from local storage."""
        try:
            file_path = self.base_path / path

            if file_path.exists():
                file_path.unlink()
                return True

            return False

        except Exception as e:
            raise StorageError(f"Failed to delete metadata file: {str(e)}")

    def _save_as_csv(self, metadata: ServerMetadata, base_path: Path):
        """Save metadata as CSV files."""
        base_path.mkdir(parents=True, exist_ok=True)

        # Save workbooks
        workbooks_path = base_path / "workbooks.csv"
        with open(workbooks_path, "w", newline="", encoding="utf-8") as f:
            if metadata.workbooks:
                writer = csv.DictWriter(f, fieldnames=self._get_workbook_fields())
                writer.writeheader()
                for wb in metadata.workbooks:
                    row = self._workbook_to_row(wb)
                    writer.writerow(row)

        # Save datasources
        datasources_path = base_path / "datasources.csv"
        with open(datasources_path, "w", newline="", encoding="utf-8") as f:
            if metadata.datasources:
                writer = csv.DictWriter(f, fieldnames=self._get_datasource_fields())
                writer.writeheader()
                for ds in metadata.datasources:
                    row = self._datasource_to_row(ds)
                    writer.writerow(row)

        # Save projects
        projects_path = base_path / "projects.csv"
        with open(projects_path, "w", newline="", encoding="utf-8") as f:
            if metadata.projects:
                writer = csv.DictWriter(f, fieldnames=self._get_project_fields())
                writer.writeheader()
                for p in metadata.projects:
                    row = self._project_to_row(p)
                    writer.writerow(row)

    def _get_workbook_fields(self) -> List[str]:
        """Get CSV field names for workbooks."""
        return [
            "id",
            "name",
            "description",
            "project_id",
            "project_name",
            "owner_id",
            "owner_name",
            "created_at",
            "updated_at",
            "size",
            "webpage_url",
            "show_tabs",
            "content_url",
            "tags",
        ]

    def _get_datasource_fields(self) -> List[str]:
        """Get CSV field names for datasources."""
        return [
            "id",
            "name",
            "description",
            "project_id",
            "project_name",
            "owner_id",
            "owner_name",
            "created_at",
            "updated_at",
            "size",
            "webpage_url",
            "content_url",
            "tags",
        ]

    def _get_project_fields(self) -> List[str]:
        """Get CSV field names for projects."""
        return [
            "id",
            "name",
            "description",
            "parent_id",
            "content_permissions",
            "created_at",
            "updated_at",
            "workbook_count",
            "datasource_count",
            "flow_count",
        ]

    def _workbook_to_row(self, wb) -> Dict[str, Any]:
        """Convert workbook to CSV row."""
        return {
            "id": wb.id,
            "name": wb.name,
            "description": wb.description or "",
            "project_id": wb.project_id,
            "project_name": wb.project_name,
            "owner_id": wb.owner_id,
            "owner_name": wb.owner_name,
            "created_at": wb.created_at.isoformat() if wb.created_at else "",
            "updated_at": wb.updated_at.isoformat() if wb.updated_at else "",
            "size": wb.size or 0,
            "webpage_url": wb.webpage_url or "",
            "show_tabs": wb.show_tabs or False,
            "content_url": wb.content_url or "",
            "tags": ",".join(wb.tags) if wb.tags else "",
        }

    def _datasource_to_row(self, ds) -> Dict[str, Any]:
        """Convert datasource to CSV row."""
        return {
            "id": ds.id,
            "name": ds.name,
            "description": ds.description or "",
            "project_id": ds.project_id,
            "project_name": ds.project_name,
            "owner_id": ds.owner_id,
            "owner_name": ds.owner_name,
            "created_at": ds.created_at.isoformat() if ds.created_at else "",
            "updated_at": ds.updated_at.isoformat() if ds.updated_at else "",
            "size": ds.size or 0,
            "webpage_url": ds.webpage_url or "",
            "content_url": ds.content_url or "",
            "tags": ",".join(ds.tags) if ds.tags else "",
        }

    def _project_to_row(self, p) -> Dict[str, Any]:
        """Convert project to CSV row."""
        return {
            "id": p.id,
            "name": p.name,
            "description": p.description or "",
            "parent_id": p.parent_id or "",
            "content_permissions": p.content_permissions or "",
            "created_at": p.created_at.isoformat() if p.created_at else "",
            "updated_at": p.updated_at.isoformat() if p.updated_at else "",
            "workbook_count": p.workbook_count,
            "datasource_count": p.datasource_count,
            "flow_count": p.flow_count,
        }

    def _dict_to_metadata(self, data: Dict[str, Any]) -> ServerMetadata:
        """Convert dictionary back to ServerMetadata object."""
        # This is a simplified conversion - in a real implementation,
        # you would need to properly reconstruct all nested objects
        from tableauapi.core.metadata import (
            ServerMetadata,
            WorkbookMetadata,
        )

        # Convert string dates back to datetime objects
        def parse_datetime(date_str):
            if date_str:
                return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return None

        # Reconstruct workbooks
        workbooks = []
        for wb_data in data.get("workbooks", []):
            workbooks.append(
                WorkbookMetadata(
                    id=wb_data["id"],
                    name=wb_data["name"],
                    description=wb_data.get("description"),
                    project_id=wb_data["project_id"],
                    project_name=wb_data["project_name"],
                    owner_id=wb_data["owner_id"],
                    owner_name=wb_data["owner_name"],
                    created_at=parse_datetime(wb_data.get("created_at")),
                    updated_at=parse_datetime(wb_data.get("updated_at")),
                    size=wb_data.get("size"),
                    webpage_url=wb_data.get("webpage_url"),
                    show_tabs=wb_data.get("show_tabs"),
                    content_url=wb_data.get("content_url"),
                    views=wb_data.get("views", []),
                    connections=wb_data.get("connections", []),
                    tags=wb_data.get("tags", []),
                    lineage=wb_data.get("lineage"),
                )
            )

        # Similar reconstruction for other types would go here...
        # For brevity, I'll create empty lists for now
        datasources = []
        projects = []
        flows = []

        return ServerMetadata(
            server_url=data["server_url"],
            version=data["version"],
            rest_api_version=data["rest_api_version"],
            site_id=data["site_id"],
            site_name=data["site_name"],
            timestamp=parse_datetime(data["timestamp"]),
            workbooks=workbooks,
            datasources=datasources,
            projects=projects,
            flows=flows,
        )


class S3Storage(StorageBackend):
    """S3 storage backend."""

    def __init__(
        self,
        bucket_name: str,
        prefix: str = "tableau_metadata/",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
    ):
        """Initialize S3 storage."""
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.processor = MetadataProcessor()

        try:
            # Initialize S3 client
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )
            self.s3_client = session.client("s3")

            # Test connection
            self.s3_client.head_bucket(Bucket=bucket_name)

        except NoCredentialsError:
            raise StorageError("AWS credentials not found")
        except ClientError as e:
            raise StorageError(f"Failed to connect to S3: {str(e)}")

    def save_metadata(
        self, metadata: ServerMetadata, path: str, format: str = "json"
    ) -> str:
        """Save metadata to S3."""
        try:
            s3_key = f"{self.prefix}{path}"

            if format.lower() == "json":
                content = self.processor.metadata_to_json(metadata)
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=content,
                    ContentType="application/json",
                )

            elif format.lower() == "json.gz":
                content = self.processor.metadata_to_json(metadata)
                compressed_content = gzip.compress(content.encode("utf-8"))
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=compressed_content,
                    ContentType="application/json",
                    ContentEncoding="gzip",
                )

            else:
                raise StorageError(f"Unsupported format: {format}")

            return f"s3://{self.bucket_name}/{s3_key}"

        except Exception as e:
            raise StorageError(f"Failed to save metadata to S3: {str(e)}")

    def load_metadata(self, path: str) -> ServerMetadata:
        """Load metadata from S3."""
        try:
            s3_key = f"{self.prefix}{path}"

            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)

            content = response["Body"].read()

            # Handle compressed content
            if response.get("ContentEncoding") == "gzip":
                content = gzip.decompress(content)

            content_str = content.decode("utf-8")
            data = json.loads(content_str)

            return self._dict_to_metadata(data)

        except Exception as e:
            raise StorageError(f"Failed to load metadata from S3: {str(e)}")

    def list_metadata_files(self, prefix: str = "") -> List[str]:
        """List metadata files in S3."""
        try:
            search_prefix = f"{self.prefix}{prefix}"

            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=search_prefix
            )

            files = []
            for obj in response.get("Contents", []):
                # Remove the base prefix to get relative path
                relative_path = obj["Key"][len(self.prefix) :]
                files.append(relative_path)

            return sorted(files)

        except Exception as e:
            raise StorageError(f"Failed to list metadata files in S3: {str(e)}")

    def delete_metadata(self, path: str) -> bool:
        """Delete metadata file from S3."""
        try:
            s3_key = f"{self.prefix}{path}"

            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)

            return True

        except Exception as e:
            raise StorageError(f"Failed to delete metadata file from S3: {str(e)}")

    def _dict_to_metadata(self, data: Dict[str, Any]) -> ServerMetadata:
        """Convert dictionary back to ServerMetadata object."""
        # Use the same implementation as LocalStorage
        local_storage = LocalStorage()
        return local_storage._dict_to_metadata(data)


class StorageManager:
    """Manages multiple storage backends."""

    def __init__(
        self,
        local_storage: Optional[LocalStorage] = None,
        s3_storage: Optional[S3Storage] = None,
    ):
        """Initialize storage manager."""
        self.local_storage = local_storage or LocalStorage()
        self.s3_storage = s3_storage

    def save_metadata(
        self,
        metadata: ServerMetadata,
        filename: str,
        backends: List[str] = ["local"],
        format: str = "json",
    ) -> Dict[str, str]:
        """Save metadata to multiple backends."""
        results = {}

        for backend in backends:
            if backend == "local":
                path = self.local_storage.save_metadata(metadata, filename, format)
                results["local"] = path

            elif backend == "s3" and self.s3_storage:
                path = self.s3_storage.save_metadata(metadata, filename, format)
                results["s3"] = path

            else:
                raise StorageError(f"Unsupported backend: {backend}")

        return results

    def load_metadata(self, filename: str, backend: str = "local") -> ServerMetadata:
        """Load metadata from specified backend."""
        if backend == "local":
            return self.local_storage.load_metadata(filename)

        elif backend == "s3" and self.s3_storage:
            return self.s3_storage.load_metadata(filename)

        else:
            raise StorageError(f"Unsupported backend: {backend}")

    def generate_filename(
        self, metadata: ServerMetadata, include_timestamp: bool = True
    ) -> str:
        """Generate filename for metadata."""
        site_name = metadata.site_name or "default"
        site_name = site_name.replace(" ", "_").replace("/", "_")

        if include_timestamp:
            timestamp = metadata.timestamp.strftime("%Y%m%d_%H%M%S")
            return f"{site_name}_{timestamp}.json"
        else:
            return f"{site_name}.json"
