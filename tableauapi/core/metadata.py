"""Metadata processing and normalization."""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import tableauserverclient as TSC

from tableauapi.utils.exceptions import MetadataError


@dataclass
class WorkbookMetadata:
    """Normalized workbook metadata."""

    id: str
    name: str
    description: Optional[str]
    project_id: str
    project_name: str
    owner_id: str
    owner_name: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    size: Optional[int]
    webpage_url: Optional[str]
    show_tabs: Optional[bool]
    content_url: Optional[str]
    views: List[Dict[str, Any]]
    connections: List[Dict[str, Any]]
    tags: List[str]
    lineage: Optional[Dict[str, Any]] = None


@dataclass
class DatasourceMetadata:
    """Normalized data source metadata."""

    id: str
    name: str
    description: Optional[str]
    project_id: str
    project_name: str
    owner_id: str
    owner_name: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    size: Optional[int]
    webpage_url: Optional[str]
    content_url: Optional[str]
    connections: List[Dict[str, Any]]
    tags: List[str]
    lineage: Optional[Dict[str, Any]] = None


@dataclass
class ProjectMetadata:
    """Normalized project metadata."""

    id: str
    name: str
    description: Optional[str]
    parent_id: Optional[str]
    content_permissions: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    workbook_count: int = 0
    datasource_count: int = 0
    flow_count: int = 0


@dataclass
class FlowMetadata:
    """Normalized flow metadata."""

    id: str
    name: str
    description: Optional[str]
    project_id: str
    project_name: str
    owner_id: str
    owner_name: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    webpage_url: Optional[str]
    tags: List[str]


@dataclass
class ServerMetadata:
    """Server-level metadata."""

    server_url: str
    version: str
    rest_api_version: str
    site_id: str
    site_name: str
    timestamp: datetime
    workbooks: List[WorkbookMetadata]
    datasources: List[DatasourceMetadata]
    projects: List[ProjectMetadata]
    flows: List[FlowMetadata]


class MetadataProcessor:
    """Processes and normalizes Tableau metadata."""

    def __init__(self):
        """Initialize metadata processor."""
        pass

    def process_workbook(
        self,
        workbook: TSC.WorkbookItem,
        views: Optional[List[TSC.ViewItem]] = None,
        connections: Optional[List[TSC.ConnectionItem]] = None,
        lineage: Optional[Dict[str, Any]] = None,
    ) -> WorkbookMetadata:
        """Process and normalize workbook metadata."""
        try:
            # Process views
            view_data = []
            if views:
                for view in views:
                    view_data.append(
                        {
                            "id": view.id,
                            "name": view.name,
                            "content_url": view.content_url,
                            "webpage_url": view.webpage_url,
                            "created_at": view.created_at,
                            "updated_at": view.updated_at,
                        }
                    )

            # Process connections
            connection_data = []
            if connections:
                for conn in connections:
                    connection_data.append(
                        {
                            "id": conn.id,
                            "datasource_id": conn.datasource_id,
                            "datasource_name": conn.datasource_name,
                            "connection_type": conn.connection_type,
                            "server_address": conn.server_address,
                            "server_port": conn.server_port,
                            "username": conn.username,
                            "embed_password": conn.embed_password,
                        }
                    )

            return WorkbookMetadata(
                id=workbook.id,
                name=workbook.name,
                description=workbook.description,
                project_id=workbook.project_id,
                project_name=workbook.project_name,
                owner_id=workbook.owner_id,
                owner_name=workbook.owner_name,
                created_at=workbook.created_at,
                updated_at=workbook.updated_at,
                size=workbook.size,
                webpage_url=workbook.webpage_url,
                show_tabs=workbook.show_tabs,
                content_url=workbook.content_url,
                views=view_data,
                connections=connection_data,
                tags=workbook.tags or [],
                lineage=lineage,
            )
        except Exception as e:
            raise MetadataError(f"Failed to process workbook metadata: {str(e)}")

    def process_datasource(
        self,
        datasource: TSC.DatasourceItem,
        connections: Optional[List[TSC.ConnectionItem]] = None,
        lineage: Optional[Dict[str, Any]] = None,
    ) -> DatasourceMetadata:
        """Process and normalize data source metadata."""
        try:
            # Process connections
            connection_data = []
            if connections:
                for conn in connections:
                    connection_data.append(
                        {
                            "id": conn.id,
                            "datasource_id": conn.datasource_id,
                            "datasource_name": conn.datasource_name,
                            "connection_type": conn.connection_type,
                            "server_address": conn.server_address,
                            "server_port": conn.server_port,
                            "username": conn.username,
                            "embed_password": conn.embed_password,
                        }
                    )

            return DatasourceMetadata(
                id=datasource.id,
                name=datasource.name,
                description=datasource.description,
                project_id=datasource.project_id,
                project_name=datasource.project_name,
                owner_id=datasource.owner_id,
                owner_name=datasource.owner_name,
                created_at=datasource.created_at,
                updated_at=datasource.updated_at,
                size=datasource.size,
                webpage_url=datasource.webpage_url,
                content_url=datasource.content_url,
                connections=connection_data,
                tags=datasource.tags or [],
                lineage=lineage,
            )
        except Exception as e:
            raise MetadataError(f"Failed to process datasource metadata: {str(e)}")

    def process_project(self, project: TSC.ProjectItem) -> ProjectMetadata:
        """Process and normalize project metadata."""
        try:
            return ProjectMetadata(
                id=project.id,
                name=project.name,
                description=project.description,
                parent_id=project.parent_id,
                content_permissions=project.content_permissions,
                created_at=project.created_at,
                updated_at=project.updated_at,
            )
        except Exception as e:
            raise MetadataError(f"Failed to process project metadata: {str(e)}")

    def process_flow(self, flow: TSC.FlowItem) -> FlowMetadata:
        """Process and normalize flow metadata."""
        try:
            return FlowMetadata(
                id=flow.id,
                name=flow.name,
                description=flow.description,
                project_id=flow.project_id,
                project_name=flow.project_name,
                owner_id=flow.owner_id,
                owner_name=flow.owner_name,
                created_at=flow.created_at,
                updated_at=flow.updated_at,
                webpage_url=flow.webpage_url,
                tags=flow.tags or [],
            )
        except Exception as e:
            raise MetadataError(f"Failed to process flow metadata: {str(e)}")

    def create_server_metadata(
        self,
        server_info: TSC.ServerInfoItem,
        workbooks: List[WorkbookMetadata],
        datasources: List[DatasourceMetadata],
        projects: List[ProjectMetadata],
        flows: List[FlowMetadata],
        server_url: str,
        site_id: str,
        site_name: str,
    ) -> ServerMetadata:
        """Create comprehensive server metadata."""
        try:
            return ServerMetadata(
                server_url=server_url,
                version=server_info.product_version,
                rest_api_version=server_info.rest_api_version,
                site_id=site_id,
                site_name=site_name,
                timestamp=datetime.now(),
                workbooks=workbooks,
                datasources=datasources,
                projects=projects,
                flows=flows,
            )
        except Exception as e:
            raise MetadataError(f"Failed to create server metadata: {str(e)}")

    def filter_metadata(
        self, metadata: ServerMetadata, filters: Dict[str, Any]
    ) -> ServerMetadata:
        """Filter metadata based on criteria."""
        try:
            filtered_workbooks = metadata.workbooks
            filtered_datasources = metadata.datasources
            filtered_projects = metadata.projects
            filtered_flows = metadata.flows

            # Apply filters
            if "project_names" in filters:
                project_names = filters["project_names"]
                filtered_workbooks = [
                    wb for wb in filtered_workbooks if wb.project_name in project_names
                ]
                filtered_datasources = [
                    ds
                    for ds in filtered_datasources
                    if ds.project_name in project_names
                ]
                filtered_projects = [
                    p for p in filtered_projects if p.name in project_names
                ]
                filtered_flows = [
                    f for f in filtered_flows if f.project_name in project_names
                ]

            if "owner_names" in filters:
                owner_names = filters["owner_names"]
                filtered_workbooks = [
                    wb for wb in filtered_workbooks if wb.owner_name in owner_names
                ]
                filtered_datasources = [
                    ds for ds in filtered_datasources if ds.owner_name in owner_names
                ]
                filtered_flows = [
                    f for f in filtered_flows if f.owner_name in owner_names
                ]

            if "tags" in filters:
                tags = filters["tags"]
                filtered_workbooks = [
                    wb
                    for wb in filtered_workbooks
                    if any(tag in wb.tags for tag in tags)
                ]
                filtered_datasources = [
                    ds
                    for ds in filtered_datasources
                    if any(tag in ds.tags for tag in tags)
                ]
                filtered_flows = [
                    f for f in filtered_flows if any(tag in f.tags for tag in tags)
                ]

            if "updated_since" in filters:
                updated_since = filters["updated_since"]
                filtered_workbooks = [
                    wb
                    for wb in filtered_workbooks
                    if wb.updated_at and wb.updated_at >= updated_since
                ]
                filtered_datasources = [
                    ds
                    for ds in filtered_datasources
                    if ds.updated_at and ds.updated_at >= updated_since
                ]
                filtered_flows = [
                    f
                    for f in filtered_flows
                    if f.updated_at and f.updated_at >= updated_since
                ]

            return ServerMetadata(
                server_url=metadata.server_url,
                version=metadata.version,
                rest_api_version=metadata.rest_api_version,
                site_id=metadata.site_id,
                site_name=metadata.site_name,
                timestamp=metadata.timestamp,
                workbooks=filtered_workbooks,
                datasources=filtered_datasources,
                projects=filtered_projects,
                flows=filtered_flows,
            )
        except Exception as e:
            raise MetadataError(f"Failed to filter metadata: {str(e)}")

    def metadata_to_dict(self, metadata: ServerMetadata) -> Dict[str, Any]:
        """Convert metadata to dictionary for serialization."""
        try:
            return asdict(metadata)
        except Exception as e:
            raise MetadataError(f"Failed to convert metadata to dict: {str(e)}")

    def metadata_to_json(self, metadata: ServerMetadata, indent: int = 2) -> str:
        """Convert metadata to JSON string."""
        try:
            data = self.metadata_to_dict(metadata)
            return json.dumps(data, indent=indent, default=str)
        except Exception as e:
            raise MetadataError(f"Failed to convert metadata to JSON: {str(e)}")

    def get_metadata_summary(self, metadata: ServerMetadata) -> Dict[str, Any]:
        """Get summary statistics of metadata."""
        try:
            return {
                "server_url": metadata.server_url,
                "site_name": metadata.site_name,
                "timestamp": metadata.timestamp.isoformat(),
                "counts": {
                    "workbooks": len(metadata.workbooks),
                    "datasources": len(metadata.datasources),
                    "projects": len(metadata.projects),
                    "flows": len(metadata.flows),
                },
                "projects": [{"name": p.name, "id": p.id} for p in metadata.projects],
                "recent_updates": {
                    "workbooks": [
                        {
                            "name": wb.name,
                            "updated_at": wb.updated_at.isoformat()
                            if wb.updated_at
                            else None,
                        }
                        for wb in sorted(
                            metadata.workbooks,
                            key=lambda x: x.updated_at or datetime.min,
                            reverse=True,
                        )[:5]
                    ],
                    "datasources": [
                        {
                            "name": ds.name,
                            "updated_at": ds.updated_at.isoformat()
                            if ds.updated_at
                            else None,
                        }
                        for ds in sorted(
                            metadata.datasources,
                            key=lambda x: x.updated_at or datetime.min,
                            reverse=True,
                        )[:5]
                    ],
                },
            }
        except Exception as e:
            raise MetadataError(f"Failed to generate metadata summary: {str(e)}")
