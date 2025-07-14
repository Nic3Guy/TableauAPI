"""Tableau API client wrapper."""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import tableauserverclient as TSC

from tableauapi.core.auth import AuthConfig, TableauAuthenticator
from tableauapi.utils.exceptions import ConnectionError, TableauAPIError

logger = logging.getLogger(__name__)


class TableauClient:
    """Wrapper for Tableau Server Client with enhanced functionality."""

    def __init__(self, auth_config: AuthConfig):
        """Initialize the Tableau client."""
        self.auth_config = auth_config
        self.authenticator = TableauAuthenticator(auth_config)
        self.server = None
        self._retry_count = 3
        self._retry_delay = 1.0

    @contextmanager
    def connection(self):
        """Context manager for Tableau Server connection."""
        try:
            self.server = self.authenticator.authenticate()
            logger.info(f"Connected to Tableau Server: {self.auth_config.server_url}")
            yield self
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to Tableau Server: {str(e)}")
        finally:
            if self.server:
                self.authenticator.sign_out()
                logger.info("Disconnected from Tableau Server")

    def _retry_operation(self, operation, *args, **kwargs):
        """Retry operation with exponential backoff."""
        for attempt in range(self._retry_count):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                if attempt == self._retry_count - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self._retry_delay * (2**attempt))

    def get_sites(self) -> List[TSC.SiteItem]:
        """Get all sites on the server."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(lambda: list(TSC.Pager(self.server.sites)))
        except Exception as e:
            raise TableauAPIError(f"Failed to get sites: {str(e)}")

    def get_workbooks(self, site_id: Optional[str] = None) -> List[TSC.WorkbookItem]:
        """Get all workbooks."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(lambda: list(TSC.Pager(self.server.workbooks)))
        except Exception as e:
            raise TableauAPIError(f"Failed to get workbooks: {str(e)}")

    def get_workbook_details(self, workbook_id: str) -> TSC.WorkbookItem:
        """Get detailed information about a specific workbook."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(self.server.workbooks.get_by_id, workbook_id)
        except Exception as e:
            raise TableauAPIError(f"Failed to get workbook details: {str(e)}")

    def get_datasources(self) -> List[TSC.DatasourceItem]:
        """Get all data sources."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(
                lambda: list(TSC.Pager(self.server.datasources))
            )
        except Exception as e:
            raise TableauAPIError(f"Failed to get data sources: {str(e)}")

    def get_datasource_details(self, datasource_id: str) -> TSC.DatasourceItem:
        """Get detailed information about a specific data source."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(
                self.server.datasources.get_by_id, datasource_id
            )
        except Exception as e:
            raise TableauAPIError(f"Failed to get data source details: {str(e)}")

    def get_projects(self) -> List[TSC.ProjectItem]:
        """Get all projects."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(lambda: list(TSC.Pager(self.server.projects)))
        except Exception as e:
            raise TableauAPIError(f"Failed to get projects: {str(e)}")

    def get_flows(self) -> List[TSC.FlowItem]:
        """Get all flows."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(lambda: list(TSC.Pager(self.server.flows)))
        except Exception as e:
            raise TableauAPIError(f"Failed to get flows: {str(e)}")

    def get_users(self) -> List[TSC.UserItem]:
        """Get all users."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(lambda: list(TSC.Pager(self.server.users)))
        except Exception as e:
            raise TableauAPIError(f"Failed to get users: {str(e)}")

    def get_groups(self) -> List[TSC.GroupItem]:
        """Get all groups."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(lambda: list(TSC.Pager(self.server.groups)))
        except Exception as e:
            raise TableauAPIError(f"Failed to get groups: {str(e)}")

    def search_content(
        self, search_term: str, content_type: str = "all"
    ) -> Dict[str, List[Any]]:
        """Search for content across the server."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        results = {"workbooks": [], "datasources": [], "projects": [], "flows": []}

        try:
            if content_type in ["all", "workbooks"]:
                req_option = TSC.RequestOptions()
                req_option.filter.add(
                    TSC.Filter(
                        TSC.RequestOptions.Field.Name,
                        TSC.RequestOptions.Operator.Contains,
                        search_term,
                    )
                )
                workbooks = self._retry_operation(
                    lambda: list(TSC.Pager(self.server.workbooks, req_option))
                )
                results["workbooks"] = workbooks

            if content_type in ["all", "datasources"]:
                req_option = TSC.RequestOptions()
                req_option.filter.add(
                    TSC.Filter(
                        TSC.RequestOptions.Field.Name,
                        TSC.RequestOptions.Operator.Contains,
                        search_term,
                    )
                )
                datasources = self._retry_operation(
                    lambda: list(TSC.Pager(self.server.datasources, req_option))
                )
                results["datasources"] = datasources

            if content_type in ["all", "projects"]:
                req_option = TSC.RequestOptions()
                req_option.filter.add(
                    TSC.Filter(
                        TSC.RequestOptions.Field.Name,
                        TSC.RequestOptions.Operator.Contains,
                        search_term,
                    )
                )
                projects = self._retry_operation(
                    lambda: list(TSC.Pager(self.server.projects, req_option))
                )
                results["projects"] = projects

            if content_type in ["all", "flows"]:
                req_option = TSC.RequestOptions()
                req_option.filter.add(
                    TSC.Filter(
                        TSC.RequestOptions.Field.Name,
                        TSC.RequestOptions.Operator.Contains,
                        search_term,
                    )
                )
                flows = self._retry_operation(
                    lambda: list(TSC.Pager(self.server.flows, req_option))
                )
                results["flows"] = flows

            return results

        except Exception as e:
            raise TableauAPIError(f"Failed to search content: {str(e)}")

    def get_workbook_views(self, workbook_id: str) -> List[TSC.ViewItem]:
        """Get all views for a specific workbook."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            workbook = self.get_workbook_details(workbook_id)
            return self._retry_operation(
                lambda: list(TSC.Pager(self.server.workbooks.populate_views, workbook))
            )
        except Exception as e:
            raise TableauAPIError(f"Failed to get workbook views: {str(e)}")

    def get_workbook_connections(self, workbook_id: str) -> List[TSC.ConnectionItem]:
        """Get all connections for a specific workbook."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            workbook = self.get_workbook_details(workbook_id)
            return self._retry_operation(
                lambda: list(
                    TSC.Pager(self.server.workbooks.populate_connections, workbook)
                )
            )
        except Exception as e:
            raise TableauAPIError(f"Failed to get workbook connections: {str(e)}")

    def get_datasource_connections(
        self, datasource_id: str
    ) -> List[TSC.ConnectionItem]:
        """Get all connections for a specific data source."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            datasource = self.get_datasource_details(datasource_id)
            return self._retry_operation(
                lambda: list(
                    TSC.Pager(self.server.datasources.populate_connections, datasource)
                )
            )
        except Exception as e:
            raise TableauAPIError(f"Failed to get data source connections: {str(e)}")

    def get_server_info(self) -> TSC.ServerInfoItem:
        """Get server information."""
        if not self.server:
            raise ConnectionError("Not connected to server")

        try:
            return self._retry_operation(self.server.server_info.get)
        except Exception as e:
            raise TableauAPIError(f"Failed to get server info: {str(e)}")


class TableauMetadataClient:
    """Client for Tableau Metadata API (GraphQL)."""

    def __init__(self, auth_config: AuthConfig):
        """Initialize the metadata client."""
        self.auth_config = auth_config
        self.base_url = f"{auth_config.server_url}/api/metadata/graphql"
        self.session = None
        self.csrf_token = None

    def _authenticate(self):
        """Authenticate and get CSRF token."""
        import requests

        self.session = requests.Session()

        # Use the same authentication as REST API
        authenticator = TableauAuthenticator(self.auth_config)
        server = authenticator.authenticate()

        # Extract auth token from server
        auth_token = server.auth_token

        # Set headers for GraphQL requests
        self.session.headers.update(
            {"X-Tableau-Auth": auth_token, "Content-Type": "application/json"}
        )

        return auth_token

    def query(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute GraphQL query."""
        if not self.session:
            self._authenticate()

        payload = {"query": query, "variables": variables or {}}

        try:
            response = self.session.post(self.base_url, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise TableauAPIError(f"GraphQL query failed: {str(e)}")

    def get_workbook_lineage(self, workbook_luid: str) -> Dict[str, Any]:
        """Get lineage information for a workbook."""
        query = """
        query GetWorkbookLineage($luid: String!) {
            workbook(luid: $luid) {
                luid
                name
                upstreamDatasources {
                    luid
                    name
                    upstreamTables {
                        luid
                        name
                        schema
                        database {
                            name
                        }
                    }
                }
                downstreamSheets {
                    luid
                    name
                }
            }
        }
        """

        variables = {"luid": workbook_luid}
        return self.query(query, variables)

    def get_datasource_lineage(self, datasource_luid: str) -> Dict[str, Any]:
        """Get lineage information for a data source."""
        query = """
        query GetDatasourceLineage($luid: String!) {
            publishedDatasource(luid: $luid) {
                luid
                name
                upstreamTables {
                    luid
                    name
                    schema
                    database {
                        name
                        connectionType
                    }
                    columns {
                        luid
                        name
                        dataType
                    }
                }
                downstreamWorkbooks {
                    luid
                    name
                }
            }
        }
        """

        variables = {"luid": datasource_luid}
        return self.query(query, variables)
