"""Metadata management commands."""

import click
from rich.console import Console
from rich.json import JSON
from rich.progress import Progress
from rich.table import Table

from tableauapi.core.auth import create_auth_config_from_env
from tableauapi.core.client import TableauClient, TableauMetadataClient
from tableauapi.core.metadata import MetadataProcessor
from tableauapi.core.storage import LocalStorage, S3Storage, StorageManager
from tableauapi.utils.exceptions import ConfigurationError, TableauAPIError

console = Console()


@click.group(name="metadata")
def metadata_cmd():
    """Metadata collection and management commands."""
    pass


@metadata_cmd.command()
@click.option("--include-views", is_flag=True, help="Include view details")
@click.option("--include-connections", is_flag=True, help="Include connection details")
@click.option("--include-lineage", is_flag=True, help="Include lineage information")
@click.option("--projects", "-p", multiple=True, help="Filter by project names")
@click.option("--owners", "-o", multiple=True, help="Filter by owner names")
@click.option("--tags", "-t", multiple=True, help="Filter by tags")
@click.option("--save-local", is_flag=True, help="Save to local storage")
@click.option("--save-s3", is_flag=True, help="Save to S3 storage")
@click.option("--s3-bucket", help="S3 bucket name")
@click.option("--s3-prefix", default="tableau_metadata/", help="S3 prefix")
@click.option(
    "--format",
    "-f",
    type=click.Choice(["json", "json.gz", "csv"]),
    default="json",
    help="Output format",
)
def collect(
    include_views,
    include_connections,
    include_lineage,
    projects,
    owners,
    tags,
    save_local,
    save_s3,
    s3_bucket,
    s3_prefix,
    format,
):
    """Collect comprehensive metadata from Tableau Server."""
    try:
        config = create_auth_config_from_env()
        client = TableauClient(config)
        processor = MetadataProcessor()

        # Initialize storage
        storage_backends = []
        local_storage = None
        s3_storage = None

        if save_local:
            local_storage = LocalStorage()
            storage_backends.append("local")

        if save_s3:
            if not s3_bucket:
                console.print("[red]S3 bucket name required for S3 storage[/red]")
                exit(1)
            s3_storage = S3Storage(s3_bucket, s3_prefix)
            storage_backends.append("s3")

        storage_manager = StorageManager(local_storage, s3_storage)

        with client.connection():
            with Progress() as progress:
                # Get server info
                task = progress.add_task("Collecting server info...", total=1)
                server_info = client.get_server_info()
                progress.update(task, advance=1)

                # Get all content
                task = progress.add_task("Collecting projects...", total=1)
                projects_data = client.get_projects()
                progress.update(task, advance=1)

                task = progress.add_task("Collecting workbooks...", total=1)
                workbooks_data = client.get_workbooks()
                progress.update(task, advance=1)

                task = progress.add_task("Collecting data sources...", total=1)
                datasources_data = client.get_datasources()
                progress.update(task, advance=1)

                task = progress.add_task("Collecting flows...", total=1)
                flows_data = client.get_flows()
                progress.update(task, advance=1)

                # Process workbooks with additional details
                processed_workbooks = []
                if workbooks_data:
                    task = progress.add_task(
                        "Processing workbooks...", total=len(workbooks_data)
                    )

                    for wb in workbooks_data:
                        views = None
                        connections = None
                        lineage = None

                        if include_views:
                            try:
                                views = client.get_workbook_views(wb.id)
                            except Exception as e:
                                console.print(
                                    f"[yellow]Warning: Could not get views for {wb.name}: {str(e)}[/yellow]"
                                )

                        if include_connections:
                            try:
                                connections = client.get_workbook_connections(wb.id)
                            except Exception as e:
                                console.print(
                                    f"[yellow]Warning: Could not get connections for {wb.name}: {str(e)}[/yellow]"
                                )

                        if include_lineage:
                            try:
                                metadata_client = TableauMetadataClient(config)
                                lineage = metadata_client.get_workbook_lineage(wb.id)
                            except Exception as e:
                                console.print(
                                    f"[yellow]Warning: Could not get lineage for {wb.name}: {str(e)}[/yellow]"
                                )

                        processed_wb = processor.process_workbook(
                            wb, views, connections, lineage
                        )
                        processed_workbooks.append(processed_wb)
                        progress.update(task, advance=1)

                # Process data sources with additional details
                processed_datasources = []
                if datasources_data:
                    task = progress.add_task(
                        "Processing data sources...", total=len(datasources_data)
                    )

                    for ds in datasources_data:
                        connections = None
                        lineage = None

                        if include_connections:
                            try:
                                connections = client.get_datasource_connections(ds.id)
                            except Exception as e:
                                console.print(
                                    f"[yellow]Warning: Could not get connections for {ds.name}: {str(e)}[/yellow]"
                                )

                        if include_lineage:
                            try:
                                metadata_client = TableauMetadataClient(config)
                                lineage = metadata_client.get_datasource_lineage(ds.id)
                            except Exception as e:
                                console.print(
                                    f"[yellow]Warning: Could not get lineage for {ds.name}: {str(e)}[/yellow]"
                                )

                        processed_ds = processor.process_datasource(
                            ds, connections, lineage
                        )
                        processed_datasources.append(processed_ds)
                        progress.update(task, advance=1)

                # Process other content
                processed_projects = [
                    processor.process_project(p) for p in projects_data
                ]
                processed_flows = [processor.process_flow(f) for f in flows_data]

                # Create server metadata
                server_metadata = processor.create_server_metadata(
                    server_info,
                    processed_workbooks,
                    processed_datasources,
                    processed_projects,
                    processed_flows,
                    config.server_url,
                    config.site_id,
                    config.site_id or "default",
                )

                # Apply filters
                if projects or owners or tags:
                    filters = {}
                    if projects:
                        filters["project_names"] = list(projects)
                    if owners:
                        filters["owner_names"] = list(owners)
                    if tags:
                        filters["tags"] = list(tags)

                    server_metadata = processor.filter_metadata(
                        server_metadata, filters
                    )

                # Save metadata
                if storage_backends:
                    task = progress.add_task(
                        "Saving metadata...", total=len(storage_backends)
                    )

                    filename = storage_manager.generate_filename(server_metadata)
                    if format != "json":
                        filename = filename.replace(".json", f".{format}")

                    paths = storage_manager.save_metadata(
                        server_metadata, filename, storage_backends, format
                    )

                    console.print("\n[green]✓ Metadata collected successfully[/green]")
                    console.print(f"Workbooks: {len(server_metadata.workbooks)}")
                    console.print(f"Data Sources: {len(server_metadata.datasources)}")
                    console.print(f"Projects: {len(server_metadata.projects)}")
                    console.print(f"Flows: {len(server_metadata.flows)}")

                    console.print("\n[bold]Saved to:[/bold]")
                    for backend, path in paths.items():
                        console.print(f"  {backend}: {path}")

                    progress.update(task, advance=len(storage_backends))

                else:
                    # Display summary
                    summary = processor.get_metadata_summary(server_metadata)
                    console.print("\n[green]✓ Metadata collected successfully[/green]")
                    console.print(JSON.from_data(summary))

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        exit(1)
    except TableauAPIError as e:
        console.print(f"[red]API error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        console.print_exception()
        exit(1)


@metadata_cmd.command()
@click.option(
    "--backend",
    "-b",
    type=click.Choice(["local", "s3"]),
    default="local",
    help="Storage backend to list from",
)
@click.option("--s3-bucket", help="S3 bucket name (required for S3 backend)")
@click.option("--s3-prefix", default="tableau_metadata/", help="S3 prefix")
def list(backend, s3_bucket, s3_prefix):
    """List stored metadata files."""
    try:
        storage_manager = StorageManager()

        if backend == "s3":
            if not s3_bucket:
                console.print("[red]S3 bucket name required for S3 backend[/red]")
                exit(1)
            s3_storage = S3Storage(s3_bucket, s3_prefix)
            storage_manager = StorageManager(s3_storage=s3_storage)

        if backend == "local":
            files = storage_manager.local_storage.list_metadata_files()
        else:
            files = storage_manager.s3_storage.list_metadata_files()

        if not files:
            console.print(
                f"[yellow]No metadata files found in {backend} storage[/yellow]"
            )
            return

        console.print(f"[bold]Metadata files in {backend} storage:[/bold]")

        table = Table()
        table.add_column("File", style="cyan")
        table.add_column("Size", style="green")
        table.add_column("Modified", style="magenta")

        for file_path in files:
            # For now, just show the filename
            # In a real implementation, you'd get file stats
            table.add_row(file_path, "N/A", "N/A")

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        exit(1)


@metadata_cmd.command()
@click.argument("filename")
@click.option(
    "--backend",
    "-b",
    type=click.Choice(["local", "s3"]),
    default="local",
    help="Storage backend to load from",
)
@click.option("--s3-bucket", help="S3 bucket name (required for S3 backend)")
@click.option("--s3-prefix", default="tableau_metadata/", help="S3 prefix")
@click.option("--summary", is_flag=True, help="Show summary only")
def show(filename, backend, s3_bucket, s3_prefix, summary):
    """Show metadata file contents."""
    try:
        storage_manager = StorageManager()

        if backend == "s3":
            if not s3_bucket:
                console.print("[red]S3 bucket name required for S3 backend[/red]")
                exit(1)
            s3_storage = S3Storage(s3_bucket, s3_prefix)
            storage_manager = StorageManager(s3_storage=s3_storage)

        metadata = storage_manager.load_metadata(filename, backend)
        processor = MetadataProcessor()

        if summary:
            summary_data = processor.get_metadata_summary(metadata)
            console.print(JSON.from_data(summary_data))
        else:
            metadata_dict = processor.metadata_to_dict(metadata)
            console.print(JSON.from_data(metadata_dict))

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        exit(1)


@metadata_cmd.command()
@click.argument("workbook_id")
def lineage(workbook_id):
    """Get lineage information for a workbook."""
    try:
        config = create_auth_config_from_env()
        metadata_client = TableauMetadataClient(config)

        with console.status("Fetching lineage information..."):
            lineage_data = metadata_client.get_workbook_lineage(workbook_id)

        console.print(f"[bold]Lineage for workbook {workbook_id}:[/bold]")
        console.print(JSON.from_data(lineage_data))

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        exit(1)
    except TableauAPIError as e:
        console.print(f"[red]API error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)
