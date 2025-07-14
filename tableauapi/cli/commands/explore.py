"""Exploration commands for Tableau artifacts."""

import click
from rich.console import Console
from rich.table import Table

from tableauapi.core.auth import create_auth_config_from_env
from tableauapi.core.client import TableauClient
from tableauapi.utils.exceptions import ConfigurationError, TableauAPIError

console = Console()


@click.group(name="explore")
def explore_cmd():
    """Explore Tableau artifacts (workbooks, data sources, etc.)."""
    pass


@explore_cmd.command()
@click.option("--limit", "-l", type=int, help="Limit number of results")
@click.option("--project", "-p", help="Filter by project name")
@click.option("--owner", "-o", help="Filter by owner name")
def workbooks(limit, project, owner):
    """List workbooks on the server."""
    try:
        config = create_auth_config_from_env()
        client = TableauClient(config)

        with client.connection():
            with console.status("Fetching workbooks..."):
                workbooks = client.get_workbooks()

            # Apply filters
            if project:
                workbooks = [wb for wb in workbooks if wb.project_name == project]
            if owner:
                workbooks = [wb for wb in workbooks if wb.owner_name == owner]

            # Apply limit
            if limit:
                workbooks = workbooks[:limit]

            # Create table
            table = Table(title=f"Workbooks ({len(workbooks)} found)")
            table.add_column("Name", style="cyan")
            table.add_column("Project", style="green")
            table.add_column("Owner", style="yellow")
            table.add_column("Updated", style="magenta")
            table.add_column("Size", style="blue")

            for wb in workbooks:
                size_str = f"{wb.size} bytes" if wb.size else "N/A"
                updated_str = (
                    wb.updated_at.strftime("%Y-%m-%d %H:%M") if wb.updated_at else "N/A"
                )

                table.add_row(
                    wb.name, wb.project_name, wb.owner_name, updated_str, size_str
                )

            console.print(table)

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        exit(1)
    except TableauAPIError as e:
        console.print(f"[red]API error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)


@explore_cmd.command()
@click.option("--limit", "-l", type=int, help="Limit number of results")
@click.option("--project", "-p", help="Filter by project name")
@click.option("--owner", "-o", help="Filter by owner name")
def datasources(limit, project, owner):
    """List data sources on the server."""
    try:
        config = create_auth_config_from_env()
        client = TableauClient(config)

        with client.connection():
            with console.status("Fetching data sources..."):
                datasources = client.get_datasources()

            # Apply filters
            if project:
                datasources = [ds for ds in datasources if ds.project_name == project]
            if owner:
                datasources = [ds for ds in datasources if ds.owner_name == owner]

            # Apply limit
            if limit:
                datasources = datasources[:limit]

            # Create table
            table = Table(title=f"Data Sources ({len(datasources)} found)")
            table.add_column("Name", style="cyan")
            table.add_column("Project", style="green")
            table.add_column("Owner", style="yellow")
            table.add_column("Updated", style="magenta")
            table.add_column("Size", style="blue")

            for ds in datasources:
                size_str = f"{ds.size} bytes" if ds.size else "N/A"
                updated_str = (
                    ds.updated_at.strftime("%Y-%m-%d %H:%M") if ds.updated_at else "N/A"
                )

                table.add_row(
                    ds.name, ds.project_name, ds.owner_name, updated_str, size_str
                )

            console.print(table)

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        exit(1)
    except TableauAPIError as e:
        console.print(f"[red]API error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)


@explore_cmd.command()
def projects():
    """List projects on the server."""
    try:
        config = create_auth_config_from_env()
        client = TableauClient(config)

        with client.connection():
            with console.status("Fetching projects..."):
                projects = client.get_projects()

            # Create table
            table = Table(title=f"Projects ({len(projects)} found)")
            table.add_column("Name", style="cyan")
            table.add_column("Description", style="green")
            table.add_column("Parent", style="yellow")
            table.add_column("Permissions", style="magenta")

            for project in projects:
                parent_name = project.parent_id if project.parent_id else "Root"

                table.add_row(
                    project.name,
                    project.description or "No description",
                    parent_name,
                    project.content_permissions or "Default",
                )

            console.print(table)

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        exit(1)
    except TableauAPIError as e:
        console.print(f"[red]API error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)


@explore_cmd.command()
@click.argument("workbook_id")
@click.option("--views", is_flag=True, help="Show views")
@click.option("--connections", is_flag=True, help="Show connections")
def workbook(workbook_id, views, connections):
    """Show detailed information about a specific workbook."""
    try:
        config = create_auth_config_from_env()
        client = TableauClient(config)

        with client.connection():
            with console.status("Fetching workbook details..."):
                wb = client.get_workbook_details(workbook_id)

            # Basic info
            console.print(f"[bold cyan]Workbook: {wb.name}[/bold cyan]")
            console.print(f"ID: {wb.id}")
            console.print(f"Project: {wb.project_name}")
            console.print(f"Owner: {wb.owner_name}")
            console.print(
                f"Created: {wb.created_at.strftime('%Y-%m-%d %H:%M') if wb.created_at else 'N/A'}"
            )
            console.print(
                f"Updated: {wb.updated_at.strftime('%Y-%m-%d %H:%M') if wb.updated_at else 'N/A'}"
            )
            console.print(f"Size: {wb.size} bytes" if wb.size else "Size: N/A")
            console.print(f"Show Tabs: {wb.show_tabs}")
            console.print(f"URL: {wb.webpage_url}")

            if wb.description:
                console.print(f"Description: {wb.description}")

            if wb.tags:
                console.print(f"Tags: {', '.join(wb.tags)}")

            # Show views if requested
            if views:
                view_list = client.get_workbook_views(workbook_id)
                if view_list:
                    console.print(f"\n[bold]Views ({len(view_list)}):[/bold]")
                    view_table = Table()
                    view_table.add_column("Name", style="cyan")
                    view_table.add_column("Content URL", style="green")
                    view_table.add_column("Updated", style="magenta")

                    for view in view_list:
                        updated_str = (
                            view.updated_at.strftime("%Y-%m-%d %H:%M")
                            if view.updated_at
                            else "N/A"
                        )
                        view_table.add_row(
                            view.name, view.content_url or "N/A", updated_str
                        )

                    console.print(view_table)

            # Show connections if requested
            if connections:
                conn_list = client.get_workbook_connections(workbook_id)
                if conn_list:
                    console.print(f"\n[bold]Connections ({len(conn_list)}):[/bold]")
                    conn_table = Table()
                    conn_table.add_column("Data Source", style="cyan")
                    conn_table.add_column("Type", style="green")
                    conn_table.add_column("Server", style="yellow")
                    conn_table.add_column("Username", style="magenta")

                    for conn in conn_list:
                        conn_table.add_row(
                            conn.datasource_name or "N/A",
                            conn.connection_type or "N/A",
                            conn.server_address or "N/A",
                            conn.username or "N/A",
                        )

                    console.print(conn_table)

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        exit(1)
    except TableauAPIError as e:
        console.print(f"[red]API error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)


@explore_cmd.command()
@click.argument("search_term")
@click.option(
    "--type",
    "-t",
    type=click.Choice(["all", "workbooks", "datasources", "projects", "flows"]),
    default="all",
    help="Content type to search",
)
@click.option("--limit", "-l", type=int, default=20, help="Limit number of results")
def search(search_term, type, limit):
    """Search for content on the server."""
    try:
        config = create_auth_config_from_env()
        client = TableauClient(config)

        with client.connection():
            with console.status(f"Searching for '{search_term}'..."):
                results = client.search_content(search_term, type)

            total_results = sum(len(items) for items in results.values())
            console.print(
                f"[bold]Search Results for '{search_term}' ({total_results} found)[/bold]"
            )

            # Show workbooks
            if results["workbooks"]:
                workbooks = results["workbooks"][:limit]
                console.print(f"\n[cyan]Workbooks ({len(workbooks)} shown):[/cyan]")
                wb_table = Table()
                wb_table.add_column("Name", style="cyan")
                wb_table.add_column("Project", style="green")
                wb_table.add_column("Owner", style="yellow")

                for wb in workbooks:
                    wb_table.add_row(wb.name, wb.project_name, wb.owner_name)

                console.print(wb_table)

            # Show datasources
            if results["datasources"]:
                datasources = results["datasources"][:limit]
                console.print(
                    f"\n[green]Data Sources ({len(datasources)} shown):[/green]"
                )
                ds_table = Table()
                ds_table.add_column("Name", style="cyan")
                ds_table.add_column("Project", style="green")
                ds_table.add_column("Owner", style="yellow")

                for ds in datasources:
                    ds_table.add_row(ds.name, ds.project_name, ds.owner_name)

                console.print(ds_table)

            # Show projects
            if results["projects"]:
                projects = results["projects"][:limit]
                console.print(f"\n[yellow]Projects ({len(projects)} shown):[/yellow]")
                proj_table = Table()
                proj_table.add_column("Name", style="cyan")
                proj_table.add_column("Description", style="green")

                for proj in projects:
                    proj_table.add_row(proj.name, proj.description or "No description")

                console.print(proj_table)

            # Show flows
            if results["flows"]:
                flows = results["flows"][:limit]
                console.print(f"\n[magenta]Flows ({len(flows)} shown):[/magenta]")
                flow_table = Table()
                flow_table.add_column("Name", style="cyan")
                flow_table.add_column("Project", style="green")
                flow_table.add_column("Owner", style="yellow")

                for flow in flows:
                    flow_table.add_row(flow.name, flow.project_name, flow.owner_name)

                console.print(flow_table)

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        exit(1)
    except TableauAPIError as e:
        console.print(f"[red]API error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)
