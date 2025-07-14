"""Main CLI entry point for Tableau API tool."""

import click
from rich.console import Console
from rich.traceback import install
from dotenv import load_dotenv

from tableauapi.cli.commands.auth import auth_cmd
from tableauapi.cli.commands.explore import explore_cmd
from tableauapi.cli.commands.export import export_cmd
from tableauapi.cli.commands.metadata import metadata_cmd
from tableauapi.utils.exceptions import TableauAPIError

# Install rich traceback handler
install(show_locals=True)

console = Console()

# Load environment variables from .env file
load_dotenv(dotenv_path=".env", verbose=False)


@click.group()
@click.version_option(version="0.1.0", prog_name="tableau-cli")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.option("--no-color", is_flag=True, help="Disable colored output")
@click.pass_context
def cli(ctx, verbose, no_color):
    """Tableau API CLI tool for exploring artifacts and managing metadata.

    This tool allows you to connect to Tableau servers, explore workbooks,
    data sources, and other artifacts, and export metadata to local storage or S3.
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["no_color"] = no_color

    if no_color:
        console.no_color = True


# Add command groups
cli.add_command(auth_cmd)
cli.add_command(explore_cmd)
cli.add_command(metadata_cmd)
cli.add_command(export_cmd)


@cli.command()
@click.option("--debug", is_flag=True, help="Show debug information")
@click.pass_context
def config(ctx, debug):
    """Show current configuration."""
    from tableauapi.core.auth import create_auth_config_from_env
    import os

    try:
        console.print("[bold blue]Current Configuration[/bold blue]")

        if debug:
            console.print("[yellow]Debug: Environment variables:[/yellow]")
            console.print(f"TABLEAU_SERVER_URL: {os.getenv('TABLEAU_SERVER_URL', 'NOT SET')}")
            console.print(f"TABLEAU_SITE_ID: {os.getenv('TABLEAU_SITE_ID', 'NOT SET')}")
            console.print(f"TABLEAU_TOKEN_NAME: {os.getenv('TABLEAU_TOKEN_NAME', 'NOT SET')}")
            console.print(f"TABLEAU_TOKEN_VALUE: {'SET' if os.getenv('TABLEAU_TOKEN_VALUE') else 'NOT SET'}")
            console.print(f"TABLEAU_USERNAME: {os.getenv('TABLEAU_USERNAME', 'NOT SET')}")
            console.print(f"TABLEAU_PASSWORD: {'SET' if os.getenv('TABLEAU_PASSWORD') else 'NOT SET'}")
            console.print()

        # Try to load configuration from environment
        try:
            config = create_auth_config_from_env()
            console.print(f"Server URL: {config.server_url}")
            console.print(f"Site ID: {config.site_id or 'Default'}")
            console.print(f"Authentication Method: {config.auth_method.value}")

            if config.auth_method.value == "pat":
                console.print(f"Token Name: {config.token_name}")
                console.print("Token Value: [HIDDEN]")
            elif config.auth_method.value == "credentials":
                console.print(f"Username: {config.username}")
                console.print("Password: [HIDDEN]")

        except Exception as e:
            console.print(f"[red]No valid configuration found: {str(e)}[/red]")
            console.print("\n[yellow]Environment variables expected:[/yellow]")
            console.print("- TABLEAU_SERVER_URL (required)")
            console.print("- TABLEAU_SITE_ID (optional)")
            console.print("- TABLEAU_TOKEN_NAME + TABLEAU_TOKEN_VALUE (for PAT)")
            console.print("- TABLEAU_USERNAME + TABLEAU_PASSWORD (for credentials)")
            console.print("- TABLEAU_JWT_TOKEN (for JWT)")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        if ctx.obj["verbose"]:
            console.print_exception()


@cli.command()
def version():
    """Show version information."""
    console.print("Tableau CLI Tool v0.1.0")
    console.print("Built with tableauserverclient, click, and rich")


def main():
    """Main entry point."""
    try:
        cli()
    except TableauAPIError as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        console.print_exception()
        exit(1)


if __name__ == "__main__":
    main()
