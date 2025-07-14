"""Authentication commands."""

import click
from rich.console import Console

from tableauapi.core.auth import (
    TableauAuthenticator,
    create_auth_config_from_env,
    create_auth_config_interactive,
)
from tableauapi.utils.exceptions import AuthenticationError, ConfigurationError

console = Console()


@click.group(name="auth")
def auth_cmd():
    """Authentication management commands."""
    pass


@auth_cmd.command()
@click.option("--interactive", "-i", is_flag=True, help="Interactive setup")
def setup(interactive):
    """Set up authentication configuration."""
    try:
        if interactive:
            console.print("[bold blue]Interactive Authentication Setup[/bold blue]")
            config = create_auth_config_interactive()

            # Test the configuration
            console.print("\n[yellow]Testing configuration...[/yellow]")
            authenticator = TableauAuthenticator(config)

            with console.status("Connecting to Tableau Server..."):
                server = authenticator.authenticate()
                server_info = server.server_info.get()
                # Get current site info to verify connection
                current_site = server.sites.get_by_id(server.site_id)
                authenticator.sign_out()

            console.print("[green]✓ Successfully connected to Tableau Server[/green]")
            console.print(f"Server version: {server_info.product_version}")
            console.print(f"REST API version: {server_info.rest_api_version}")
            console.print(f"Connected to site: {current_site.name}")

            # Suggest saving to environment
            console.print(
                "\n[yellow]To persist this configuration, set these environment variables:[/yellow]"
            )
            console.print(f"export TABLEAU_SERVER_URL='{config.server_url}'")
            console.print(f"export TABLEAU_SITE_ID='{config.site_id}'")

            if config.auth_method.value == "pat":
                console.print(f"export TABLEAU_TOKEN_NAME='{config.token_name}'")
                console.print(f"export TABLEAU_TOKEN_VALUE='{config.token_value}'")
            elif config.auth_method.value == "credentials":
                console.print(f"export TABLEAU_USERNAME='{config.username}'")
                console.print(f"export TABLEAU_PASSWORD='{config.password}'")

        else:
            console.print("[yellow]Use --interactive flag for guided setup[/yellow]")
            console.print("Or set these environment variables:")
            console.print("- TABLEAU_SERVER_URL")
            console.print("- TABLEAU_SITE_ID (optional)")
            console.print("- TABLEAU_TOKEN_NAME + TABLEAU_TOKEN_VALUE (recommended)")
            console.print("- Or TABLEAU_USERNAME + TABLEAU_PASSWORD")

    except (AuthenticationError, ConfigurationError) as e:
        console.print(f"[red]Authentication error: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)


@auth_cmd.command()
@click.option("--debug", is_flag=True, help="Show debug information")
def test(debug):
    """Test current authentication configuration."""
    try:
        console.print("[bold blue]Testing Authentication[/bold blue]")

        if debug:
            import os
            console.print("[yellow]Debug: Environment variables:[/yellow]")
            console.print(f"TABLEAU_SERVER_URL: {os.getenv('TABLEAU_SERVER_URL', 'NOT SET')}")
            console.print(f"TABLEAU_SITE_ID: {os.getenv('TABLEAU_SITE_ID', 'NOT SET')}")
            console.print(f"TABLEAU_TOKEN_NAME: {os.getenv('TABLEAU_TOKEN_NAME', 'NOT SET')}")
            console.print(f"TABLEAU_TOKEN_VALUE: {'SET' if os.getenv('TABLEAU_TOKEN_VALUE') else 'NOT SET'}")
            console.print(f"TABLEAU_USERNAME: {os.getenv('TABLEAU_USERNAME', 'NOT SET')}")
            console.print(f"TABLEAU_PASSWORD: {'SET' if os.getenv('TABLEAU_PASSWORD') else 'NOT SET'}")
            console.print()

        # Load configuration from environment
        config = create_auth_config_from_env()

        console.print(f"Server URL: {config.server_url}")
        console.print(f"Site ID: {config.site_id or 'Default'}")
        console.print(f"Authentication Method: {config.auth_method.value}")

        # Test authentication
        authenticator = TableauAuthenticator(config)

        with console.status("Connecting to Tableau Server..."):
            server = authenticator.authenticate()
            server_info = server.server_info.get()
            
            # Get current site info (doesn't require admin privileges)
            current_site = server.sites.get_by_id(server.site_id)

            authenticator.sign_out()

        console.print("[green]✓ Authentication successful[/green]")
        console.print(f"Server version: {server_info.product_version}")
        console.print(f"REST API version: {server_info.rest_api_version}")
        console.print(f"Current site: {current_site.name}")
        console.print(f"Site ID: {current_site.id}")
        console.print(f"Site URL: {current_site.content_url}")

    except ConfigurationError as e:
        console.print(f"[red]Configuration error: {str(e)}[/red]")
        console.print(
            "\n[yellow]Run 'tableau-cli auth setup --interactive' to configure[/yellow]"
        )
        exit(1)
    except AuthenticationError as e:
        console.print(f"[red]Authentication failed: {str(e)}[/red]")
        exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/red]")
        exit(1)


@auth_cmd.command()
def info():
    """Show authentication information."""
    try:
        config = create_auth_config_from_env()

        console.print("[bold blue]Authentication Configuration[/bold blue]")
        console.print(f"Server URL: {config.server_url}")
        console.print(f"Site ID: {config.site_id or 'Default'}")
        console.print(f"Authentication Method: {config.auth_method.value}")

        if config.auth_method.value == "pat":
            console.print(f"Token Name: {config.token_name}")
            console.print("Token Value: [HIDDEN]")
        elif config.auth_method.value == "credentials":
            console.print(f"Username: {config.username}")
            console.print("Password: [HIDDEN]")
        elif config.auth_method.value == "jwt":
            console.print("JWT Token: [HIDDEN]")

    except ConfigurationError as e:
        console.print(f"[red]No configuration found: {str(e)}[/red]")
        console.print(
            "\n[yellow]Run 'tableau-cli auth setup --interactive' to configure[/yellow]"
        )
        exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        exit(1)
