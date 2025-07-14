"""Authentication module for Tableau API."""

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import tableauserverclient as TSC

from tableauapi.utils.exceptions import AuthenticationError, ConfigurationError


class AuthMethod(Enum):
    """Authentication methods."""

    PERSONAL_ACCESS_TOKEN = "pat"
    USERNAME_PASSWORD = "credentials"
    JWT = "jwt"


@dataclass
class AuthConfig:
    """Authentication configuration."""

    server_url: str
    site_id: str = ""
    auth_method: AuthMethod = AuthMethod.PERSONAL_ACCESS_TOKEN
    token_name: Optional[str] = None
    token_value: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    jwt_token: Optional[str] = None


class TableauAuthenticator:
    """Handles authentication with Tableau Server."""

    def __init__(self, config: AuthConfig):
        """Initialize authenticator with configuration."""
        self.config = config
        self.server = TSC.Server(config.server_url)
        self._auth_object = None

    def create_auth_object(self):
        """Create appropriate authentication object based on method."""
        if self.config.auth_method == AuthMethod.PERSONAL_ACCESS_TOKEN:
            if not self.config.token_name or not self.config.token_value:
                raise AuthenticationError(
                    "Token name and value required for PAT authentication"
                )
            return TSC.PersonalAccessTokenAuth(
                self.config.token_name, self.config.token_value, self.config.site_id
            )

        elif self.config.auth_method == AuthMethod.USERNAME_PASSWORD:
            if not self.config.username or not self.config.password:
                raise AuthenticationError(
                    "Username and password required for credential authentication"
                )
            return TSC.TableauAuth(
                self.config.username, self.config.password, self.config.site_id
            )

        elif self.config.auth_method == AuthMethod.JWT:
            if not self.config.jwt_token:
                raise AuthenticationError("JWT token required for JWT authentication")
            return TSC.JWTAuth(self.config.jwt_token)

        else:
            raise AuthenticationError(
                f"Unsupported authentication method: {self.config.auth_method}"
            )

    def authenticate(self) -> TSC.Server:
        """Authenticate with Tableau Server."""
        try:
            auth_object = self.create_auth_object()
            self.server.auth.sign_in(auth_object)
            return self.server
        except Exception as e:
            raise AuthenticationError(f"Authentication failed: {str(e)}")

    def sign_out(self):
        """Sign out from Tableau Server."""
        try:
            if self.server.is_signed_in():
                self.server.auth.sign_out()
        except Exception as e:
            raise AuthenticationError(f"Sign out failed: {str(e)}")


def create_auth_config_from_env() -> AuthConfig:
    """Create authentication configuration from environment variables."""
    server_url = os.getenv("TABLEAU_SERVER_URL")
    if not server_url:
        raise ConfigurationError("TABLEAU_SERVER_URL environment variable required")

    site_id = os.getenv("TABLEAU_SITE_ID", "")

    # Check for PAT credentials first
    token_name = os.getenv("TABLEAU_TOKEN_NAME")
    token_value = os.getenv("TABLEAU_TOKEN_VALUE")

    if token_name and token_value:
        return AuthConfig(
            server_url=server_url,
            site_id=site_id,
            auth_method=AuthMethod.PERSONAL_ACCESS_TOKEN,
            token_name=token_name,
            token_value=token_value,
        )

    # Check for username/password
    username = os.getenv("TABLEAU_USERNAME")
    password = os.getenv("TABLEAU_PASSWORD")

    if username and password:
        return AuthConfig(
            server_url=server_url,
            site_id=site_id,
            auth_method=AuthMethod.USERNAME_PASSWORD,
            username=username,
            password=password,
        )

    # Check for JWT
    jwt_token = os.getenv("TABLEAU_JWT_TOKEN")

    if jwt_token:
        return AuthConfig(
            server_url=server_url,
            site_id=site_id,
            auth_method=AuthMethod.JWT,
            jwt_token=jwt_token,
        )

    raise ConfigurationError(
        "No valid authentication credentials found in environment variables"
    )


def create_auth_config_interactive() -> AuthConfig:
    """Create authentication configuration interactively."""
    from rich.console import Console
    from rich.prompt import Prompt

    console = Console()

    console.print("[bold blue]Tableau Server Authentication Setup[/bold blue]")

    server_url = Prompt.ask("Server URL", default="https://your-server.com")
    site_id = Prompt.ask("Site ID (leave blank for default)", default="")

    console.print("\n[bold]Authentication Methods:[/bold]")
    console.print("1. Personal Access Token (Recommended)")
    console.print("2. Username/Password")
    console.print("3. JWT Token")

    auth_choice = Prompt.ask(
        "Choose authentication method", choices=["1", "2", "3"], default="1"
    )

    if auth_choice == "1":
        token_name = Prompt.ask("Token Name")
        token_value = Prompt.ask("Token Value", password=True)

        return AuthConfig(
            server_url=server_url,
            site_id=site_id,
            auth_method=AuthMethod.PERSONAL_ACCESS_TOKEN,
            token_name=token_name,
            token_value=token_value,
        )

    elif auth_choice == "2":
        username = Prompt.ask("Username")
        password = Prompt.ask("Password", password=True)

        return AuthConfig(
            server_url=server_url,
            site_id=site_id,
            auth_method=AuthMethod.USERNAME_PASSWORD,
            username=username,
            password=password,
        )

    elif auth_choice == "3":
        jwt_token = Prompt.ask("JWT Token", password=True)

        return AuthConfig(
            server_url=server_url,
            site_id=site_id,
            auth_method=AuthMethod.JWT,
            jwt_token=jwt_token,
        )

    raise ConfigurationError("Invalid authentication method selected")
