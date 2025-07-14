"""Utility helper functions."""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from rich.console import Console
from rich.logging import RichHandler

console = Console()


def setup_logging(
    verbose: bool = False, log_file: Optional[str] = None
) -> logging.Logger:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO

    # Create logger
    logger = logging.getLogger("tableauapi")
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

    # Console handler with rich formatting
    console_handler = RichHandler(
        console=console, show_time=True, show_path=verbose, markup=True
    )
    console_handler.setLevel(level)

    # Format for console
    console_format = "%(message)s"
    console_handler.setFormatter(logging.Formatter(console_format))

    logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)

        # More detailed format for file
        file_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        file_handler.setFormatter(logging.Formatter(file_format))

        logger.addHandler(file_handler)

    return logger


def load_config_file(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        console.print(f"[red]Configuration file not found: {config_path}[/red]")
        return {}
    except json.JSONDecodeError as e:
        console.print(f"[red]Invalid JSON in configuration file: {str(e)}[/red]")
        return {}


def save_config_file(config: Dict[str, Any], config_path: str) -> bool:
    """Save configuration to JSON file."""
    try:
        # Create directory if it doesn't exist
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)

        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, default=str)

        return True
    except Exception as e:
        console.print(f"[red]Failed to save configuration: {str(e)}[/red]")
        return False


def get_config_dir() -> Path:
    """Get the configuration directory for the application."""
    if os.name == "nt":  # Windows
        config_dir = Path(os.environ.get("APPDATA", "~")) / "tableau-cli"
    else:  # Unix-like systems
        config_dir = Path.home() / ".config" / "tableau-cli"

    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_default_config_path() -> Path:
    """Get the default configuration file path."""
    return get_config_dir() / "config.json"


def format_bytes(size: int) -> str:
    """Format bytes to human-readable format."""
    if size is None:
        return "N/A"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0

    return f"{size:.1f} PB"


def format_datetime(dt: Optional[datetime]) -> str:
    """Format datetime to human-readable format."""
    if dt is None:
        return "N/A"

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def truncate_string(text: str, max_length: int = 50) -> str:
    """Truncate string to maximum length."""
    if len(text) <= max_length:
        return text

    return text[: max_length - 3] + "..."


def sanitize_filename(filename: str) -> str:
    """Sanitize filename by removing invalid characters."""
    invalid_chars = '<>:"/\\|?*'

    for char in invalid_chars:
        filename = filename.replace(char, "_")

    return filename


def validate_url(url: str) -> bool:
    """Validate if URL is properly formatted."""
    import re

    url_pattern = re.compile(
        r"^https?://"  # http:// or https://
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"  # domain...
        r"localhost|"  # localhost...
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
        r"(?::\d+)?"  # optional port
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )

    return url_pattern.match(url) is not None


def confirm_action(message: str, default: bool = False) -> bool:
    """Confirm an action with the user."""
    from rich.prompt import Confirm

    return Confirm.ask(message, default=default)


def select_from_list(items: list, prompt: str = "Select an item") -> Any:
    """Allow user to select from a list of items."""
    if not items:
        return None

    if len(items) == 1:
        return items[0]

    console.print(f"[bold]{prompt}:[/bold]")
    for i, item in enumerate(items, 1):
        console.print(f"{i}. {item}")

    while True:
        try:
            choice = int(console.input("Enter your choice: "))
            if 1 <= choice <= len(items):
                return items[choice - 1]
            else:
                console.print("[red]Invalid choice. Please try again.[/red]")
        except ValueError:
            console.print("[red]Please enter a valid number.[/red]")
        except KeyboardInterrupt:
            console.print("\n[yellow]Operation cancelled.[/yellow]")
            return None


def progress_callback(current: int, total: int, message: str = "Processing..."):
    """Simple progress callback function."""
    percentage = (current / total) * 100 if total > 0 else 0
    console.print(f"\r{message} {current}/{total} ({percentage:.1f}%)", end="")

    if current == total:
        console.print()  # New line when complete


def retry_on_failure(func, max_retries: int = 3, delay: float = 1.0):
    """Retry a function on failure with exponential backoff."""
    import time

    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise

            console.print(f"[yellow]Attempt {attempt + 1} failed: {str(e)}[/yellow]")
            console.print(f"[yellow]Retrying in {delay} seconds...[/yellow]")
            time.sleep(delay)
            delay *= 2  # Exponential backoff


def get_env_var(
    name: str, default: Optional[str] = None, required: bool = False
) -> Optional[str]:
    """Get environment variable with optional default and validation."""
    value = os.environ.get(name, default)

    if required and value is None:
        raise ValueError(f"Required environment variable '{name}' not set")

    return value


def create_timestamp() -> str:
    """Create a timestamp string for filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_tags(tags_str: str) -> list:
    """Parse comma-separated tags string into list."""
    if not tags_str:
        return []

    return [tag.strip() for tag in tags_str.split(",") if tag.strip()]


def dict_to_table_rows(data: Dict[str, Any]) -> list:
    """Convert dictionary to table rows for display."""
    rows = []

    for key, value in data.items():
        if isinstance(value, (dict, list)):
            value_str = json.dumps(value, indent=2)
        else:
            value_str = str(value)

        rows.append([key, value_str])

    return rows
