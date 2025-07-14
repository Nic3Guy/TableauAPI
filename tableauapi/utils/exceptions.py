"""Custom exceptions for Tableau API CLI tool."""


class TableauAPIError(Exception):
    """Base exception for Tableau API errors."""

    pass


class AuthenticationError(TableauAPIError):
    """Authentication failed."""

    pass


class ConfigurationError(TableauAPIError):
    """Configuration error."""

    pass


class ConnectionError(TableauAPIError):
    """Connection error."""

    pass


class MetadataError(TableauAPIError):
    """Metadata processing error."""

    pass


class StorageError(TableauAPIError):
    """Storage operation error."""

    pass
