# Tableau API CLI Tool

A comprehensive command-line interface for exploring Tableau artifacts and managing metadata. This tool allows you to connect to Tableau servers, explore workbooks, data sources, and other artifacts, and export metadata to local storage or S3.

## Features

- **Multiple Authentication Methods**: Support for Personal Access Tokens (PAT), username/password, and JWT authentication
- **Comprehensive Artifact Exploration**: Browse workbooks, data sources, projects, flows, and more
- **Metadata Collection**: Extract detailed metadata including lineage information
- **Multiple Storage Backends**: Save metadata locally or to S3
- **Export Capabilities**: Export metadata to JSON, CSV, or Excel formats
- **Rich CLI Interface**: Beautiful command-line interface with progress bars and tables
- **Search Functionality**: Search across all content types
- **Filtering Options**: Filter by projects, owners, tags, and more

## Installation

### Using Poetry (Recommended)

1. Clone the repository:
```bash
git clone <repository-url>
cd TableauAPI
```

2. Install Poetry if you haven't already:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

3. Install dependencies:
```bash
poetry install
```

4. Activate the virtual environment:
```bash
poetry shell
# OR 
sourve .venv/bin/activate
```

5. Verify installation:
```bash
tableau-cli --version
```

### Using pip

1. Clone the repository:
```bash
git clone <repository-url>
cd TableauAPI
```

2. Install dependencies:
```bash
pip install -e .
```

3. Verify installation:
```bash
tableau-cli --version
```

## Quick Start

### 1. Authentication Setup

Set up authentication using environment variables:

```bash
# For Personal Access Token (Recommended)
export TABLEAU_SERVER_URL="https://your-tableau-server.com"
export TABLEAU_SITE_ID="your-site-id"  # Optional, leave empty for default site
export TABLEAU_TOKEN_NAME="your-token-name"
export TABLEAU_TOKEN_VALUE="your-token-value"

# Alternative: Username/Password
export TABLEAU_SERVER_URL="https://your-tableau-server.com"
export TABLEAU_SITE_ID="your-site-id"
export TABLEAU_USERNAME="your-username"
export TABLEAU_PASSWORD="your-password"
```

Or use interactive setup:
```bash
tableau-cli auth setup --interactive
```

### 2. Test Authentication

```bash
tableau-cli auth test
```

### 3. Explore Content

```bash
# List workbooks
tableau-cli explore workbooks

# List data sources
tableau-cli explore datasources

# List projects
tableau-cli explore projects

# Search for content
tableau-cli explore search "sales"

# Get detailed workbook information
tableau-cli explore workbook <workbook-id> --views --connections
```

### 4. Collect Metadata

```bash
# Collect all metadata and save locally
tableau-cli metadata collect --save-local

# Collect metadata with additional details
tableau-cli metadata collect --include-views --include-connections --include-lineage --save-local

# Save to S3
tableau-cli metadata collect --save-s3 --s3-bucket my-bucket --s3-prefix tableau-metadata/

# Filter by projects
tableau-cli metadata collect --projects "Project A" "Project B" --save-local
```

### 5. Export and Report

```bash
# Export metadata to Excel
tableau-cli export metadata --filename metadata.json --format xlsx

# Generate comprehensive report
tableau-cli export report --output tableau-report.xlsx

# Export with filters
tableau-cli export report --projects "Sales" --owners "john.doe" --output filtered-report.xlsx
```

## Commands Reference

### Authentication Commands

- `tableau-cli auth setup --interactive` - Interactive authentication setup
- `tableau-cli auth test` - Test current authentication
- `tableau-cli auth info` - Show authentication information

### Exploration Commands

- `tableau-cli explore workbooks` - List workbooks
- `tableau-cli explore datasources` - List data sources
- `tableau-cli explore projects` - List projects
- `tableau-cli explore workbook <id>` - Show workbook details
- `tableau-cli explore search <term>` - Search content

### Metadata Commands

- `tableau-cli metadata collect` - Collect comprehensive metadata
- `tableau-cli metadata list` - List stored metadata files
- `tableau-cli metadata show <filename>` - Show metadata contents
- `tableau-cli metadata lineage <workbook-id>` - Get lineage information

### Export Commands

- `tableau-cli export metadata` - Export metadata to different formats
- `tableau-cli export report` - Generate comprehensive report

### Configuration Commands

- `tableau-cli config` - Show current configuration
- `tableau-cli version` - Show version information

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `TABLEAU_SERVER_URL` | Tableau Server URL | Yes |
| `TABLEAU_SITE_ID` | Site ID (empty for default) | No |
| `TABLEAU_TOKEN_NAME` | Personal Access Token name | Yes (for PAT) |
| `TABLEAU_TOKEN_VALUE` | Personal Access Token value | Yes (for PAT) |
| `TABLEAU_USERNAME` | Username | Yes (for credentials) |
| `TABLEAU_PASSWORD` | Password | Yes (for credentials) |
| `TABLEAU_JWT_TOKEN` | JWT Token | Yes (for JWT) |

### S3 Configuration

For S3 storage, you can use standard AWS credentials:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

Or use AWS CLI profiles, IAM roles, or other AWS credential methods.

## Examples

### Basic Usage

```bash
# Test connection
tableau-cli auth test

# Explore workbooks in a specific project
tableau-cli explore workbooks --project "Sales Dashboard"

# Search for specific content
tableau-cli explore search "quarterly" --type workbooks

# Get detailed information about a workbook
tableau-cli explore workbook wb-abc123 --views --connections
```

### Advanced Metadata Collection

```bash
# Collect comprehensive metadata with lineage
tableau-cli metadata collect \
    --include-views \
    --include-connections \
    --include-lineage \
    --save-local \
    --save-s3 \
    --s3-bucket tableau-metadata \
    --format json.gz

# Collect filtered metadata
tableau-cli metadata collect \
    --projects "Sales" "Marketing" \
    --owners "john.doe" "jane.smith" \
    --tags "important" "production" \
    --save-local
```

### Export and Reporting

```bash
# Export to Excel with all sheets
tableau-cli export metadata \
    --filename latest_metadata.json \
    --format xlsx \
    --output tableau-export.xlsx

# Generate filtered report
tableau-cli export report \
    --projects "Sales" \
    --output sales-report.xlsx

# Export specific metadata file
tableau-cli export metadata \
    --filename "default_20240101_120000.json" \
    --format csv \
    --output metadata-export/
```

## Development

### Setup Development Environment

Using Poetry:
```bash
# Install dependencies including dev dependencies
poetry install

# Activate virtual environment
poetry shell

# Run linting
poetry run ruff check .

# Run formatting
poetry run black .

# Run type checking
poetry run mypy tableauapi/

# Run tests
poetry run pytest
```

### Project Structure

```
tableauapi/
├── cli/                 # CLI commands and interface
│   ├── main.py         # Main entry point
│   └── commands/       # Command modules
├── core/               # Core functionality
│   ├── auth.py        # Authentication
│   ├── client.py      # API client wrapper
│   ├── metadata.py    # Metadata processing
│   └── storage.py     # Storage backends
└── utils/             # Utility functions
    ├── exceptions.py  # Custom exceptions
    └── helpers.py     # Helper functions
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Testing

Run tests with:
```bash
pytest tests/
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Ensure your credentials are correct and the server URL is accessible
2. **Permission Errors**: Verify your user has appropriate permissions on the Tableau server
3. **Network Issues**: Check firewall settings and network connectivity
4. **S3 Errors**: Verify AWS credentials and bucket permissions

### Debug Mode

Enable verbose logging for troubleshooting:
```bash
tableau-cli --verbose explore workbooks
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and feature requests, please create an issue in the GitHub repository.