# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2025-03-31

### Changed
- **Simplified tool naming convention**: Removed prefix logic while registering tool names
  - Tools now use only the original names without any additional prefixes
  - Format is now simply `<tooltype>_<dbID>` (e.g., `query_mysql1`)
  - Global tools like `list_databases` have no prefix or suffix
- Removed `MCP_SERVER_NAME` environment variable as it's no longer needed
- Removed `MCP_TOOL_PREFIX` environment variable from Dockerfile
- Updated documentation to reflect new naming convention

### Fixed
- Eliminated duplicate tool registration
- Simplified tool name parsing logic

## [1.0.0] - 2025-03-22

### Added
- Initial release of DB MCP Server
- Multi-database support for MySQL and PostgreSQL
- Database-specific tool generation for each connection
- Support for queries, statements, transactions, schema exploration, and performance analysis
- Support for both STDIO and SSE transport modes
- Docker and Docker Compose deployment options 