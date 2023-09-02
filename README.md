# Muscan: Music Library Scanner

Muscan is a Python-based utility designed to scan your music library and populate a PostgreSQL database with metadata. This enables powerful SQL-based analysis of your music collection.

## Features

- Scans music files recursively from a given directory.
- Populates a PostgreSQL database with relevant metadata.
- Supports multiple audio formats through TinyTag library.
- Provides CLI interface for easy database initialization and scanning operations.
- Offers SHA-256 file hashing for duplicate detection.

## Requirements

These are versions used for dev, but it probably runs on earlier versions.

- Python 3.11
- PostgreSQL 15
- Poetry

## Installation

Clone the repository and navigate to the project directory.

```bash
git clone https://github.com/your_username/muscan.git
cd muscan
```

### Dependencies

We use [Poetry](https://python-poetry.org/) for dependency management. If you haven't installed Poetry yet, you can install it following the instructions [here](https://python-poetry.org/docs/#installation).

Once Poetry is installed, you can install project dependencies by running:

```bash
poetry install
```

## Database Configuration

By default, the application assumes:

- PostgreSQL is running on localhost:5432
- There's no password for PostgreSQL
- The process user and database user are the same
- A schema named "musician" will be created in the PostgreSQL database

To set up the database, run:

```bash
poetry run python muscan.py init_db
```

## Usage

Muscan provides a command-line interface for initializing the database and scanning your music library.

### Initialize Database

To initialize the database schema, run:

```bash
poetry run python muscan.py init_db
```

### Scan Music Library

To scan a music directory, use the `scan` command with the `--path` flag to specify the directory and `--scan-name` to give this scan a name.

```bash
poetry run python muscan.py scan --path /path/to/music/directory --scan-name my_scan_name
```

## SQL Analysis

Once the database is populated, you can run SQL queries for analysis. For example, to find files present in one scan but not another, you can execute SQL queries like the ones in the codebase.

---

Feel free to adjust the `README.md` content to better match your project's specific needs.