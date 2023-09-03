import os
import sys
import psycopg2
import hashlib
from tinytag import TinyTag
from getpass import getuser
import argparse
from datetime import datetime


def init_db():
    # Initialize the database
    username = getuser()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user=username
    )
    cur = conn.cursor()

    # Create musician schema and tables
    cur.execute("CREATE SCHEMA IF NOT EXISTS musician;")

    # Create file_data table with new columns
    cur.execute("""
    CREATE TABLE IF NOT EXISTS musician.file_data (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255),
        full_path TEXT,
        extension VARCHAR(10),
        song_title VARCHAR(255),
        album_name VARCHAR(255),
        album_artist VARCHAR(255),
        genre VARCHAR(255),
        year INT,
        duration REAL,
        taggable BOOLEAN,
        scan_name VARCHAR(255),
        sha256_hash TEXT
    );
    """)

    # Create scans table with new num_taggable column
    cur.execute("""
    CREATE TABLE IF NOT EXISTS musician.scans (
        id SERIAL PRIMARY KEY,
        scan_name VARCHAR(255) UNIQUE,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        num_files INT,
        num_taggable INT,
        num_errors INT
    );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized.")


def calculate_sha256(file_path):
    # Calculate SHA-256 hash
    sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(4096):
                sha256.update(chunk)
        return sha256.hexdigest()
    except OSError as e:
        print(f"Could not hash {file_path}: {e}")
        return None


def walk_and_record(directory, scan_name):
    # Initialize DB connection and cursor
    username = getuser()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user=username
    )
    cur = conn.cursor()

    # Check if scan_name already exists
    cur.execute("SELECT scan_name FROM musician.scans WHERE scan_name = %s;", (scan_name,))
    if cur.fetchone():
        print(f"Scan name {scan_name} already exists.")
        return

    # Initialize scan counters and start_time
    start_time = datetime.now()
    process_count = 0
    taggable_count = 0
    error_count = 0

    cur.execute("""
    INSERT INTO musician.scans (scan_name, start_time)
    VALUES (%s, %s)
    """, (scan_name, start_time))

    # Walk the directory and record file metadata
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            try:
                full_path = os.path.join(dirpath, filename)
                extension = os.path.splitext(filename)[1][1:]
                if extension in {'plist', 'jpg'} or full_path.endswith('.DS_Store'):
                    continue

                sha256_hash = calculate_sha256(full_path)
                tag = None
                taggable = TinyTag.is_supported(full_path)
                if taggable:
                    tag = TinyTag.get(full_path)
                    taggable_count += 1

                year = None
                if tag and tag.year:
                    try:
                        year_tag: str = tag.year.split('-')[0]
                        if year_tag:
                            year_tag = year_tag.replace(' ', '')
                            year = int(year_tag)
                    except (ValueError, TypeError):
                        pass


                cur.execute("""
                INSERT INTO musician.file_data (file_name, full_path, extension, song_title, album_name, album_artist, genre, year, duration, taggable, scan_name, sha256_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    filename,
                    full_path,
                    extension,
                    tag.title if tag else None,
                    tag.album if tag else None,
                    tag.artist if tag else None,
                    tag.genre if tag else None,
                    year,
                    tag.duration if tag else None,
                    taggable,
                    scan_name,
                    sha256_hash
                ))

                conn.commit()
                process_count += 1
                if process_count % 500 == 0:
                    print(f"{process_count} files processed.")
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                error_count += 1

    end_time = datetime.now()
    cur.execute("""
    UPDATE musician.scans
    SET end_time = %s, num_files = %s, num_taggable = %s, num_errors = %s
    WHERE scan_name = %s
    """, (end_time, process_count, taggable_count, error_count, scan_name))
    conn.commit()

    cur.close()
    conn.close()
    print(
        f"Scan complete ({process_count} files, {taggable_count} taggable, {error_count} errors) for directory: {directory}")


def main():
    if __name__ == '__main__':
        # Create a top-level argument parser
        parser = argparse.ArgumentParser(
            description='A CLI tool to manage music files and populate metadata into a database.')

        # Add sub-commands: 'init_db' and 'scan'
        subparsers = parser.add_subparsers(dest='command', help='Available commands')

        # Create a parser for 'init_db'
        parser_init = subparsers.add_parser('init_db',
                                            help='Initialize the database. This command requires no additional flags.')

        # Create a parser for 'scan'
        parser_scan = subparsers.add_parser('scan', help='Scan and populate metadata from a given music directory.')
        parser_scan.add_argument('--path', required=True,
                                 help='The absolute or relative path to the music directory to scan.')
        parser_scan.add_argument('--scan-name', required=True,
                                 help='A unique name for this scanning session for easier identification later.')

        # Parse command-line arguments
        args = parser.parse_args()

        # No command provided
        if args.command is None:
            parser.print_help(sys.stderr)
            sys.exit(1)

        # Execute the command based on user input
        if args.command == 'init_db':
            init_db()
        elif args.command == 'scan':
            if not args.path:
                print("Error: The --path flag must be provided when using the 'scan' command.")
                sys.exit(1)
            walk_and_record(args.path, args.scan_name)


if __name__ == '__main__':
    main()
