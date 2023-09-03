import os
import shutil
from typing import Optional

import psycopg2
import hashlib

from psycopg2.extensions import connection
from tinytag import TinyTag
from getpass import getuser
import argparse
from datetime import datetime


def get_db() -> connection:
    # Initialize the database
    username = getuser()
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user=username
    )


def init_db(conn: connection) -> None:
    cur = conn.cursor()

    # Create musician schema and tables
    cur.execute("CREATE SCHEMA IF NOT EXISTS musician;")
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
    print("Database initialized.")


def calculate_sha256(file_path):
    sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(4096):
                sha256.update(chunk)
        return sha256.hexdigest()
    except OSError as e:
        print(f"Could not hash {file_path}: {e}")
        return None


def walk_and_record(conn: connection, path: str, scan_name: str):
    cur = conn.cursor()

    # Check if scan_name already exists and stop early if it does
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
    for dirpath, dirnames, filenames in os.walk(path):
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
    print(
        f"Scan complete ({process_count} files, {taggable_count} taggable, {error_count} errors) for directory: {path}")


def main():
    if __name__ == '__main__':
        # Create a top-level argument parser
        parser = argparse.ArgumentParser(
            description='A CLI tool to manage music files and populate metadata into a database.')

        # Add sub-commands
        subparsers = parser.add_subparsers(dest='command', help='Available commands')

        # 'init_db' command
        parser_init = subparsers.add_parser('init_db',
                                            help='Initialize the database. This command requires no additional flags.')
        parser_init.set_defaults(func=init_db)

        # 'scan' command
        parser_scan = subparsers.add_parser('scan',
                                            help='Scan and populate metadata from a given music directory.')
        parser_scan.add_argument('--path', required=True,
                                 help='The absolute or relative path to the music directory to scan.')
        parser_scan.add_argument('--scan-name', required=True,
                                 help='A unique name for this scanning session for easier identification later.')
        parser_scan.set_defaults(func=walk_and_record)

        # 'exts' command
        exts_parser = subparsers.add_parser('exts')
        exts_parser.add_argument('--scan-name', help='Scan name to filter by')
        exts_parser.set_defaults(func=list_extensions)

        # 'list_file_data' command
        list_file_data_parser = subparsers.add_parser('list_file_data')
        list_file_data_parser.add_argument('--ext', required=True, help='File extension to filter by')
        list_file_data_parser.add_argument('--limit', type=int, default=25, help='Limit the number of results')
        list_file_data_parser.add_argument('--offset', type=int, default=0, help='Offset for the results')
        list_file_data_parser.set_defaults(func=list_file_data)

        # 'list_diff' command
        list_diff_parser = subparsers.add_parser('list_diff')
        list_diff_parser.add_argument('--origin-scan', required=True, help='Origin scan name')
        list_diff_parser.add_argument('--dest-scan', required=True, help='Destination scan name')
        list_diff_parser.set_defaults(func=list_diff)

        # 'copy_diff_files' command
        copy_diff_files_parser = subparsers.add_parser('copy_diff_files')
        copy_diff_files_parser.add_argument('--origin-scan', required=True, help='Origin scan name')
        copy_diff_files_parser.add_argument('--dest-scan', required=True, help='Destination scan name')
        copy_diff_files_parser.add_argument('--folder-name', required=True, help='Folder name to copy files into')
        copy_diff_files_parser.set_defaults(func=copy_diff_files)

        args = parser.parse_args()

        conn = get_db()

        # Call the appropriate function based on the command
        if args.command:
            func_args = {k: v for k, v in vars(args).items() if k not in ('func', 'command')}
            args.func(conn, **func_args)  # Pass the database connection and other arguments
        conn.close()


def list_extensions(conn: connection, scan_name: Optional[str] = None) -> None:
    cur = conn.cursor()
    # Check if the scan exists
    if scan_name:
        cur.execute("SELECT COUNT(*) FROM musician.file_data WHERE scan_name = %s;", (scan_name,))
        count = cur.fetchone()[0]
        if count == 0:
            print(f"No records found for scan_name: {scan_name}")
            return

    query = '''
    SELECT extension, COUNT(*)
    FROM musician.file_data
    WHERE CASE WHEN %s IS NOT NULL THEN scan_name = %s ELSE TRUE END
    GROUP BY extension
    ORDER BY COUNT(*) DESC;
    '''
    cur.execute(query, (scan_name, scan_name))
    results = cur.fetchall()

    # Display results
    for extension, count in results:
        print(f"\t{extension}\t\t{count}")


def list_file_data(conn: connection, ext: str, limit: int = 25, offset: int = 0) -> None:
    cur = conn.cursor()

    # Execute modified SQL query
    query = '''
    SELECT *
    FROM musician.file_data
    WHERE extension = %s
    ORDER BY file_name DESC
    LIMIT %s OFFSET %s;
    '''
    cur.execute(query, (ext, limit, offset))
    results = cur.fetchall()

    # Display results
    for row in results:
        print("\t".join(str(col) for col in row))
        print()


def list_diff(conn: connection, origin_scan: str, dest_scan: str) -> None:
    cur = conn.cursor()

    query = '''
    SELECT count(origin.*)
    FROM musician.file_data origin
    LEFT JOIN musician.file_data dest
      ON CONCAT(COALESCE(origin.song_title, origin.file_name), origin.album_name) = CONCAT(COALESCE(dest.song_title, dest.file_name), dest.album_name)
      AND dest.scan_name = %s
    WHERE origin.scan_name = %s
      AND dest.id IS NULL;
    '''
    cur.execute(query, (dest_scan, origin_scan))
    result = cur.fetchone()[0]

    # Display result
    print(f"Different files count between {origin_scan} and {dest_scan}: {result}")


def copy_diff_files(conn: connection, origin_scan: str, dest_scan: str, folder_name: str) -> None:
    cur = conn.cursor()
    os.makedirs(folder_name, exist_ok=True)

    query = '''
    SELECT origin.full_path
    FROM musician.file_data origin
    LEFT JOIN musician.file_data dest
      ON CONCAT(COALESCE(origin.song_title, origin.file_name), origin.album_name) = CONCAT(COALESCE(dest.song_title, dest.file_name), dest.album_name)
      AND dest.scan_name = %s
    WHERE origin.scan_name = %s
      AND dest.id IS NULL;
    '''
    cur.execute(query, (dest_scan, origin_scan))
    results = cur.fetchall()

    total_files = len(results)
    copied_files = 0

    for source_path, in results:
        dest_path = folder_name

        # Copy the file if it exists in the source directory
        if os.path.exists(source_path):
            shutil.copy2(source_path, dest_path)
        else:
            print(f"File {source_path} not found in source directory")

        copied_files += 1
        if copied_files % 250 == 0:
            percentage = (copied_files / total_files) * 100
            print(f"{percentage:.2f}%: {copied_files} files out of {total_files} copied")

    print(f"done: {copied_files} files out of {total_files} copied")


if __name__ == '__main__':
    main()
