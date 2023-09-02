import os
import sys
import psycopg2
import hashlib
from tinytag import TinyTag
from getpass import getuser
import argparse


def init_db():
    username = getuser()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user=username
    )
    cur = conn.cursor()

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
        year INT,
        scan_name VARCHAR(255),
        sha256_hash TEXT
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized.")


def calculate_sha256(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(4096):
            sha256.update(chunk)
    return sha256.hexdigest()


def walk_and_record(directory, scan_name):
    username = getuser()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user=username
    )
    cur = conn.cursor()
    process_count = 0

    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            extension = os.path.splitext(filename)[1][1:]
            if extension == 'plist':
                continue
            sha256_hash = calculate_sha256(full_path)

            tag = None
            if TinyTag.is_supported(full_path):
                try:
                    tag = TinyTag.get(full_path)
                except Exception as ex:
                    print(f'{full_path}: {ex}')

            year = None
            if tag and tag.year:
                try:
                    # If it's a full date, store only the year
                    year = int(tag.year.split('-')[0])
                except ValueError:
                    # If the year isn't an integer and isn't in the YYYY-MM-DD format, set it to None
                    year = None

            cur.execute("""
            INSERT INTO musician.file_data (file_name, full_path, extension, song_title, album_name, album_artist, year, scan_name, sha256_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                filename,
                full_path,
                extension,
                tag.title if tag else None,
                tag.album if tag else None,
                tag.artist if tag else None,
                year,
                scan_name,
                sha256_hash
            ))
            conn.commit()
            process_count += 1
            # every 500 files, report progress
            if process_count % 500 == 0:
                print(f'{process_count} files processed')

    cur.close()
    conn.close()
    print(f"Scan complete ({process_count} files) for directory: {directory}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage music files and metadata.')
    parser.add_argument('command', choices=['init_db', 'scan'], help='Command to run.')
    parser.add_argument('--path', help='Path to the music folder.')
    parser.add_argument('--scan-name', help='Name of the scan.')

    args = parser.parse_args()

    if args.command == 'init_db':
        init_db()
    elif args.command == 'scan':
        if not args.path:
            print("Usage: python script.py scan --path <path> [--scan-name <scan_name>]")
            sys.exit(1)
        walk_and_record(args.path, args.scan_name)
