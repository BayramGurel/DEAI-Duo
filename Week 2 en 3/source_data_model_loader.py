from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable

BASE_DIR = Path(__file__).resolve().parent
SDM_PATH = BASE_DIR / 'SDM.db'
SCHEMA_PATH = BASE_DIR / 'BikeToDrive_RIM - SDM.txt'
LOG_PATH = BASE_DIR / 'sdm_etl.log'

SOURCE_DBS = {
    'accessoire_inkoop': BASE_DIR / 'BikeToDrive_4_Accessoire_Inkoop.db',
    'accessoireverkoop': BASE_DIR / 'BikeToDrive_1_Accessoireverkoop.db',
    'onderhoud': BASE_DIR / 'BikeToDrive_3_Onderhoud.db',
    'fiets_inkoop': BASE_DIR / 'BikeToDrive_5_Fiets_Inkoop.db',
    'fietsverkoop': BASE_DIR / 'BikeToDrive_2_Fietsverkoop.db',
}

TABLE_MAPPINGS = [
    ('accessoire_inkoop', 'Leverancier', 'Accessoire_Inkoop_Leverancier', ('leveranciernr',)),
    ('accessoire_inkoop', 'Accessoire', 'Accessoire_Inkoop_Accessoire', ('accessoirenr',)),
    ('accessoire_inkoop', 'Accessoire_Inkoop', 'Accessoire_Inkoop', ('inkoopnr',)),
    ('accessoireverkoop', 'Filiaal', 'Accessoireverkoop_Filiaal', ('filiaalnr',)),
    ('accessoireverkoop', 'Leverancier', 'Accessoireverkoop_Leverancier', ('leveranciernr',)),
    ('accessoireverkoop', 'Klant', 'Accessoireverkoop_Klant', ('klantnr',)),
    ('accessoireverkoop', 'Monteur', 'Accessoireverkoop_Monteur', ('monteurnr',)),
    ('accessoireverkoop', 'Accessoire', 'Accessoireverkoop_Accessoire', ('accessoirenr',)),
    ('accessoireverkoop', 'Accessoire_Verkoop', 'Accessoireverkoop_Accessoire_Verkoop', ('accessoire_verkoopnr',)),
    ('onderhoud', 'Fabrikant', 'Onderhoud_Fabrikant', ('fabrikantnr',)),
    ('onderhoud', 'Filiaal', 'Onderhoud_Filiaal', ('filiaalnr',)),
    ('onderhoud', 'Fiets', 'Onderhoud_Fiets', ('fietsnr',)),
    ('onderhoud', 'Monteur', 'Onderhoud_Monteur', ('monteurnr',)),
    ('onderhoud', 'Onderhoud', 'Onderhoud', ('onderhoudnr',)),
    ('fiets_inkoop', 'Fabrikant', 'Fiets_Inkoop_Fabrikant', ('fabrikantnr',)),
    ('fiets_inkoop', 'Fiets', 'Fiets_Inkoop_Fiets', ('fietsnr',)),
    ('fiets_inkoop', 'Fiets_Inkoop', 'Fiets_Inkoop', ('inkoopnr',)),
    ('fietsverkoop', 'Filiaal', 'Fietsverkoop_Filiaal', ('filiaalnr',)),
    ('fietsverkoop', 'Klant', 'Fietsverkoop_Klant', ('klantnr',)),
    ('fietsverkoop', 'Fabrikant', 'Fietsverkoop_Fabrikant', ('fabrikantnr',)),
    ('fietsverkoop', 'Monteur', 'Fietsverkoop_Monteur', ('monteurnr',)),
    ('fietsverkoop', 'Fiets', 'Fietsverkoop_Fiets', ('fietsnr',)),
    ('fietsverkoop', 'Fiets_Verkoop', 'Fietsverkoop_Fiets_Verkoop', ('fiets_verkoopnr',)),
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_PATH, encoding='utf-8'), logging.StreamHandler()],
)


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f'PRAGMA table_info({quote(table)})').fetchall()
    return [row[1] for row in rows]


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?"
    return conn.execute(sql, (table,)).fetchone() is not None


def ensure_files_exist(paths: Iterable[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError('Ontbrekende bestanden:\n- ' + '\n- '.join(missing))


def ensure_sdm_schema(conn: sqlite3.Connection) -> None:
    if table_exists(conn, TABLE_MAPPINGS[0][2]):
        return
    logging.info('SDM-schema ontbreekt; schema wordt opnieuw opgebouwd vanuit %s', SCHEMA_PATH.name)
    conn.executescript(SCHEMA_PATH.read_text(encoding='utf-8'))
    conn.commit()


def reset_sdm(conn: sqlite3.Connection) -> None:
    conn.execute('PRAGMA foreign_keys = OFF')
    try:
        for _, _, target_table, _ in reversed(TABLE_MAPPINGS):
            if table_exists(conn, target_table):
                conn.execute(f'DELETE FROM {quote(target_table)}')
        conn.commit()
    finally:
        conn.execute('PRAGMA foreign_keys = ON')
    logging.info('Reset van SDM is klaar.')


def stage_source(conn: sqlite3.Connection, source_conn: sqlite3.Connection, source_table: str, target_table: str) -> tuple[str, list[str]]:
    target_columns = columns(conn, target_table)
    source_columns = columns(source_conn, source_table)
    if target_columns != source_columns:
        raise ValueError(
            f'Kolommen komen niet overeen voor {source_table} -> {target_table}: '
            f'{source_columns} != {target_columns}'
        )

    temp_table = f'tmp_{target_table}'
    column_sql = ', '.join(quote(column) for column in target_columns)
    placeholders = ', '.join('?' for _ in target_columns)

    conn.execute(f'DROP TABLE IF EXISTS {quote(temp_table)}')
    conn.execute(
        f'CREATE TEMP TABLE {quote(temp_table)} AS '
        f'SELECT {column_sql} FROM {quote(target_table)} WHERE 1 = 0'
    )

    rows = source_conn.execute(f'SELECT {column_sql} FROM {quote(source_table)}').fetchall()
    if rows:
        conn.executemany(
            f'INSERT INTO {quote(temp_table)} ({column_sql}) VALUES ({placeholders})',
            rows,
        )
    return temp_table, target_columns


def join_condition(left_alias: str, right_alias: str, keys: tuple[str, ...]) -> str:
    return ' AND '.join(f'{left_alias}.{quote(key)} = {right_alias}.{quote(key)}' for key in keys)


def sync_table(conn: sqlite3.Connection, source_conn: sqlite3.Connection, mapping: tuple[str, str, str, tuple[str, ...]]) -> None:
    source_db, source_table, target_table, keys = mapping
    logging.info('Start sync: %s.%s -> %s', source_db, source_table, target_table)

    temp_table, all_columns = stage_source(conn, source_conn, source_table, target_table)
    non_keys = [column for column in all_columns if column not in keys]
    column_sql = ', '.join(quote(column) for column in all_columns)
    staged_sql = ', '.join(f'bron.{quote(column)}' for column in all_columns)
    match_sql = join_condition('bron', 'doel', keys)

    conn.execute(f'''
        INSERT INTO {quote(target_table)} ({column_sql})
        SELECT {staged_sql}
        FROM {quote(temp_table)} AS bron
        WHERE NOT EXISTS (
            SELECT 1 FROM {quote(target_table)} AS doel WHERE {match_sql}
        )
    ''')

    if non_keys:
        set_sql = ', '.join(
            f'{quote(column)} = (SELECT bron.{quote(column)} FROM {quote(temp_table)} AS bron WHERE {match_sql})'
            for column in non_keys
        )
        diff_sql = ' OR '.join(
            f'doel.{quote(column)} IS NOT (SELECT bron.{quote(column)} FROM {quote(temp_table)} AS bron WHERE {match_sql})'
            for column in non_keys
        )
        conn.execute(f'''
            UPDATE {quote(target_table)} AS doel
            SET {set_sql}
            WHERE EXISTS (SELECT 1 FROM {quote(temp_table)} AS bron WHERE {match_sql})
              AND ({diff_sql})
        ''')

    conn.execute(f'''
        DELETE FROM {quote(target_table)} AS doel
        WHERE NOT EXISTS (
            SELECT 1 FROM {quote(temp_table)} AS bron WHERE {match_sql}
        )
    ''')
    conn.execute(f'DROP TABLE IF EXISTS {quote(temp_table)}')
    conn.commit()


def load_all_sources(reset_first: bool = False) -> None:
    ensure_files_exist([SCHEMA_PATH, SDM_PATH, *SOURCE_DBS.values()])

    with sqlite3.connect(SDM_PATH) as sdm_conn:
        sdm_conn.execute('PRAGMA foreign_keys = ON')
        ensure_sdm_schema(sdm_conn)
        if reset_first:
            reset_sdm(sdm_conn)

        source_conns = {name: sqlite3.connect(path) for name, path in SOURCE_DBS.items()}
        try:
            for mapping in TABLE_MAPPINGS:
                sync_table(sdm_conn, source_conns[mapping[0]], mapping)
        finally:
            for source_conn in source_conns.values():
                source_conn.close()

    logging.info('Alle databronnen zijn succesvol naar het SDM gesynchroniseerd.')


def count_rows() -> list[tuple[str, int]]:
    with sqlite3.connect(SDM_PATH) as conn:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        return [(table, conn.execute(f'SELECT COUNT(*) FROM {quote(table)}').fetchone()[0]) for table in tables]


def print_counts() -> None:
    for table, total in count_rows():
        print(f'{table}: {total} rijen')


if __name__ == '__main__':
    load_all_sources(reset_first=True)
    print_counts()
