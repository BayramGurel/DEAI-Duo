import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Tuple

# ============================================================
# SOURCE DATA MODEL - VULLEN VAN SDM.db
# ------------------------------------------------------------
# Dit script is bedoeld voor gebruik in Jupyter Notebook of als
# los Python-bestand.
#
# Gekozen inlaadstrategieën:
# 1. String-built SQL
#    - gebruikt voor resetten en voor dynamisch opbouwen van SQL
# 2. Incremental Data Loading
#    - gebruikt om inserts, updates en deletes uit de bronnen
#      over te nemen naar het SDM
#
# Het script kan:
# a. Alle tabellen in SDM.db leegmaken (reset-knop)
# b. Data uit alle .db-bestanden overzetten naar SDM.db
# c. Inserts, updates en deletes in de bron synchroniseren
#
# Belangrijk:
# - Het schema van SDM.db moet al bestaan.
# - De kolommen van bron- en doeltabel moeten inhoudelijk gelijk zijn.
# ============================================================

# -----------------------------
# Configuratie
# -----------------------------
SDM_PATH = "SDM.db"

SOURCE_DBS: Dict[str, str] = {
    "accessoireverkoop": "BikeToDrive_1_Accessoireverkoop.db",
    "fietsverkoop": "BikeToDrive_2_Fietsverkoop.db",
    "onderhoud": "BikeToDrive_3_Onderhoud.db",
    "accessoire_inkoop": "BikeToDrive_4_Accessoire_Inkoop.db",
    "fiets_inkoop": "BikeToDrive_5_Fiets_Inkoop.db",
}

# Laadvolgorde:
# eerst oudertabellen, daarna kindtabellen.
# De pk-kolommen zijn hier de logische sleutels waarmee we records matchen.
TABLE_MAPPINGS: List[Dict[str, object]] = [
    # Accessoire Inkoop
    {"source_db": "accessoire_inkoop", "source_table": "Leverancier", "target_table": "Accessoire_Inkoop_Leverancier", "pk": ["leveranciernr"]},
    {"source_db": "accessoire_inkoop", "source_table": "Accessoire", "target_table": "Accessoire_Inkoop_Accessoire", "pk": ["accessoirenr"]},
    {"source_db": "accessoire_inkoop", "source_table": "Accessoire_Inkoop", "target_table": "Accessoire_Inkoop", "pk": ["inkoopnr"]},

    # Accessoireverkoop
    {"source_db": "accessoireverkoop", "source_table": "Filiaal", "target_table": "Accessoireverkoop_Filiaal", "pk": ["filiaalnr"]},
    {"source_db": "accessoireverkoop", "source_table": "Leverancier", "target_table": "Accessoireverkoop_Leverancier", "pk": ["leveranciernr"]},
    {"source_db": "accessoireverkoop", "source_table": "Klant", "target_table": "Accessoireverkoop_Klant", "pk": ["klantnr"]},
    {"source_db": "accessoireverkoop", "source_table": "Monteur", "target_table": "Accessoireverkoop_Monteur", "pk": ["monteurnr"]},
    {"source_db": "accessoireverkoop", "source_table": "Accessoire", "target_table": "Accessoireverkoop_Accessoire", "pk": ["accessoirenr"]},
    {"source_db": "accessoireverkoop", "source_table": "Accessoire_Verkoop", "target_table": "Accessoireverkoop_Accessoire_Verkoop", "pk": ["accessoire_verkoopnr"]},

    # Onderhoud
    {"source_db": "onderhoud", "source_table": "Fabrikant", "target_table": "Onderhoud_Fabrikant", "pk": ["fabrikantnr"]},
    {"source_db": "onderhoud", "source_table": "Filiaal", "target_table": "Onderhoud_Filiaal", "pk": ["filiaalnr"]},
    {"source_db": "onderhoud", "source_table": "Fiets", "target_table": "Onderhoud_Fiets", "pk": ["fietsnr"]},
    {"source_db": "onderhoud", "source_table": "Monteur", "target_table": "Onderhoud_Monteur", "pk": ["monteurnr"]},
    {"source_db": "onderhoud", "source_table": "Onderhoud", "target_table": "Onderhoud", "pk": ["onderhoudnr"]},

    # Fiets Inkoop
    {"source_db": "fiets_inkoop", "source_table": "Fabrikant", "target_table": "Fiets_Inkoop_Fabrikant", "pk": ["fabrikantnr"]},
    {"source_db": "fiets_inkoop", "source_table": "Fiets", "target_table": "Fiets_Inkoop_Fiets", "pk": ["fietsnr"]},
    {"source_db": "fiets_inkoop", "source_table": "Fiets_Inkoop", "target_table": "Fiets_Inkoop", "pk": ["inkoopnr"]},

    # Fietsverkoop
    {"source_db": "fietsverkoop", "source_table": "Filiaal", "target_table": "Fietsverkoop_Filiaal", "pk": ["filiaalnr"]},
    {"source_db": "fietsverkoop", "source_table": "Klant", "target_table": "Fietsverkoop_Klant", "pk": ["klantnr"]},
    {"source_db": "fietsverkoop", "source_table": "Fabrikant", "target_table": "Fietsverkoop_Fabrikant", "pk": ["fabrikantnr"]},
    {"source_db": "fietsverkoop", "source_table": "Monteur", "target_table": "Fietsverkoop_Monteur", "pk": ["monteurnr"]},
    {"source_db": "fietsverkoop", "source_table": "Fiets", "target_table": "Fietsverkoop_Fiets", "pk": ["fietsnr"]},
    {"source_db": "fietsverkoop", "source_table": "Fiets_Verkoop", "target_table": "Fietsverkoop_Fiets_Verkoop", "pk": ["fiets_verkoopnr"]},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_proces.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


# ============================================================
# HULPFUNCTIES
# ============================================================

def q(identifier: str) -> str:
    """
    Zet tabel- of kolomnamen veilig tussen quotes.
    Dit hoort bij de strategie 'String-built SQL'.
    """
    return '"' + identifier.replace('"', '""') + '"'


def qcol(alias: str, column: str) -> str:
    """
    Bouwt een kolomreferentie op met alias, bijvoorbeeld:
    doel."klantnr"
    """
    return f'{alias}.{q(column)}'


def get_user_tables(conn: sqlite3.Connection) -> List[str]:
    """
    Geeft alle gewone tabellen terug uit de database.
    """
    sql = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """
    return [row[0] for row in conn.execute(sql).fetchall()]


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """
    Leest alle kolommen van een tabel uit.
    """
    sql = f"PRAGMA table_info({q(table_name)});"
    rows = conn.execute(sql).fetchall()
    columns = [row[1] for row in rows]

    if not columns:
        raise ValueError(f"Tabel '{table_name}' bestaat niet.")

    return columns


def controleer_bestanden() -> None:
    """
    Controleert of SDM.db en alle brondatabases aanwezig zijn.
    """
    alle_bestanden = [SDM_PATH, *SOURCE_DBS.values()]

    for db_pad in alle_bestanden:
        if not Path(db_pad).exists():
            raise FileNotFoundError(f"Bestand niet gevonden: {db_pad}")


# ============================================================
# 5a. RESET-KNOP
# Strategie: String-built SQL
# ============================================================

def reset_sdm(sdm_conn: sqlite3.Connection) -> None:
    """
    Maakt alle tabellen in het SDM leeg.

    We lopen in omgekeerde volgorde door de mappings heen.
    Daardoor worden child-tabellen eerst geleegd en daarna
    parent-tabellen. Dat is veiliger als er foreign keys zijn.
    """
    bestaande_tabellen = set(get_user_tables(sdm_conn))
    reset_volgorde = [m["target_table"] for m in reversed(TABLE_MAPPINGS)]

    sdm_conn.execute("PRAGMA foreign_keys = OFF;")

    try:
        for tabel in reset_volgorde:
            if tabel in bestaande_tabellen:
                sql = f"DELETE FROM {q(tabel)};"
                logging.info(f"Reset -> {sql}")
                sdm_conn.execute(sql)
            else:
                logging.warning(f"Tabel bestaat niet in SDM en wordt overgeslagen: {tabel}")

        sdm_conn.commit()
        logging.info("Reset van SDM is klaar.")
    finally:
        sdm_conn.execute("PRAGMA foreign_keys = ON;")


# ============================================================
# 5b. DATA OVERZETTEN NAAR SDM
# 6. TESTEN VAN INSERTS / UPDATES / DELETES
# Strategie: Incremental Data Loading
# ============================================================

def stage_source_in_temp_table(
    sdm_conn: sqlite3.Connection,
    source_conn: sqlite3.Connection,
    source_table: str,
    target_table: str
) -> Tuple[str, List[str]]:
    """
    Laadt eerst de brondata in een tijdelijke tabel binnen het SDM.

    Waarom dit handig is:
    - daarna kunnen we in SQL bepalen welke records nieuw zijn
    - welke gewijzigd zijn
    - en welke verwijderd moeten worden
    """
    target_columns = get_table_columns(sdm_conn, target_table)
    source_columns = get_table_columns(source_conn, source_table)

    # Dit script gaat ervan uit dat de kolommen inhoudelijk gelijk zijn
    # tussen bron- en doeltabel.
    if source_columns != target_columns:
        raise ValueError(
            f"Kolommen komen niet overeen:\n"
            f"Bron ({source_table}): {source_columns}\n"
            f"Doel ({target_table}): {target_columns}"
        )

    temp_table = f"tmp_{target_table}"
    column_list = ", ".join(q(col) for col in target_columns)

    # Oude temp-tabel verwijderen als die nog bestaat
    sdm_conn.execute(f"DROP TABLE IF EXISTS {q(temp_table)};")

    # Nieuwe temp-tabel maken met exact dezelfde kolommen als de doeltabel
    create_temp_sql = (
        f"CREATE TEMP TABLE {q(temp_table)} AS "
        f"SELECT {column_list} FROM {q(target_table)} WHERE 1 = 0;"
    )
    logging.info(f"Stage create -> {create_temp_sql}")
    sdm_conn.execute(create_temp_sql)

    # Brondata ophalen
    select_source_sql = f"SELECT {column_list} FROM {q(source_table)};"
    bron_rijen = source_conn.execute(select_source_sql).fetchall()

    # Brondata in de temp-tabel zetten
    if bron_rijen:
        placeholders = ", ".join(["?"] * len(target_columns))
        insert_temp_sql = (
            f"INSERT INTO {q(temp_table)} ({column_list}) "
            f"VALUES ({placeholders});"
        )
        sdm_conn.executemany(insert_temp_sql, bron_rijen)

    return temp_table, target_columns


def build_match_condition(left_alias: str, right_alias: str, pk_columns: List[str]) -> str:
    """
    Bouwt een join-conditie op basis van de primaire sleutelkolommen.
    """
    return " AND ".join(
        f"{qcol(left_alias, pk)} = {qcol(right_alias, pk)}"
        for pk in pk_columns
    )


def incremental_sync_table(
    sdm_conn: sqlite3.Connection,
    source_conn: sqlite3.Connection,
    mapping: Dict[str, object]
) -> None:
    """
    Synchroniseert exact één tabel met Incremental Data Loading.

    Wat gebeurt er:
    1. Nieuwe records worden toegevoegd
    2. Gewijzigde records worden bijgewerkt
    3. Verwijderde records worden ook uit het SDM verwijderd

    Hiermee kun je stap 6 aantonen:
    add / update / delete in de bron -> ook zichtbaar in het SDM
    """
    source_table = str(mapping["source_table"])
    target_table = str(mapping["target_table"])
    pk_columns = list(mapping["pk"])

    temp_table, all_columns = stage_source_in_temp_table(
        sdm_conn=sdm_conn,
        source_conn=source_conn,
        source_table=source_table,
        target_table=target_table
    )

    non_pk_columns = [col for col in all_columns if col not in pk_columns]

    col_list = ", ".join(q(col) for col in all_columns)
    select_temp_cols = ", ".join(qcol("bron", col) for col in all_columns)

    match_bron_doel = build_match_condition("bron", "doel", pk_columns)

    # --------------------------------------------------------
    # A. Nieuwe records toevoegen
    # --------------------------------------------------------
    insert_new_sql = f"""
        INSERT INTO {q(target_table)} ({col_list})
        SELECT {select_temp_cols}
        FROM {q(temp_table)} AS bron
        WHERE NOT EXISTS (
            SELECT 1
            FROM {q(target_table)} AS doel
            WHERE {match_bron_doel}
        );
    """
    logging.info(f"Insert new -> {target_table}")
    sdm_conn.execute(insert_new_sql)

    # --------------------------------------------------------
    # B. Gewijzigde records updaten
    # --------------------------------------------------------
    if non_pk_columns:
        # Voor iedere niet-PK kolom bouwen we een update-expressie.
        set_clause = ", ".join(
            f'{q(col)} = ('
            f'SELECT {qcol("bron", col)} '
            f'FROM {q(temp_table)} AS bron '
            f'WHERE {match_bron_doel}'
            f')'
            for col in non_pk_columns
        )

        # Alleen updaten als minstens één niet-PK kolom anders is.
        # "IS NOT" werkt in SQLite netjes met NULL-waarden.
        diff_clause = " OR ".join(
            f'{qcol("doel", col)} IS NOT ('
            f'SELECT {qcol("bron", col)} '
            f'FROM {q(temp_table)} AS bron '
            f'WHERE {match_bron_doel}'
            f')'
            for col in non_pk_columns
        )

        update_sql = f"""
            UPDATE {q(target_table)} AS doel
            SET {set_clause}
            WHERE EXISTS (
                SELECT 1
                FROM {q(temp_table)} AS bron
                WHERE {match_bron_doel}
            )
              AND ({diff_clause});
        """
        logging.info(f"Update changed -> {target_table}")
        sdm_conn.execute(update_sql)

    # --------------------------------------------------------
    # C. Records verwijderen die niet meer in de bron voorkomen
    # --------------------------------------------------------
    delete_removed_sql = f"""
        DELETE FROM {q(target_table)} AS doel
        WHERE NOT EXISTS (
            SELECT 1
            FROM {q(temp_table)} AS bron
            WHERE {match_bron_doel}
        );
    """
    logging.info(f"Delete removed -> {target_table}")
    sdm_conn.execute(delete_removed_sql)

    # Temp-tabel opruimen
    sdm_conn.execute(f"DROP TABLE IF EXISTS {q(temp_table)};")
    sdm_conn.commit()

    logging.info(f"Incremental sync klaar voor {source_table} -> {target_table}")


# ============================================================
# HOOFDFUNCTIES
# ============================================================

def laad_alle_bronnen_incremental(reset_eerst: bool = False) -> None:
    """
    Hoofdfunctie.

    reset_eerst = True:
        eerst SDM leegmaken, daarna alles opnieuw inladen
    reset_eerst = False:
        alleen incremental synchroniseren
    """
    controleer_bestanden()

    sdm_conn = sqlite3.connect(SDM_PATH)
    sdm_conn.execute("PRAGMA foreign_keys = ON;")

    source_conns = {
        naam: sqlite3.connect(pad)
        for naam, pad in SOURCE_DBS.items()
    }

    try:
        if reset_eerst:
            reset_sdm(sdm_conn)

        # Deze volgorde is gekozen zodat ouder-tabellen eerder worden geladen
        # dan kind-tabellen. Dit helpt bij database-overschrijdende associaties
        # en foreign key-afhankelijkheden.
        for mapping in TABLE_MAPPINGS:
            bron_conn = source_conns[str(mapping["source_db"])]

            logging.info(
                f'Start sync: {mapping["source_db"]}.{mapping["source_table"]} '
                f'-> {mapping["target_table"]}'
            )

            incremental_sync_table(
                sdm_conn=sdm_conn,
                source_conn=bron_conn,
                mapping=mapping
            )

        logging.info("Alle databronnen zijn succesvol naar het SDM gesynchroniseerd.")

    finally:
        for conn in source_conns.values():
            conn.close()
        sdm_conn.close()
        logging.info("Alle databaseverbindingen zijn gesloten.")


def toon_aantallen() -> None:
    """
    Handige controlefunctie om snel te zien hoeveel rijen elke SDM-tabel bevat.
    """
    with sqlite3.connect(SDM_PATH) as conn:
        for tabel in get_user_tables(conn):
            aantal = conn.execute(f"SELECT COUNT(*) FROM {q(tabel)};").fetchone()[0]
            print(f"{tabel}: {aantal} rijen")


# ============================================================
# UITVOERING
# ============================================================
if __name__ == "__main__":
    # Stap 5:
    # Eerst het SDM volledig resetten en daarna opnieuw vullen.
    laad_alle_bronnen_incremental(reset_eerst=True)

    # Controle van de aantallen.
    toon_aantallen()

    # Stap 6:
    # Na het aanpassen van rijen in een bronbestand kun je alleen dit opnieuw draaien:
    # laad_alle_bronnen_incremental(reset_eerst=False)
