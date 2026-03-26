import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# 1. Connecties opzetten
conn_sdm = sqlite3.connect('SDM.db')
conn_dwh = sqlite3.connect('DWH.db')

print("Start ETL Proces...")

# ==========================================
# EXTRACT & TRANSFORM: DIMENSIES
# ==========================================

# -- Dimensie: Klant (Conformed) --
print("Bouwen van Dim_Klant...")
df_klant_fiets = pd.read_sql("SELECT * FROM Fietsverkoop_Klant", conn_sdm)
df_klant_acc = pd.read_sql("SELECT * FROM Accessoireverkoop_Klant", conn_sdm)
df_klant = pd.concat([df_klant_fiets, df_klant_acc]).drop_duplicates(subset=['klantnr'])

# Afgeleide dimensiewaarde 1: Leeftijdscategorie
# Converteer geboortedatum en bereken leeftijd (we nemen 2024 als huidig jaar voor de dataset)
df_klant['geboortedatum'] = pd.to_datetime(df_klant['geboortedatum'], errors='coerce')
df_klant['leeftijd'] = 2024 - df_klant['geboortedatum'].dt.year
df_klant['Leeftijdscategorie'] = pd.cut(df_klant['leeftijd'], bins=[0, 18, 30, 50, 100], labels=['<18', '18-30', '31-50', '50+'])
# Drop overbodige attributen (Keuze vastleggen voor beoordeling!)
df_klant = df_klant[['klantnr', 'naam', 'woonplaats', 'geslacht', 'Leeftijdscategorie']]

# -- Dimensie: Fiets --
print("Bouwen van Dim_Fiets...")
df_fiets = pd.read_sql("SELECT * FROM Fietsverkoop_Fiets", conn_sdm)
# Afgeleide dimensiewaarde 2: Prijsklasse
df_fiets['Prijsklasse'] = pd.cut(df_fiets['standaardprijs'], bins=[0, 500, 1500, 10000], labels=['Budget', 'Middenklasse', 'Premium'])
df_fiets_dim = df_fiets[['fietsnr', 'soort', 'merk', 'type', 'kleur', 'Prijsklasse']]

# -- Dimensie: Monteur (Conformed) --
print("Bouwen van Dim_Monteur...")
df_mont_fiets = pd.read_sql("SELECT * FROM Fietsverkoop_Monteur", conn_sdm)
df_mont_ond = pd.read_sql("SELECT * FROM Onderhoud_Monteur", conn_sdm)
df_monteur = pd.concat([df_mont_fiets, df_mont_ond]).drop_duplicates(subset=['monteurnr'])
df_monteur = df_monteur[['monteurnr', 'naam', 'woonplaats', 'uurloon']]

# -- Dimensie: Tijd (Verborgen Dimensie) --
print("Bouwen van Dim_Tijd...")
# Verzamel alle unieke datums uit de feitentabellen
df_dates = pd.concat([
    pd.read_sql("SELECT datum FROM Fietsverkoop_Fiets_Verkoop", conn_sdm),
    pd.read_sql("SELECT datum FROM Accessoireverkoop_Accessoire_Verkoop", conn_sdm),
    pd.read_sql("SELECT datum FROM Onderhoud", conn_sdm)
]).drop_duplicates().dropna()

df_dates['datum'] = pd.to_datetime(df_dates['datum'])
df_tijd = pd.DataFrame()
df_tijd['DatumKey'] = df_dates['datum'].dt.strftime('%Y%m%d').astype(int) # Bijv 20241027
df_tijd['Datum'] = df_dates['datum'].dt.date
df_tijd['Jaar'] = df_dates['datum'].dt.year
df_tijd['Maand'] = df_dates['datum'].dt.month
# Afgeleide dimensiewaarde 3: Kwartaal
df_tijd['Kwartaal'] = df_dates['datum'].dt.quarter

# ==========================================
# EXTRACT & TRANSFORM: FEITEN
# ==========================================

# -- Feit 1: Fietsverkoop --
print("Bouwen van Feit_Fietsverkoop...")
df_feit_fiets = pd.read_sql("SELECT * FROM Fietsverkoop_Fiets_Verkoop", conn_sdm)
df_feit_fiets['DatumKey'] = pd.to_datetime(df_feit_fiets['datum']).dt.strftime('%Y%m%d').astype(int)

# Om winst te berekenen, hebben we de inkoopprijs nodig uit de fietstabel
df_feit_fiets = df_feit_fiets.merge(df_fiets[['fietsnr', 'inkoopprijs']], left_on='fiets', right_on='fietsnr', how='left')

# Afgeleide meetwaarde 1: Omzet
df_feit_fiets['Omzet'] = df_feit_fiets['aantal'] * df_feit_fiets['verkoopprijs']
# Afgeleide meetwaarde 2: Bruto Winst
df_feit_fiets['Bruto_Winst'] = df_feit_fiets['Omzet'] - (df_feit_fiets['aantal'] * df_feit_fiets['inkoopprijs'])

df_feit_fiets_final = df_feit_fiets[['fiets_verkoopnr', 'DatumKey', 'klant', 'fiets', 'monteur', 'aantal', 'verkoopprijs', 'Omzet', 'Bruto_Winst']]

# -- Feit 2: Onderhoud --
print("Bouwen van Feit_Onderhoud...")
df_feit_ond = pd.read_sql("SELECT * FROM Onderhoud", conn_sdm)
df_feit_ond['DatumKey'] = pd.to_datetime(df_feit_ond['datum']).dt.strftime('%Y%m%d').astype(int)

# Starttijd en eindtijd omzetten om duur te berekenen
df_feit_ond['starttijd'] = pd.to_datetime(df_feit_ond['starttijd'], format='%H:%M:%S.%f')
df_feit_ond['eindtijd'] = pd.to_datetime(df_feit_ond['eindtijd'], format='%H:%M:%S.%f')

# Afgeleide meetwaarde 3: Onderhoudsduur in Uren
df_feit_ond['Onderhoudsduur_Uren'] = (df_feit_ond['eindtijd'] - df_feit_ond['starttijd']).dt.total_seconds() / 3600

df_feit_ond_final = df_feit_ond[['onderhoudnr', 'DatumKey', 'fiets', 'monteur', 'Onderhoudsduur_Uren']]

# ==========================================
# LOAD: SCHRIJVEN NAAR DWH
# ==========================================
print("Laden naar DWH.db...")

# Dimensies inladen
df_klant.to_sql('Dim_Klant', conn_dwh, if_exists='replace', index=False)
df_fiets_dim.to_sql('Dim_Fiets', conn_dwh, if_exists='replace', index=False)
df_monteur.to_sql('Dim_Monteur', conn_dwh, if_exists='replace', index=False)
df_tijd.to_sql('Dim_Tijd', conn_dwh, if_exists='replace', index=False)

# Feiten inladen
df_feit_fiets_final.to_sql('Feit_Fietsverkoop', conn_dwh, if_exists='replace', index=False)
df_feit_ond_final.to_sql('Feit_Onderhoud', conn_dwh, if_exists='replace', index=False)

# Sluit connecties
conn_sdm.close()
conn_dwh.close()

print("ETL Proces voltooid! Datawarehouse is aangemaakt in DWH.db")