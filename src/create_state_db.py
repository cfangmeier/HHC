#!/usr/bin/env python3
import os
import sqlite3
import argparse


def open_state_db(state):
    if not os.path.exists("data"):
        os.mkdir("data")
    fname = "data/data_{}.sqlite3".format(state)
    if os.path.exists(fname):
        os.remove(fname)
    conn = sqlite3.connect(fname)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def open_full_db(path):
    conn = sqlite3.connect(path)
    return conn


def init_state_db(state):
    conn = open_state_db(state)
    conn.executescript('''
    CREATE TABLE Issuer (id_issuer             INTEGER PRIMARY KEY,
                         name                  TEXT    NOT NULL,
                         state                 TEXT    NOT NULL,
                         UNIQUE (id_issuer) ON CONFLICT FAIL);

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY AUTOINCREMENT,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       plan_id_type   TEXT    NOT NULL,
                       marketing_name TEXT,
                       summary_url    TEXT);

    CREATE TABLE Provider (idx_provider    INTEGER PRIMARY KEY AUTOINCREMENT,
                           npi             INTEGER,
                           name            TEXT    NOT NULL,
                           last_updated_on INTEGER NOT NULL,
                           type            INTEGER NOT NULL,
                           accepting       INTEGER NOT NULL);

    CREATE TABLE Address (idx_provider INTEGER NOT NULL,
                          address      TEXT,
                          city         TEXT,
                          state        TEXT,
                          zip          TEXT,
                          phone        TEXT,
                          UNIQUE (idx_provider, address, zip)
                              ON CONFLICT IGNORE,
                          FOREIGN KEY(idx_provider)
                              REFERENCES Provider(idx_provider));

    CREATE TABLE Language (idx_language INTEGER PRIMARY KEY AUTOINCREMENT,
                           language     TEXT    NOT NULL,
                           UNIQUE(language) ON CONFLICT FAIL);

    CREATE TABLE Specialty (idx_specialty INTEGER PRIMARY KEY AUTOINCREMENT,
                            specialty     TEXT    NOT NULL,
                            UNIQUE(specialty) ON CONFLICT FAIL);

    CREATE TABLE FacilityType (idx_facility_type INTEGER
                                   PRIMARY KEY AUTOINCREMENT,
                               facility_type     TEXT    NOT NULL,
                               UNIQUE(facility_type) ON CONFLICT FAIL);

    CREATE TABLE Provider_Language (idx_provider INTEGER NOT NULL,
                                    idx_language INTEGER NOT NULL,
                                    UNIQUE(idx_provider,idx_language)
                                        ON CONFLICT IGNORE
                                    FOREIGN KEY(idx_provider)
                                        REFERENCES Provider(idx_provider),
                                    FOREIGN KEY(idx_language)
                                        REFERENCES Language(idx_language));

    CREATE TABLE Provider_Specialty (idx_provider  INTEGER NOT NULL,
                                     idx_specialty INTEGER NOT NULL,
                                     UNIQUE(idx_provider,idx_specialty)
                                         ON CONFLICT IGNORE,
                                    FOREIGN KEY(idx_provider)
                                        REFERENCES Provider(idx_provider),
                                    FOREIGN KEY(idx_specialty)
                                        REFERENCES Specialty(idx_specialty));


    CREATE TABLE Provider_FacilityType (idx_provider      INTEGER NOT NULL,
                                        idx_facility_type INTEGER NOT NULL,
                                        UNIQUE(idx_provider, idx_facility_type)
                                            ON CONFLICT IGNORE,
                                        FOREIGN KEY(idx_provider)
                                            REFERENCES Provider(idx_provider),
                                        FOREIGN KEY(idx_facility_type)
                                            REFERENCES
                                            FacilityType(idx_facility_type));

    CREATE TABLE Provider_Plan (idx_provider INTEGER NOT NULL,
                                idx_plan     INTEGER NOT NULL,
                                network_tier TEXT    NOT NULL,
                                UNIQUE (idx_provider, idx_plan, network_tier)
                                    ON CONFLICT IGNORE,
                                FOREIGN KEY(idx_provider)
                                    REFERENCES Provider(idx_provider),
                                FOREIGN KEY(idx_plan)
                                    REFERENCES Plan(idx_plan));
    ''')
    return conn


def clone_common_tables(conn_full, conn_state):
    rs = conn_full.execute("SELECT id_issuer, name, state FROM Issuer")
    while True:
        val = rs.fetchone()
        if val is None:
            break
        conn_state.execute("INSERT INTO Issuer VALUES (?,?,?)", val)

    clone_tables = ['Plan', 'Language',
                    'Specialty', 'FacilityType']
    for table in clone_tables:
        sel = "SELECT * FROM {}".format(table)
        rs = conn_full.execute(sel)
        while True:
            val = rs.fetchone()
            if val is None:
                break
            s = ','.join("?"*len(val))
            ins = "INSERT INTO {} VALUES ({});".format(table, s)
            conn_state.execute(ins)
    conn_state.commit()


def main(db_path, state):
    conn_full = open_full_db(db_path)
    conn_state = init_state_db(state)

    clone_common_tables(conn_full, conn_state)

    conn_full.close()
    conn_state.close()


if __name__ == '__main__':
    desc = 'Utility to extract state-specific data'
    parser = argparse.ArgumentParser(description=desc)
    add = parser.add_argument
    add('full_db', help='path to full-data sqlite file', type=str)
    add('state', help='state abbreviation', type=str)
    args = parser.parse_args()
    main(args.full_db, args.state.lower())
