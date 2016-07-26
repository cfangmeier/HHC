#!/usr/bin/env python3
import os
import sqlite3
import argparse
from collections import defaultdict


def open_clean_db():
    if not os.path.exists("data"):
        os.mkdir("data")
    fname = "data/data_clean.sqlite3"
    if os.path.exists(fname):
        os.remove(fname)
    conn = sqlite3.connect(fname)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def open_full_db(path):
    conn = sqlite3.connect(path)
    return conn


def init_clean_db():
    conn = open_clean_db()
    conn.executescript('''
    CREATE TABLE Issuer (id_issuer INTEGER PRIMARY KEY,
                         name      TEXT    NOT NULL,
                         state     TEXT    NOT NULL,
                         UNIQUE (id_issuer) ON CONFLICT FAIL);

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       marketing_name TEXT,
                       summary_url    TEXT,
                       FOREIGN KEY(id_issuer)
                           REFERENCES Issuer(id_issuer));

    CREATE TABLE Provider (npi             INTEGER PRIMARY KEY,
                           name            TEXT    NOT NULL,
                           type            INTEGER NOT NULL,
                           accepting       INTEGER NOT NULL);

    CREATE TABLE Address (npi             INTEGER NOT NULL,
                          address         TEXT,
                          city            TEXT,
                          state           TEXT,
                          zip             TEXT,
                          phone           TEXT,
                          FOREIGN KEY(npi)
                              REFERENCES Provider(npi));

    CREATE TABLE Language (idx_language INTEGER PRIMARY KEY,
                           language     TEXT    NOT NULL,
                           UNIQUE(language) ON CONFLICT FAIL);

    CREATE TABLE Specialty (idx_specialty INTEGER PRIMARY KEY,
                            specialty     TEXT    NOT NULL,
                            UNIQUE(specialty) ON CONFLICT FAIL);

    CREATE TABLE FacilityType (idx_facility_type INTEGER PRIMARY KEY,
                               facility_type     TEXT    NOT NULL,
                               UNIQUE(facility_type) ON CONFLICT FAIL);

    CREATE TABLE Provider_Language (npi          INTEGER NOT NULL,
                                    idx_language INTEGER NOT NULL,
                                    UNIQUE(npi,idx_language)
                                        ON CONFLICT IGNORE
                                    FOREIGN KEY(npi)
                                        REFERENCES Provider(npi),
                                    FOREIGN KEY(idx_language)
                                        REFERENCES Language(idx_language));

    CREATE TABLE Provider_Specialty (npi           INTEGER NOT NULL,
                                     idx_specialty INTEGER NOT NULL,
                                     UNIQUE(npi,idx_specialty)
                                         ON CONFLICT IGNORE,
                                    FOREIGN KEY(npi)
                                        REFERENCES Provider(npi),
                                    FOREIGN KEY(idx_specialty)
                                        REFERENCES Specialty(idx_specialty));


    CREATE TABLE Provider_FacilityType (npi               INTEGER NOT NULL,
                                        idx_facility_type INTEGER NOT NULL,
                                        UNIQUE(npi, idx_facility_type)
                                            ON CONFLICT IGNORE,
                                        FOREIGN KEY(npi)
                                            REFERENCES Provider(npi),
                                        FOREIGN KEY(idx_facility_type)
                                            REFERENCES
                                            FacilityType(idx_facility_type));

    CREATE TABLE Provider_Plan (npi             INTEGER NOT NULL,
                                idx_plan        INTEGER NOT NULL,
                                network_tier    TEXT    NOT NULL,
                                last_updated_on INTEGER NOT NULL,
                                FOREIGN KEY(npi)
                                    REFERENCES Provider(npi),
                                FOREIGN KEY(idx_plan)
                                    REFERENCES Plan(idx_plan));

    CREATE TABLE Drug (rxnorm_id INTEGER PRIMARY KEY,
                       drug_name TEXT    NOT NULL);

    CREATE TABLE Drug_Plan (rxnorm_id           INTEGER NOT NULL,
                            idx_plan            INTEGER NOT NULL,
                            drug_tier           TEXT,
                            prior_authorization INTEGER,
                            step_therapy        INTEGER,
                            quantity_limit      INTEGER,
                            FOREIGN KEY(rxnorm_id)
                                REFERENCES Drug(rxnorm_id),
                            FOREIGN KEY(idx_plan)
                                REFERENCES Plan(idx_plan));
    ''')
    return conn


def copy_common_tables(conn_full, conn_state):
    """ copies the tables that can be duplicated verbatim
    """
    ##################
    # Issuer
    ##################
    query = "SELECT id_issuer, name, state FROM Issuer"
    for row in conn_full.execute(query).fetchall():
        conn_state.execute(("INSERT INTO Issuer (id_issuer, name, state) "
                            "VALUES (?,?,?)"), row)
    ##################
    # Plan
    ##################
    query = ("SELECT idx_plan, id_plan, id_issuer, "
             "marketing_name, summary_url "
             "FROM Plan")
    for row in conn_full.execute(query).fetchall():
        query = ("INSERT INTO Plan "
                 "(idx_plan, id_plan, id_issuer, "
                 "marketing_name, summary_url) "
                 "VALUES (?,?,?,?,?)")
        conn_state.execute(query, row)
    ##################
    # Language
    ##################
    query = "SELECT idx_language, language FROM Language"
    for row in conn_full.execute(query).fetchall():
        conn_state.execute(("INSERT INTO Language "
                            "(idx_language, language) "
                            "VALUES (?,?)"), row)
    ##################
    # Specialty
    ##################
    query = "SELECT idx_specialty, specialty FROM Specialty"
    for row in conn_full.execute(query).fetchall():
        conn_state.execute(("INSERT INTO Specialty "
                            "(idx_specialty, specialty) "
                            "VALUES (?,?)"), row)
    ##################
    # Facility Type
    ##################
    query = "SELECT idx_facility_type, facility_type FROM FacilityType"
    for row in conn_full.execute(query).fetchall():
        conn_state.execute(("INSERT INTO FacilityType "
                            "(idx_facility_type, facility_type) "
                            "VALUES (?,?)"), row)
    conn_state.commit()


def copy_providers(conn_full, conn_clean):
    """ copies the provider data
    """
    def most_recent_set(provs):
        """ Get all providers with most recently updated information
        """
        provs.sort(key=lambda x: x[2], reverse=True)
        provs_most_recent = []
        time_most_recent = provs[0][2]
        for prov in provs:
            if prov[2] != time_most_recent:
                break
            provs_most_recent.append(prov)
        return provs_most_recent, time_most_recent

    def copy_provider_plans(npi, provs):
        # split providers into groups originating from each issuer
        prov_groups = defaultdict(list)
        for prov in provs:
            prov_groups[prov[5]].append(prov)
        for prov_group in prov_groups.values():
            # get most recently updated providers from each issuer
            prov_group, update_date = most_recent_set(prov_group)
            orig_ids = ','.join([str(prov[0]) for prov in prov_group])
            query = ("SELECT "
                     "idx_plan, network_tier "
                     "FROM Provider_Plan "
                     "WHERE idx_provider IN ({})"
                     ).format(orig_ids)
            for row in set(conn_full.execute(query).fetchall()):
                # Insert all plan information for most recently updated
                # provider entries. Hopefully it is not contridictory.
                query = ("INSERT "
                         "INTO Provider_Plan "
                         "(npi, idx_plan, network_tier, last_updated_on) "
                         "VALUES (?,?,?,?)")
                args = (npi, *row, update_date)
                conn_clean.execute(query, args)

    def copy_provider_info(npi, provs):
        provs, _ = most_recent_set(provs)
        orig_ids = ','.join([str(prov[0]) for prov in provs])
        ##################
        # Provider
        ##################
        # Use the first(arbitrary) provider in the list for name/type/accepting
        # information
        query = ("INSERT "
                 "INTO Provider "
                 "(npi, name, type, accepting) "
                 "VALUES (?,?,?,?)")
        args = (npi, provs[0][1], provs[0][3], provs[0][4])
        conn_clean.execute(query, args)
        ##################
        # Address
        ##################
        query = ("SELECT address, city, state, zip, phone "
                 "FROM Address "
                 "WHERE idx_provider IN ({})").format(orig_ids)
        addrs = conn_full.execute(query).fetchall()
        for addr in set(addrs):
            query = ("INSERT "
                     "INTO Address "
                     "(npi, address, city, state, zip, phone) "
                     "VALUES (?,?,?,?,?,?)")
            args = (npi, *addr)
            conn_clean.execute(query, args)
        ##################
        # Language
        ##################
        query = ("SELECT DISTINCT idx_language "
                 "FROM Provider_Language "
                 "WHERE idx_provider IN ({})").format(orig_ids)
        languages = conn_full.execute(query).fetchall()
        for language in languages:
            query = ("INSERT "
                     "INTO Provider_Language "
                     "(npi, idx_language) "
                     "VALUES (?,?)")
            args = (npi, language[0])
            conn_clean.execute(query, args)
        ##################
        # Specialty
        ##################
        query = ("SELECT DISTINCT idx_specialty "
                 "FROM Provider_Specialty "
                 "WHERE idx_provider IN ({})").format(orig_ids)
        specialties = conn_full.execute(query).fetchall()
        for specialty in specialties:
            query = ("INSERT "
                     "INTO Provider_Specialty "
                     "(npi, idx_specialty) "
                     "VALUES (?,?)")
            args = (npi, specialty[0])
            conn_clean.execute(query, args)
        ##################
        # Facility Type
        ##################
        query = ("SELECT DISTINCT idx_facility_type "
                 "FROM Provider_FacilityType "
                 "WHERE idx_provider IN ({})").format(orig_ids)
        facility_types = conn_full.execute(query).fetchall()
        for facility_type in facility_types:
            query = ("INSERT "
                     "INTO Provider_FacilityType "
                     "(npi, idx_facility_type) "
                     "VALUES (?,?)")
            args = (npi, facility_type[0])
            conn_clean.execute(query, args)

    def copy_provider(npi=None):
        if npi is None:
            return
        query = ("SELECT "
                 "idx_provider, name, last_updated_on, "
                 "type, accepting, idx_issuer_group "
                 "FROM Provider "
                 "INNER JOIN ProviderURL ON (source_url_id=url_id) "
                 "WHERE npi=?;")
        provs = conn_full.execute(query, (npi,)).fetchall()

        copy_provider_info(npi, provs)
        copy_provider_plans(npi, provs)

    query = "SELECT DISTINCT npi FROM Provider;"
    npis = conn_full.execute(query).fetchall()
    num_npis = len(npis)
    for i, row in enumerate(npis):
        print("\rProcessing Provider {}/{}".format(i+1, num_npis), end='')
        copy_provider(npi=row[0])
        if (i % 1000) == 0:
            conn_clean.commit()
    conn_clean.commit()


def copy_drugs(conn_full, conn_clean):
    def copy_drug_plans(rxnorm_id, drugs):
        orig_ids = ','.join([str(drug[0]) for drug in drugs])
        query = ("SELECT "
                 "idx_plan, drug_tier, prior_authorization, "
                 "step_therapy, quantity_limit "
                 "FROM Drug_Plan "
                 "WHERE idx_drug IN ({})"
                 ).format(orig_ids)
        for row in set(conn_full.execute(query).fetchall()):
            # Insert all plan information for most recently updated
            # provider entries. Hopefully it is not contridictory.
            query = ("INSERT "
                     "INTO Drug_Plan "
                     "(rxnorm_id, idx_plan, drug_tier, prior_authorization, "
                     "step_therapy, quantity_limit) "
                     "VALUES (?,?,?,?,?,?)")
            args = (rxnorm_id, *row)
            conn_clean.execute(query, args)

    def copy_drug_info(rxnorm_id, drugs):
        # Use the first(arbitrary) drug in the list for name information
        query = ("INSERT "
                 "INTO Drug "
                 "(rxnorm_id, drug_name) "
                 "VALUES (?,?)")
        args = (rxnorm_id, drugs[0][1])
        conn_clean.execute(query, args)

    def copy_drug(rxnorm_id=None):
        if rxnorm_id is None:
            return
        query = ("SELECT "
                 "idx_drug, drug_name "
                 "FROM Drug "
                 "WHERE rxnorm_id=?")
        drugs = conn_full.execute(query, (rxnorm_id,)).fetchall()

        copy_drug_info(rxnorm_id, drugs)
        copy_drug_plans(rxnorm_id, drugs)

    query = "SELECT DISTINCT rxnorm_id FROM Drug;"
    rxnorm_ids = conn_full.execute(query).fetchall()
    num_rxnorm_ids = len(rxnorm_ids)
    for i, row in enumerate(rxnorm_ids):
        print("\rProcessing Drug {}/{}".format(i+1, num_rxnorm_ids), end='')
        copy_drug(rxnorm_id=row[0])
        if (i % 1000) == 0:
            conn_clean.commit()
    conn_clean.commit()


def main(db_path):
    conn_full = open_full_db(db_path)
    conn_clean = init_clean_db()

    print("Copying common tables")
    copy_common_tables(conn_full, conn_clean)
    print("Finished!")
    print("Copying providers")
    copy_providers(conn_full, conn_clean)
    print("Finished!")
    print("Copying drugs")
    copy_drugs(conn_full, conn_clean)
    print("Finished!")

    conn_full.close()
    conn_clean.close()


if __name__ == '__main__':
    desc = 'Utility to clean main data-pull data'
    parser = argparse.ArgumentParser(description=desc)
    add = parser.add_argument
    add('full_db', help='path to full-data sqlite file', type=str)
    args = parser.parse_args()
    main(args.full_db)
