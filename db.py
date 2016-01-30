import os
import sqlite3
import logging
LOGGER = logging.getLogger('HHC')

import models

def get_type_str(provider):
    if type(provider) == models.Individual:
        return "INDIVIDUAL"
    else:
        return "FACILITY"

def get_db_fname(id_issuer):
    fname = "{}.sqlite3".format(id_issuer)
    fname = os.path.join("db", fname)
    return fname

def init_db(id_issuer):
    fname = get_db_fname(id_issuer)
    if not os.path.exists("db"):
        os.mkdir("db")
    if os.path.exists(fname):
        logging.warning("DB file {} already exists. Deleting".format(fname))
        os.remove(fname)

    conn = sqlite3.connect(fname)
    conn.executescript('''
    CREATE TABLE Issuer (id_issuer            INTEGER PRIMARY KEY,
                         name                 TEXT    NOT NULL,
                         marketplace_category TEXT    NOT NULL,
                         url_submitted        TEXT    NOT NULL,
                         state                TEXT    NOT NULL);

    CREATE TABLE ProviderURL (url              TEXT    NOT NULL,
                              id_issuer           INTEGER NOT NULL);

    CREATE TABLE PlanURL (url          TEXT    NOT NULL,
                          id_issuer    INTEGER NOT NULL);

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       plan_id_type   TEXT    NOT NULL,
                       marketing_name TEXT    NOT NULL,
                       summary_url    TEXT    NOT NULL,
                       UNIQUE(id_plan,id_issuer) ON CONFLICT REPLACE);

    CREATE TABLE IndividualProvider (npi             INTEGER,
                                     name_first      TEXT    NOT NULL,
                                     name_last       TEXT    NOT NULL,
                                     last_updated_on INTEGER NOT NULL,
                                     accepting       TEXT    NOT NULL);

    CREATE TABLE FacilityProvider (npi             INTEGER,
                                   name            TEXT    NOT NULL,
                                   last_updated_on INTEGER NOT NULL);

    CREATE TABLE Address (npi         INTEGER NOT NULL,
                          type        TEXT    NOT NULL,
                          address     TEXT    NOT NULL,
                          city        TEXT    NOT NULL,
                          state       TEXT    NOT NULL,
                          zip         TEXT    NOT NULL,
                          phone       TEXT    NOT NULL);

    CREATE TABLE Language (idx_language INTEGER PRIMARY KEY AUTOINCREMENT,
                           language     TEXT    NOT NULL);

    CREATE TABLE Specialty (idx_specialty INTEGER PRIMARY KEY AUTOINCREMENT,
                            specialty     TEXT    NOT NULL);

    CREATE TABLE FacilityType (idx_facility_type INTEGER PRIMARY KEY AUTOINCREMENT,
                               facility_type     TEXT    NOT NULL);

    CREATE TABLE Provider_Language (npi          INTEGER NOT NULL,
                                    type         TEXT    NOT NULL,
                                    idx_language INTEGER NOT NULL,
                                    UNIQUE(npi,idx_language) ON CONFLICT IGNORE);

    CREATE TABLE Provider_Specialty (npi           INTEGER NOT NULL,
                                     type          TEXT    NOT NULL,
                                     idx_specialty INTEGER NOT NULL,
                                     UNIQUE(npi,idx_specialty) ON CONFLICT IGNORE);

    CREATE TABLE Provider_FacilityType (npi     TEXT    NOT NULL,
                                        idx_facility_type INTEGER);

    CREATE TABLE Provider_Plan (npi      INTEGER NOT NULL,
                                idx_plan INTEGER NOT NULL,
                                UNIQUE(npi,idx_plan) ON CONFLICT IGNORE);
    ''')
    conn.commit()
    return conn

def open_db(id_issuer):
    fname = get_db_fname(id_issuer)
    conn = sqlite3.connect(fname)
    return conn

def close_db(conn, commit=True):
    if commit:
        conn.commit()
    conn.close()

def insert_issuer_urls(conn, issuer, plan_urls, provider_urls):
    for plan_url in plan_urls:
        conn.execute("INSERT INTO PlanURL (url, id_issuer) VALUES (?, ?);",
                     (plan_url, issuer.id_issuer))
    for provider_url in provider_urls:
        conn.execute("INSERT INTO ProviderURL (url, id_issuer) VALUES (?, ?);",
                     (provider_url, issuer.id_issuer))

def query_issuer_urls(conn, issuer):
    plan_urls = conn.execute("SELECT url FROM PlanURL WHERE (id_issuer=?);",
                             (issuer.id_issuer,)).fetchall()
    prov_urls = conn.execute("SELECT url FROM ProviderURL WHERE (id_issuer=?);",
                             (issuer.id_issuer,)).fetchall()
    first = lambda xs: [x[0] for x in xs]
    return (first(plan_urls), first(prov_urls))

def insert_issuer(conn,issuer):
    conn.execute("INSERT INTO Issuer VALUES (?,?,?,?,?);",
                 (issuer.id_issuer,
                  issuer.name,
                  issuer.marketplace_category,
                  issuer.url_submitted,
                  issuer.state))

def insert_plans(conn, plans):
    for p in plans:
        conn.execute("INSERT INTO Plan VALUES (?,?,?,?,?,?)",
                     (p.idx_plan,
                      p.id_plan,
                      p.id_issuer,
                      p.plan_id_type,
                      p.marketing_name,
                      p.summary_url))

def query_issuer_ids(conn):
    res = conn.execute("SELECT id_issuer FROM Issuer;").fetchall()
    return [x[0] for x in res]

def query_issuer(conn, id_issuer, populate_plans=False):
    res = conn.execute("SELECT * FROM Issuer WHERE (id_issuer=?);",(id_issuer,))
    res = res.fetchone()
    issuer =  models.Issuer(id_issuer=res[0],
                            name=res[1],
                            marketplace_category=res[2],
                            url_submitted=res[3],
                            state=res[4],
                            plans=[])
    if populate_plans:
        res = conn.execute("SELECT * FROM Plan WHERE (id_issuer=?);",(id_issuer,))
        for p_res in res.fetchall():
            p = models.Plan(idx_plan = p_res[0],
                            id_plan = p_res[1],
                            id_issuer = p_res[2],
                            plan_id_type=p_res[3],
                            marketing_name=p_res[4],
                            summary_url=p_res[5])
            issuer.plans.append(p)
    return issuer

def query_languages(conn):
    res = conn.execute("SELECT * FROM Language;").fetchall()
    return {language: idx_language for (idx_language, language) in res}

def query_specialties(conn):
    res = conn.execute("SELECT * FROM Specialty;").fetchall()
    return {specialty: idx_specialty for (idx_specialty, specialty) in res}

def query_facility_types(conn):
    res = conn.execute("SELECT * FROM FacilityType;").fetchall()
    return {facility_type: idx_facility_type for (idx_facility_type, facility_type) in res}


def insert_providers(conn, providers):
    specialties = query_specialties(conn)        # map from specialty name to idx_specialty in db
    languages = query_languages(conn)            # same but for langs
    facility_types = query_facility_types(conn)  # same but for facility types
    for provider in providers:
        if type(provider) == models.Facility:
            conn.execute("INSERT INTO FacilityProvider VALUES (?,?,?);",
                         (provider.npi,
                          provider.facility_name,
                          provider.last_updated_on))
            for facility_type in provider.facility_types:
                if facility_type not in facility_types:
                    facility_types[facility_type] = insert_facility_type(conn, facility_type)
                insert_provider_facility_type(conn, provider, facility_types[facility_type])
        else:
            conn.execute("INSERT INTO IndividualProvider VALUES (?,?,?,?,?);",
                         (provider.npi,
                          provider.name_first,
                          provider.name_last,
                          provider.last_updated_on,
                          provider.accepting))
            for language in provider.languages:
                if language not in languages:
                    languages[language] = insert_language(conn, language)
                insert_provider_language(conn, provider, languages[language])
            for specialty in provider.specialties:
                if specialty not in specialties:
                    specialties[specialty] = insert_specialty(conn, specialty)
                insert_provider_specialty(conn, provider, specialties[specialty])

        insert_provider_plans(conn, provider)
        insert_addresses(conn, provider, provider.addresses)

def insert_addresses(conn, provider, addresses):
    for address in addresses:
        conn.execute("INSERT INTO Address VALUES (?,?,?,?,?,?,?);",
                     (provider.npi,
                      get_type_str(provider),
                      address.address,
                      address.city,
                      address.state,
                      address.zip,
                      address.phone))

def insert_language(conn, language):
    conn.execute("INSERT INTO Language (language) VALUES (?);",
                 (language,))
    return conn.execute("SELECT idx_language FROM Language WHERE (language=?);",
                        (language,)).fetchone()[0]

def insert_provider_language(conn, provider, idx_language):
    conn.execute("INSERT INTO Provider_Language (npi, type, idx_language) VALUES (?,?,?);",
                 (provider.npi, "INDIVIDUAL", idx_language))

def insert_specialty(conn, specialty):
    conn.execute("INSERT INTO Specialty (specialty) VALUES (?);",
                 (specialty,))
    return conn.execute("SELECT idx_specialty FROM Specialty WHERE (specialty=?);",
                        (specialty,)).fetchone()[0]

def insert_provider_specialty(conn, provider, idx_specialty):
    conn.execute("INSERT INTO Provider_Specialty (npi, type, idx_specialty) VALUES (?,?,?);",
                 (provider.npi, get_type_str(provider), idx_specialty))

def insert_facility_type(conn, facility_type):
    conn.execute("INSERT INTO FacilityType (facility_type) VALUES (?);",
                 (facility_type,))
    return conn.execute("SELECT idx_facility_type FROM FacilityType WHERE (facility_type=?);",
                        (facility_type,)).fetchone()[0]

def insert_provider_facility_type(conn, provider, idx_facility_type):
    conn.execute("INSERT INTO Provider_FacilityType (npi, idx_facility_type) VALUES (?,?);",
                 (provider.npi, idx_facility_type,))

def insert_provider_plans(conn, provider):
    for plan in provider.plans:
        conn.execute("INSERT INTO Provider_Plan VALUES (?,?);",(provider.npi, plan.idx_plan))

