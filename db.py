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
                              id_issuer           INTEGER NOT NULL,
                              FOREIGN KEY(id_issuer) REFERENCES Issuer(id_issuer));

    CREATE TABLE PlanURL (url          TEXT    NOT NULL,
                          id_issuer    INTEGER NOT NULL,
                          FOREIGN KEY(id_issuer) REFERENCES Issuer(id_issuer));

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY AUTOINCREMENT,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       plan_id_type   TEXT    NOT NULL,
                       marketing_name TEXT    NOT NULL,
                       summary_url    TEXT    NOT NULL,
                       UNIQUE(idx_plan,id_issuer) ON CONFLICT REPLACE,
                       FOREIGN KEY(id_issuer) REFERENCES Issuer(id_issuer));

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
                          FOREIGN KEY(idx_provider) REFERENCES Provider(idx_provider));

    CREATE TABLE Language (idx_language INTEGER PRIMARY KEY AUTOINCREMENT,
                           language     TEXT    NOT NULL);

    CREATE TABLE Specialty (idx_specialty INTEGER PRIMARY KEY AUTOINCREMENT,
                            specialty     TEXT    NOT NULL);

    CREATE TABLE FacilityType (idx_facility_type INTEGER PRIMARY KEY AUTOINCREMENT,
                               facility_type     TEXT    NOT NULL);

    CREATE TABLE Provider_Language (idx_provider INTEGER NOT NULL,
                                    idx_language INTEGER NOT NULL,
                                    UNIQUE(idx_provider,idx_language) ON CONFLICT IGNORE,
                                    FOREIGN KEY(idx_provider) REFERENCES Provider(idx_provider),
                                    FOREIGN KEY(idx_language) REFERENCES Language(idx_language));

    CREATE TABLE Provider_Specialty (idx_provider  INTEGER NOT NULL,
                                     idx_specialty INTEGER NOT NULL,
                                     UNIQUE(idx_provider,idx_specialty) ON CONFLICT IGNORE,
                                     FOREIGN KEY(idx_provider) REFERENCES Provider(idx_provider),
                                     FOREIGN KEY(idx_specialty) REFERENCES Specialty(idx_specialty));


    CREATE TABLE Provider_FacilityType (idx_provider      INTEGER NOT NULL,
                                        idx_facility_type INTEGER NOT NULL,
                                        UNIQUE(idx_provider, idx_facility_type) ON CONFLICT IGNORE,
                                        FOREIGN KEY(idx_provider) REFERENCES Provider(idx_provider),
                                        FOREIGN KEY(idx_facility_type) REFERENCES FacilityType(idx_facility_type));

    CREATE TABLE Provider_Plan (idx_provider INTEGER NOT NULL,
                                idx_plan     INTEGER NOT NULL,
                                UNIQUE(idx_provider,idx_plan) ON CONFLICT IGNORE,
                                FOREIGN KEY(idx_provider) REFERENCES Provider(idx_provider),
                                FOREIGN KEY(idx_plan) REFERENCES Specialty(idx_plan));

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
        conn.execute("INSERT INTO Plan (id_plan, id_issuer, plan_id_type, marketing_name, summary_url) VALUES (?,?,?,?,?)",
                     (p.id_plan,
                      p.issuer.id_issuer,
                      p.plan_id_type,
                      p.marketing_name,
                      p.summary_url))
        p.idx_plan = conn.execute("SELECT last_insert_rowid();").fetchone()[0]

def query_issuer_ids(conn):
    res = conn.execute("SELECT id_issuer FROM Issuer;").fetchall()
    return [x[0] for x in res]

def query_plans(conn, issuer):
    res = conn.execute("SELECT idx_plan, id_plan, plan_id_type, marketing_name, summary_url FROM Plan WHERE (id_issuer=?);",(issuer.id_issuer,))
    for p_res in res.fetchall():
        p = models.Plan()
        p.idx_plan = p_res[0]
        p.id_plan = p_res[1]
        p.issuer = issuer
        p.plan_id_type=p_res[2]
        p.marketing_name=p_res[3]
        p.summary_url=p_res[4]
        issuer.plans.append(p)

def query_issuer(conn, id_issuer, query_plans=False):
    res = conn.execute("SELECT name, marketplace_category, url_submitted, state FROM Issuer WHERE (id_issuer=?);",(id_issuer,))
    res = res.fetchone()
    issuer = models.Issuer()
    issuer.id_issuer=id_issuer
    issuer.name=res[0]
    issuer.marketplace_category=res[1]
    issuer.url_submitted=res[2]
    issuer.state=res[3]
    issuer.plans=[]
    if query_plans:
        query_plans(conn, issuer)
    return issuer

def query_languages(conn):
    res = conn.execute("SELECT idx_language, language FROM Language;").fetchall()
    return {language: models.Language(language,idx_language) for (idx_language, language) in res}

def query_specialties(conn):
    res = conn.execute("SELECT idx_specialty,specialty FROM Specialty;").fetchall()
    return {specialty: models.Specialty(specialty,idx_specialty) for (idx_specialty, specialty) in res}

def query_facility_types(conn):
    res = conn.execute("SELECT * FROM FacilityType;").fetchall()
    return {facility_type: models.FacilityType(facility_type,idx_facility_type) for (idx_facility_type, facility_type) in res}


def insert_providers(conn, providers):
    specialties = query_specialties(conn)        # map from specialty name to specialty obj w/ idx in db
    languages = query_languages(conn)            # same but for langs
    facility_types = query_facility_types(conn)  # same but for facility types
    for provider in providers:
        conn.execute("INSERT INTO Provider (npi,name,last_updated_on,type,accepting) VALUES (?,?,?,?,?);",
                     (provider.npi, provider.name,
                      provider.last_updated_on.toordinal(),
                      int(provider.type), int(provider.accepting)))
        provider.idx_provider = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
        for facility_type in provider.facility_types:
            if facility_type.facility_type not in facility_types:
                facility_types[facility_type.facility_type] = insert_facility_type(conn, facility_type)
            else:
                facility_type.idx_facility_type = facility_types[facility_type.facility_type].idx_facility_type
        for language in provider.languages:
            if language.language not in languages:
                languages[language.language] = insert_language(conn, language)
            else:
                language.idx_language = languages[language.language].idx_language
        for specialty in provider.specialties:
            if specialty.specialty not in specialties:
                specialties[specialty.specialty] = insert_specialty(conn, specialty)
            else:
                specialty.idx_specialty = specialties[specialty.specialty].idx_specialty

        insert_provider_facility_types(conn, provider)
        insert_provider_languages(conn, provider)
        insert_provider_specialties(conn, provider)
        insert_provider_plans(conn, provider)
        insert_addresses(conn, provider)

def insert_addresses(conn, provider):
    for address in provider.addresses:
        conn.execute("INSERT INTO Address (idx_provider, address, city, state, zip, phone) VALUES (?,?,?,?,?,?);",
                     (provider.idx_provider,
                      address.address, address.city,
                      address.state, address.zip, address.phone))

def insert_language(conn, language):
    conn.execute("INSERT INTO Language (language) VALUES (?);", (language.language,))
    language.idx_language = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
    return language

def insert_specialty(conn, specialty):
    conn.execute("INSERT INTO Specialty (specialty) VALUES (?);", (specialty.specialty,))
    specialty.idx_specialty = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
    return specialty

def insert_facility_type(conn, facility_type):
    conn.execute("INSERT INTO FacilityType (facility_type) VALUES (?);", (facility_type.facility_type,))
    facility_type.idx_facility_type = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
    return facility_type

def insert_provider_languages(conn, provider):
    for language in provider.languages:
        conn.execute("INSERT INTO Provider_Language (idx_provider, idx_language) VALUES (?,?);",
                     (provider.idx_provider, language.idx_language))

def insert_provider_specialties(conn, provider):
    for specialty in provider.specialties:
        conn.execute("INSERT INTO Provider_Specialty (idx_provider,idx_specialty) VALUES (?,?);",
                     (provider.idx_provider, specialty.idx_specialty))

def insert_provider_facility_types(conn, provider):
    for facility_type in provider.facility_types:
        conn.execute("INSERT INTO Provider_FacilityType (idx_provider, idx_facility_type) VALUES (?,?);",
                     (provider.idx_provider, facility_type.idx_facility_type))

def insert_provider_plans(conn, provider):
    for plan in provider.plans:
        conn.execute("INSERT INTO Provider_Plan (idx_provider, idx_plan) VALUES (?,?);", (provider.idx_provider, plan.idx_plan))

