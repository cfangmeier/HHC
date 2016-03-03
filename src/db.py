import os
import time
import sqlite3

import models


def get_last_idx(conn):
    return conn.execute("SELECT last_insert_rowid();").fetchone()[0]


def open_db(recreate=True):
    fname = "HHC_Data.sqlite3"
    if recreate and os.path.exists(fname):
        os.remove(fname)
    return sqlite3.connect(fname)


def init_db():
    conn = open_db()
    conn.executescript('''
    CREATE TABLE IssuerGroup (idx_issuer_group INTEGER PRIMARY KEY AUTOINCREMENT,
                              index_url        TEXT    NOT NULL,
                              index_status     TEXT);

    CREATE TABLE Issuer (id_issuer             INTEGER PRIMARY KEY,
                         idx_issuer_group      INTEGER NOT NULL,
                         name                  TEXT    NOT NULL,
                         state                 TEXT    NOT NULL,
                         FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(idx_issuer_group),
                         UNIQUE (id_issuer) ON CONFLICT IGNORE);

    CREATE TABLE ProviderURL (url              TEXT    NOT NULL,
                              idx_issuer_group INTEGER NOT NULL,
                              download_status  TEXT    NOT NULL,
                              FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(id_issuer_group),
                              UNIQUE(url, idx_issuer_group) ON CONFLICT REPLACE);

    CREATE TABLE PlanURL (url              TEXT    NOT NULL,
                          idx_issuer_group INTEGER NOT NULL,
                          download_status  TEXT    NOT NULL,
                          FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(id_issuer_group),
                          UNIQUE(url, idx_issuer_group) ON CONFLICT REPLACE);

    CREATE TABLE DrugURL (url              TEXT    NOT NULL,
                          idx_issuer_group INTEGER NOT NULL,
                          download_status  TEXT    NOT NULL,
                          FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(id_issuer_group),
                          UNIQUE(url, idx_issuer_group) ON CONFLICT REPLACE);

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY AUTOINCREMENT,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       plan_id_type   TEXT    NOT NULL,
                       marketing_name TEXT,
                       summary_url    TEXT,
                       UNIQUE(id_plan,id_issuer) ON CONFLICT IGNORE,
                       FOREIGN KEY(id_issuer) REFERENCES Issuer(id_issuer));

    CREATE TABLE Provider (idx_provider    INTEGER PRIMARY KEY AUTOINCREMENT,
                           npi             INTEGER,
                           name            TEXT    NOT NULL,
                           last_updated_on INTEGER NOT NULL,
                           type            INTEGER NOT NULL,
                           accepting       INTEGER NOT NULL,
                           UNIQUE(npi, name) ON CONFLICT IGNORE);

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
                                network_tier TEXT    NOT NULL,
                                UNIQUE(idx_provider,idx_plan) ON CONFLICT IGNORE,
                                FOREIGN KEY(idx_provider) REFERENCES Provider(idx_provider),
                                FOREIGN KEY(idx_plan) REFERENCES Specialty(idx_plan));

    CREATE TABLE Drug (idx_drug  INTEGER PRIMARY KEY AUTOINCREMENT,
                       rxnorm_id INTEGER NOT NULL,
                       drug_name TEXT    NOT NULL,
                       UNIQUE(rxnorm_id) ON CONFLICT IGNORE);

    CREATE TABLE Drug_Plan (idx_drug            INTEGER NOT NULL,
                            idx_plan            INTEGER NOT NULL,
                            drug_tier           TEXT,
                            prior_authorization INTEGER,
                            step_therapy        INTEGER,
                            quantity_limit      INTEGER,
                            UNIQUE(idx_drug,idx_plan) ON CONFLICT IGNORE,
                            FOREIGN KEY(idx_drug) REFERENCES Drug(idx_drug),
                            FOREIGN KEY(idx_plan) REFERENCES Plan(idx_plan));
    ''')
    return conn

def get_download_status(issuer_group, url, type_):
    types = {'plan':"PlanURL",
             'provider': "ProviderURL",
             'formulary': "DrugURL"}
    table = types[type_.lower()]
    conn = open_db()
    query = "SELECT download_status FROM {} WHERE url=? AND idx_issuer_group=?;".format(table)
    res = conn.execute(query,(url,issuer_group.idx_issuer_group)).fetchall()
    conn.close()

    matches = len(res)
    if matches != 1:
        raise ValueError("Bad URL {}, Group_id {} Specified, matched {} rows in DB".format(url, issuer_group.idx_issuer_group, matches))
    return res[0][0]


def update_download_status(issuer_group, url, type_, status):
    types = {'plan':"PlanURL",
             'provider': "ProviderURL",
             'formulary': "DrugURL"}
    table = types[type_.lower()]
    query = "UPDATE {} SET download_status=? WHERE url=? AND idx_issuer_group=?;".format(table)
    while True:
        try:
            conn = open_db()
            conn.execute(query,(status, url, issuer_group.idx_issuer_group))
            conn.commit()
            conn.close()
            break
        except Exception: #concurrent db access error, wait and try again
            conn.close()
            time.sleep(1)


def insert_issuer_group(conn, issuer_group):
    vals = (issuer_group.index_url, issuer_group.index_status)
    conn.execute(("INSERT INTO IssuerGroup "
                  "(index_url, index_status)"
                  "VALUES (?,?);"), vals)
    return get_last_idx(conn)


def insert_issuer(conn, issuer, idx):
    vals = (issuer.id_issuer, idx, issuer.name, issuer.state)
    conn.execute(("INSERT INTO Issuer "
                  "(id_issuer, idx_issuer_group, name, state) "
                  "VALUES (?,?,?,?)"), vals)
    return get_last_idx(conn)


def insert_data_url(conn, url, idx):
    type_ = models.URLType.get_name(url.url_type)
    if type_ is not None:
        query = ("INSERT INTO {}URL "
                 "(url, download_status, idx_issuer_group) "
                 "VALUES (?, ?, ?);").format(type_)
        conn.execute(query, (url.url, 'not finished', idx))


def query_issuer_group_urls(conn, issuer_group):
    def query_urls(type_):
        query = "SELECT url FROM {}URL WHERE (idx_issuer_group=?);".format(type_)
        result = conn.execute(query,(issuer_group.idx_issuer_group,)).fetchall()
        return [x[0] for x in result]
    issuer_group.plan_urls = query_urls("Plan")
    issuer_group.provider_urls = query_urls("Provider")
    issuer_group.formulary_urls = query_urls("Formulary")


def query_issuer_group_issuers(conn, issuer_group):
    q = """SELECT id_issuer, name, marketplace_category, state FROM Issuer
               WHERE (Issuer.idx_issuer_group = ?);"""
    issuer_query = conn.execute(q, (issuer_group.idx_issuer_group,))
    for row in issuer_query.fetchall():
        issuer = models.Issuer()
        issuer.issuer_group = issuer_group
        issuer.id_issuer = row[0]
        issuer.name = row[1]
        issuer.marketplace_category = row[2]
        issuer.state = row[3]
        issuer_group.issuers.append(issuer)


def insert_issuers(conn, issuers):
    for issuer in issuers:
        insert_issuer(conn, issuer)


def insert_plans(conn, plans):
    for p in plans:
        conn.execute("INSERT INTO Plan (id_plan, id_issuer, plan_id_type, marketing_name, summary_url) VALUES (?,?,?,?,?)",
                     (p.id_plan,
                      p.issuer.id_issuer,
                      p.plan_id_type,
                      p.marketing_name,
                      p.summary_url))
        p.idx_plan = get_last_idx(conn)


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
    res = conn.execute("SELECT name, marketplace_category, state FROM Issuer WHERE (id_issuer=?);",(id_issuer,))
    res = res.fetchone()
    issuer = models.Issuer()
    issuer.id_issuer=id_issuer
    issuer.name=res[0]
    issuer.marketplace_category=res[1]
    issuer.state=res[2]
    issuer.plans=[]
    if query_plans:
        query_plans(conn, issuer)
    return issuer


def query_issuers(conn):
    ids = query_issuer_ids(conn)
    issuers = []
    for id_ in ids:
        issuers.append(query_issuer(conn, id_))
    return issuers


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
        provider.idx_provider = get_last_idx(conn)
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
    language.idx_language = get_last_idx(conn)
    return language


def insert_specialty(conn, specialty):
    conn.execute("INSERT INTO Specialty (specialty) VALUES (?);", (specialty.specialty,))
    specialty.idx_specialty = get_last_idx(conn)
    return specialty


def insert_facility_type(conn, facility_type):
    conn.execute("INSERT INTO FacilityType (facility_type) VALUES (?);", (facility_type.facility_type,))
    facility_type.idx_facility_type = get_last_idx(conn)
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
    for prov_plan in provider.plans:
        conn.execute("INSERT INTO Provider_Plan (idx_provider, idx_plan, network_tier) VALUES (?,?,?);",
                     (prov_plan.provider.idx_provider,
                      prov_plan.plan.idx_plan,
                      prov_plan.network_tier))


def insert_drugs(conn, drugs):
    for drug in drugs:
        conn.execute("INSERT INTO Drug (rxnorm_id, drug_name) VALUES (?,?);",
                     (drug.rxnorm_id, drug.drug_name))
        drug.idx_drug = get_last_idx(conn)
        insert_drug_plans(conn, drug)


def insert_drug_plans(conn, drug):
    for drug_plan in drug.plans:
        conn.execute("INSERT INTO Drug_Plan (idx_drug, idx_plan, drug_tier, prior_authorization, step_therapy, quantity_limit) VALUES (?,?,?,?,?,?);",
                     (drug_plan.drug.idx_drug,
                      drug_plan.plan.idx_plan,
                      drug_plan.drug_tier,
                      drug_plan.prior_authorization,
                      drug_plan.step_therapy,
                      drug_plan.quantity_limit))
