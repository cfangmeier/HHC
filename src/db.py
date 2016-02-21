import os
import sqlite3
import logging
LOGGER = logging.getLogger('HHC')

import models

def get_last_idx(conn):
    return conn.execute("SELECT last_insert_rowid();").fetchone()[0]

def get_db_fname(db_name):
    fname = "{}.sqlite3".format(db_name)
    fname = os.path.join("db", fname)
    return fname

def create_index_db():
    fname = get_db_fname('index')
    if not os.path.exists("db"):
        os.mkdir("db")
    if os.path.exists(fname):
        logging.warning("DB file {} already exists. Deleting".format(fname))
        os.remove(fname)

    conn = sqlite3.connect(fname)
    conn.executescript('''
    CREATE TABLE IssuerGroup (idx_issuer_group INTEGER PRIMARY KEY AUTOINCREMENT,
                              url_submitted    TEXT    NOT NULL,
                              url_status       TEXT);

    CREATE TABLE Issuer (id_issuer             INTEGER PRIMARY KEY,
                         idx_issuer_group      INTEGER NOT NULL,
                         name                  TEXT    NOT NULL,
                         marketplace_category  TEXT    NOT NULL,
                         state                 TEXT    NOT NULL,
                         FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(idx_issuer_group),
                         UNIQUE (id_issuer) ON CONFLICT IGNORE);

    CREATE TABLE ProviderURL (url              TEXT    NOT NULL,
                              idx_issuer_group INTEGER NOT NULL,
                              download_status  TEXT    NOT NULL,
                              FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(id_issuer_group));

    CREATE TABLE PlanURL (url              TEXT    NOT NULL,
                          idx_issuer_group INTEGER NOT NULL,
                          download_status  TEXT    NOT NULL,
                          FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(id_issuer_group));

    CREATE TABLE FormularyURL (url              TEXT    NOT NULL,
                               idx_issuer_group INTEGER NOT NULL,
                               download_status  TEXT    NOT NULL,
                               FOREIGN KEY(idx_issuer_group) REFERENCES IssuerGroup(id_issuer_group));
    ''')
    return conn

def open_index_db():
    fname = get_db_fname('index')
    return sqlite3.connect(fname)

def get_download_status(issuer_group, url, type_):
    types = {'plan':"PlanURL",
             'provider': "ProviderURL",
             'formulary': "FormularyURL"}
    table = types[type_.lower()]
    conn = open_index_db()
    query = "SELECT download_status FROM {} WHERE url=? AND idx_issuer_group=?;".format(table)
    res = conn.execute(query,(issuer_group.idx_issuer_group, url)).fetchall()
    conn.close()

    matches = len(res)
    if matches != 1:
        raise ValueError("Bad URL () Specified, matched {} rows in DB".format(url,matches))
    return res[0][0]


def update_download_status(issuer_group, url, type_, status):
    types = {'plan':"PlanURL",
             'provider': "ProviderURL",
             'formulary': "FormularyURL"}
    table = types[type_.lower()]
    conn = open_index_db()
    query = "UPDATE {} SET download_status=? WHERE url=? AND idx_issuer_group=?;".format(table)
    conn.execute(query,(status, issuer_group.idx_issuer_group, url))
    conn.commit()
    conn.close()

def insert_issuer_groups(conn, issuer_groups):
    for issuer_group in issuer_groups:
        conn.execute("INSERT INTO IssuerGroup (url_submitted) VALUES (?)",
                     (issuer_group.url_submitted,))
        issuer_group.idx_issuer_group = get_last_idx(conn)
        for issuer in issuer_group.issuers:
            conn.execute("""INSERT INTO Issuer
                            (id_issuer, idx_issuer_group, name, marketplace_category, state)
                            VALUES (?,?,?,?,?)""", (issuer.id_issuer,
                                                    issuer_group.idx_issuer_group,
                                                    issuer.name,
                                                    issuer.marketplace_category,
                                                    issuer.state))
    conn.commit()

def insert_issuer_group_urls(conn, issuer_group):
    conn.execute("UPDATE IssuerGroup SET url_status=? WHERE idx_issuer_group=?;",
                 (issuer_group.url_status,issuer_group.idx_issuer_group))
    def insert_urls(type_, urls):
        query = "INSERT INTO {}URL (url, download_status, idx_issuer_group) VALUES (?, ?, ?);".format(type_)
        for url in urls:
            conn.execute(query,(url, 'not finished', issuer_group.idx_issuer_group))
    insert_urls("Plan", issuer_group.plan_urls)
    insert_urls("Provider", issuer_group.provider_urls)
    insert_urls("Formulary", issuer_group.formulary_urls)
    conn.commit()


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


def query_requested_groups(conn, requested_ids, requested_states):
    if requested_states:
        state_query = ', '.join('?' for _ in requested_states)
    if requested_ids:
        id_query = ', '.join('?' for _ in requested_ids)

    if requested_ids and requested_states:
        where = """
            WHERE (Issuer.id_issuer IN ({})) AND (Issuer.state IN ({}))
                AND""".format(id_query, state_query)
        params = tuple(requested_ids + requested_states)
    elif requested_ids and not requested_states:
        where = """
            WHERE (Issuer.id_issuer IN ({}))
                AND""".format(id_query)
        params = tuple(requested_ids)
    elif not requested_ids and requested_states:
        where = """
            WHERE (Issuer.state IN ({}))
                AND""".format(state_query)
        params = tuple(requested_states)
    else:
        where = """WHERE """.format()
        params = tuple()

    group_query = conn.execute("""
        SELECT IssuerGroup.idx_issuer_group, IssuerGroup.url_submitted FROM Issuer
            NATURAL JOIN IssuerGroup
            {where} IssuerGroup.url_submitted <> "NOT SUBMITTED"
            GROUP BY IssuerGroup.idx_issuer_group;
        """.format(where=where), params)
    groups = []
    for row in group_query.fetchall():
        group = models.IssuerGroup()
        group.idx_issuer_group = row[0]
        group.url_submitted = row[1]
        groups.append(group)
    return groups

def init_issuer_group_db(idx_issuer_group):
    fname = get_db_fname(idx_issuer_group)
    if not os.path.exists("db"):
        os.mkdir("db")
    if os.path.exists(fname):
        logging.warning("DB file {} already exists. Replacing".format(fname))
        os.remove(fname)

    conn = sqlite3.connect(fname)
    conn.executescript('''
    CREATE TABLE Issuer (id_issuer            INTEGER PRIMARY KEY,
                         name                 TEXT,
                         marketplace_category TEXT,
                         state                TEXT,
                         UNIQUE (id_issuer) ON CONFLICT IGNORE);

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY AUTOINCREMENT,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       plan_id_type   TEXT    NOT NULL,
                       marketing_name TEXT    NOT NULL,
                       summary_url    TEXT    NOT NULL,
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
    conn.commit()
    return conn

def open_issuer_group_db(id_group):
    fname = get_db_fname(id_group)
    conn = sqlite3.connect(fname)
    return conn

def close_issuer_group_db(conn, commit=True):
    if commit:
        conn.commit()
    conn.close()

def insert_issuer(conn,issuer):
    conn.execute("INSERT INTO Issuer VALUES (?,?,?,?);",
                 (issuer.id_issuer,
                  issuer.name,
                  issuer.marketplace_category,
                  issuer.state))

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

