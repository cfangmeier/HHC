import os
import sqlite3

import models


def get_last_idx(conn):
    return conn.execute("SELECT last_insert_rowid();").fetchone()[0]


def open_db(recreate=True):
    if not os.path.exists("data"):
        os.mkdir("data")
    fname = "data/data.sqlite3"
    if recreate and os.path.exists(fname):
        os.remove(fname)
    conn = sqlite3.connect(fname)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    """ init_db
    Creates the initial database. *THIS IS NOT THE FINAL SCHEMA*. After the
    download finishes. The cleanup stage will modify some of the table
    restraints.
    """
    conn = open_db()
    conn.executescript('''
    CREATE TABLE IssuerGroup (idx_issuer_group INTEGER PRIMARY KEY,
                              index_url        TEXT    NOT NULL,
                              index_status     TEXT);

    CREATE TABLE Issuer (id_issuer             INTEGER PRIMARY KEY,
                         idx_issuer_group      INTEGER,
                         name                  TEXT    NOT NULL,
                         state                 TEXT    NOT NULL,
                         FOREIGN KEY(idx_issuer_group)
                             REFERENCES IssuerGroup(idx_issuer_group),
                         UNIQUE (id_issuer) ON CONFLICT FAIL);

    CREATE TABLE ProviderURL (url_id           INTEGER PRIMARY KEY,
                              url              TEXT    NOT NULL,
                              idx_issuer_group INTEGER NOT NULL,
                              download_status  TEXT    NOT NULL,
                              FOREIGN KEY(idx_issuer_group)
                                  REFERENCES IssuerGroup(id_issuer_group),
                              UNIQUE(url, idx_issuer_group)
                                  ON CONFLICT FAIL);

    CREATE TABLE PlanURL (url_id           INTEGER PRIMARY KEY,
                          url              TEXT    NOT NULL,
                          idx_issuer_group INTEGER NOT NULL,
                          download_status  TEXT    NOT NULL,
                          FOREIGN KEY(idx_issuer_group)
                              REFERENCES IssuerGroup(id_issuer_group),
                          UNIQUE(url, idx_issuer_group) ON CONFLICT FAIL);

    CREATE TABLE DrugURL (url_id           INTEGER PRIMARY KEY,
                          url              TEXT    NOT NULL,
                          idx_issuer_group INTEGER NOT NULL,
                          download_status  TEXT    NOT NULL,
                          FOREIGN KEY(idx_issuer_group)
                              REFERENCES IssuerGroup(id_issuer_group),
                          UNIQUE(url, idx_issuer_group) ON CONFLICT FAIL);

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY AUTOINCREMENT,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       plan_id_type   TEXT    NOT NULL,
                       marketing_name TEXT,
                       summary_url    TEXT,
                       source_url_id     INTEGER,
                       FOREIGN KEY(source_url_id)
                           REFERENCES PlanURL(url_id));

    CREATE TABLE Provider (idx_provider    INTEGER PRIMARY KEY AUTOINCREMENT,
                           npi             INTEGER,
                           name            TEXT    NOT NULL,
                           last_updated_on INTEGER NOT NULL,
                           type            INTEGER NOT NULL,
                           accepting       INTEGER NOT NULL,
                           source_url_id      INTEGER,
                           FOREIGN KEY(source_url_id)
                               REFERENCES ProviderURL(url_id));

    CREATE TABLE Address (idx_provider INTEGER NOT NULL,
                          address      TEXT,
                          city         TEXT,
                          state        TEXT,
                          zip          TEXT,
                          phone        TEXT,
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
                                    idx_language INTEGER NOT NULL);

    CREATE TABLE Provider_Specialty (idx_provider  INTEGER NOT NULL,
                                     idx_specialty INTEGER NOT NULL);


    CREATE TABLE Provider_FacilityType (idx_provider      INTEGER NOT NULL,
                                        idx_facility_type INTEGER NOT NULL);

    CREATE TABLE Provider_Plan (idx_provider INTEGER NOT NULL,
                                idx_plan     INTEGER NOT NULL,
                                network_tier TEXT    NOT NULL);

    CREATE TABLE Drug (idx_drug  INTEGER PRIMARY KEY AUTOINCREMENT,
                       rxnorm_id INTEGER NOT NULL,
                       drug_name TEXT    NOT NULL,
                       source_url_id      INTEGER,
                       FOREIGN KEY(source_url_id)
                           REFERENCES DrugURL(url_id));

    CREATE TABLE Drug_Plan (idx_drug            INTEGER NOT NULL,
                            idx_plan            INTEGER NOT NULL,
                            drug_tier           TEXT,
                            prior_authorization INTEGER,
                            step_therapy        INTEGER,
                            quantity_limit      INTEGER);
    ''')
    return conn


def create_indices(conn):
    """
    Create a series of indices that will make the "clean-up"
    Stage run much *much* faster.
    """
    conn.execute(("CREATE INDEX Provider_Plan_idx_provider "
                  "ON Provider_Plan (idx_provider)"))
    conn.execute(("CREATE INDEX Drug_Plan_idx_drug "
                  "ON Drug_Plan (idx_drug)"))
    conn.execute(("CREATE INDEX Drug_rxnorm_id "
                  "ON Drug (rxnorm_id)"))
    conn.execute(("CREATE INDEX Provider_npi "
                  "ON Provider (npi)"))
    conn.execute(("CREATE INDEX Address_idx_provider "
                  "ON Address (idx_provider)"))
    conn.execute(("CREATE INDEX Provider_Language_idx_provider "
                  "ON Provider_Language (idx_provider)"))
    conn.execute(("CREATE INDEX Provider_Specialty_idx_provider "
                  "ON Provider_Specialty (idx_provider)"))
    conn.execute(("CREATE INDEX Provider_FacilityType_idx_provider "
                  "ON Provider_FacilityType (idx_provider)"))


def insert_issuer_group(conn, issuer_group):
    vals = (issuer_group.idx_issuer_group,
            issuer_group.index_url,
            issuer_group.index_status)
    conn.execute(("INSERT INTO IssuerGroup "
                  "(idx_issuer_group, index_url, index_status)"
                  "VALUES (?,?,?);"), vals)


def insert_issuer(conn, issuer):
    vals = (issuer.id_issuer,
            issuer.idx_issuer_group,
            issuer.name,
            issuer.state)
    conn.execute(("INSERT INTO Issuer "
                  "(id_issuer, idx_issuer_group, name, state) "
                  "VALUES (?,?,?,?)"), vals)


def insert_data_url(conn, url):
    if url.url_type != models.URLType.void:
        type_ = models.URLType.get_name(url.url_type)
        try:
            query = ("INSERT INTO {}URL "
                     "(url, download_status, idx_issuer_group) "
                     "VALUES (?, ?, ?);").format(type_)
            vals = (url.url, url.status, url.idx_issuer_group)
            conn.execute(query, vals)
            return get_last_idx(conn)
        except sqlite3.IntegrityError:
            query = ("UPDATE {}URL "
                     "SET download_status=? "
                     "WHERE url_id=?;").format(type_)
            vals = (url.status, url.url_id)
            conn.execute(query, vals)
            return url.url_id


def insert_plan(conn, plan):
    args = (plan.id_plan, plan.id_issuer, plan.plan_id_type,
            plan.marketing_name, plan.summary_url, plan.source_url.url_id)
    conn.execute(("INSERT INTO Plan"
                  "(id_plan, id_issuer, plan_id_type, "
                  "marketing_name, summary_url, source_url_id) "
                  "VALUES (?,?,?,?,?,?)"), args)
    return get_last_idx(conn)


def insert_provider(conn, provider):
    args = (provider.npi, provider.name,
            provider.last_updated_on.toordinal(),
            int(provider.type_), int(provider.accepting),
            provider.source_url.url_id)
    conn.execute(("INSERT INTO Provider "
                  "(npi,name,last_updated_on,type,accepting, source_url_id) "
                  "VALUES (?,?,?,?,?,?);"), args)
    return get_last_idx(conn)


def insert_address(conn, address, idx_provider):
    args = (idx_provider, address.address, address.city,
            address.state, address.zip_, address.phone)
    conn.execute(("INSERT INTO Address "
                  "(idx_provider, address, city, state, zip, phone) "
                  "VALUES (?,?,?,?,?,?);"), args)


def insert_language(conn, language):
    args = (language,)
    conn.execute(("INSERT INTO Language "
                  "(language) "
                  "VALUES (?);"), args)
    return get_last_idx(conn)


def insert_provider_language(conn, idx_provider, idx_language):
    args = (idx_provider, idx_language)
    conn.execute(("INSERT INTO Provider_Language "
                  "(idx_provider, idx_language) "
                  "VALUES (?,?);"), args)


def insert_facility_type(conn, facility_type):
    args = (facility_type,)
    conn.execute(("INSERT INTO FacilityType "
                  "(facility_type) "
                  "VALUES (?);"), args)
    return get_last_idx(conn)


def insert_provider_facility_type(conn, idx_provider, idx_facility_type):
    args = (idx_provider, idx_facility_type)
    conn.execute(("INSERT INTO Provider_FacilityType "
                  "(idx_provider, idx_facility_type) "
                  "VALUES (?,?);"), args)


def insert_specialty(conn, specialty):
    args = (specialty,)
    conn.execute(("INSERT INTO Specialty "
                  "(specialty) "
                  "VALUES (?);"), args)
    return get_last_idx(conn)


def insert_provider_specialty(conn, idx_provider, idx_specialty):
    args = (idx_provider, idx_specialty)
    conn.execute(("INSERT INTO Provider_Specialty "
                  "(idx_provider, idx_specialty) "
                  "VALUES (?,?);"), args)


def insert_drug(conn, drug):
    args = (drug.rxnorm_id, drug.name, drug.source_url.url_id)
    conn.execute(("INSERT INTO Drug "
                  "(rxnorm_id,drug_name,source_url_id) "
                  "VALUES (?,?,?);"), args)
    return get_last_idx(conn)


def insert_drug_plan(conn, drug_plan, idx_drug):
    args = (idx_drug, drug_plan.id_plan, drug_plan.drug_tier,
            drug_plan.prior_authorization, drug_plan.step_therapy,
            drug_plan.quantity_limit)
    conn.execute(("INSERT INTO Drug_Plan "
                  "(idx_drug,idx_plan,drug_tier,prior_authorization,"
                  "step_therapy, quantity_limit) "
                  "VALUES (?,?,?,?,?,?);"), args)


def insert_provider_plan(conn, prov_plan, idx_plan, idx_provider):
    args = (idx_provider, idx_plan,
            prov_plan.network_tier)
    conn.execute(("INSERT INTO Provider_Plan"
                  "(idx_provider,idx_plan,network_tier) "
                  "VALUES (?,?,?);"), args)
