import os
import logging
import sqlite3
from collections import namedtuple

Issuer = namedtuple('Issuer', ['id_issuer',
                               'name',
                               'marketplace_category',
                               'url_submitted',
                               'state'])

Plan = namedtuple('Plan', ['idx_plan',
                           'id_plan',
                           'id_issuer',
                           'plan_id_type',
                           'marketing_name',
                           'summary_url'])

Individual = namedtuple('Individual', ['npi',
                                       'name_first',
                                       'name_last',
                                       'last_updated_on',
                                       'accepting'])

Facility = namedtuple('Facility', ['npi',
                                   'name_facility',
                                   'last_updated_on'])

Address = namedtuple('Address', ['npi',
                                 'address',
                                 'city',
                                 'state',
                                 'zip',
                                 'phone'])

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

    CREATE TABLE ProviderURL (idx_provider_url INTEGER PRIMARY KEY AUTOINCREMENT,
                              url              TEXT    NOT NULL,
                              id_issuer           INTEGER NOT NULL);

    CREATE TABLE PlanURL (idx_plan_url INTEGER PRIMARY KEY AUTOINCREMENT,
                          url          TEXT    NOT NULL,
                          id_issuer    INTEGER NOT NULL);

    CREATE TABLE Plan (idx_plan       INTEGER PRIMARY KEY,
                       id_plan        TEXT    NOT NULL,
                       id_issuer      INTEGER NOT NULL,
                       plan_id_type   TEXT    NOT NULL,
                       marketing_name TEXT    NOT NULL,
                       summary_url    TEXT    NOT NULL,
                       UNIQUE(id_plan,id_issuer) ON CONFLICT REPLACE);

    CREATE TABLE IndividualProvider (npi             INTEGER PRIMARY KEY,
                                     name_first      TEXT    NOT NULL,
                                     name_last       TEXT    NOT NULL,
                                     last_updated_on INTEGER NOT NULL,
                                     accepting       TEXT    NOT NULL);

    CREATE TABLE FaciltyProvider (npi             INTEGER PRIMARY KEY,
                                  name            TEXT    NOT NULL,
                                  last_updated_on INTEGER NOT NULL,
                                  address_address TEXT    NOT NULL);

    CREATE TABLE Address (idx_address INTEGER PRIMARY KEY AUTOINCREMENT,
                          npi         INTEGER,
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

    CREATE TABLE Provider_Plan (npi      INTEGER UNIQUE NOT NULL,
                                idx_plan INTEGER UNIQUE NOT NULL);

    CREATE TABLE IndividualProvider_Language (npi          INTEGER UNIQUE NOT NULL,
                                              idx_language INTEGER UNIQUE NOT NULL);

    CREATE TABLE Provider_Specialty (npi           INTEGER UNIQUE NOT NULL,
                                     idx_specialty INTEGER UNIQUE NOT NULL);

    CREATE TABLE FacilityProvider_Specialty (npi           INTEGER UNIQUE NOT NULL,
                                             idx_specialty INTEGER UNIQUE NOT NULL);
    ''')
    conn.commit()
    return conn


def open_db(id_issuer):
    fname = get_db_fname(id_issuer)
    conn = sqlite3.connect(fname)
    return conn

def insert_plan_url(conn, issuer, plan_url):
    conn.execute("INSERT INTO PlanURL (url, id_issuer) VALUES (?, ?);",
                 (plan_url, issuer.id_issuer))
def query_plan_urls(conn, issuer):
    print(issuer)
    res = conn.execute("SELECT url FROM PlanURL WHERE (id_issuer=?);", (issuer.id_issuer,)).fetchall()
    return [r[0] for r in res]


def insert_provider_url(conn, issuer, provider_url):
    conn.execute("INSERT INTO ProviderURL (url, id_issuer) VALUES (?, ?);",
                 (provider_url, issuer.id_issuer))
def query_provider_urls(conn, issuer):
    res = conn.execute("SELECT url FROM ProviderURL WHERE (id_issuer=?);", (issuer.id_issuer,)).fetchall()
    return [r[0] for r in res]


def insert_issuer(conn,issuer):
    conn.execute("INSERT INTO Issuer VALUES (?,?,?,?,?);", issuer)

def query_issuer(conn):
    res = conn.execute("SELECT * FROM Issuer;").fetchone()
    return Issuer(id_issuer=res[0],
                  name=res[1],
                  marketplace_category=res[2],
                  url_submitted=res[3],
                  state=res[4])

def insert_plan(conn, plan):
    conn.execute("INSERT INTO Plan VALUES (?,?,?,?,?,?)",plan)

def insert_provider(conn, provider):
    if type(provider) == Facility:
        conn.execute("INSERT INTO FacilityProvider VALUES (?,?,?);",provider)
    else:
        conn.execute("INSERT INTO IndividualProvider VALUES (?,?,?,?,?);", provider)


def close_db(conn, commit=True):
    if commit:
        conn.commit()
    conn.close()
