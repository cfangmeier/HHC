import sqlite3


def init_db(issuer_id):
    conn = sqlite3.connect("{}.sqlite3".format(issuer_id))
    cur = conn.cursor()
    ex = cur.execute
    ex('''
CREATE TABLE Issuer (id_issuer INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     marketplace_category TEXT NOT NULL,
                     state TEXT NOT NULL);
    ''')
    ex('''
CREATE TABLE Plan (idx_plan INTEGER PRIMARY KEY,
                   id_plan INTEGER UNIQUE NOT NULL,
                   id_issuer INTEGER UNIQUE NOT NULL,
                   network_tier TEXT NOT NULL,
                   plan_id_type TEXT NOT NULL,
                   marketing_name TEXT NOT NULL,
                   summary_url TEXT NOT NULL);
    ''')
    ex('''
CREATE TABLE IndividualProvider (npi INTEGER PRIMARY KEY,
                                 name_first TEXT NOT NULL,
                                 name_last TEXT NOT NULL,
                                 last_updated_on INTEGER NOT NULL,
                                 address_address TEXT NOT NULL,
                                 address_city TEXT NOT NULL,
                                 address_state TEXT NOT NULL,
                                 address_zip TEXT NOT NULL,
                                 address_phone TEXT NOT NULL);
    ''')
    ex('''
CREATE TABLE FaciltyProvider (npi INTEGER PRIMARY KEY,
                              last_updated_on INTEGER NOT NULL,
                              address_address TEXT NOT NULL,
                              address_city TEXT NOT NULL,
                              address_state TEXT NOT NULL,
                              address_zip TEXT NOT NULL,
                              address_phone TEXT NOT NULL);
    ''')
    ex('''
CREATE TABLE Language (idx_language INTEGER PRIMARY KEY AUTOINCREMENT,
                       language TEXT NOT NULL);
    ''')
    ex('''
CREATE TABLE Specialty (idx_specialty INTEGER PRIMARY KEY AUTOINCREMENT,
                        specialty TEXT NOT NULL);
    ''')
    ex('''
CREATE TABLE FacilityType (idx_facility_type INTEGER PRIMARY KEY AUTOINCREMENT,
                           facility_type TEXT NOT NULL);
    ''')
    ex('''
CREATE TABLE Provider_Plan (npi INTEGER UNIQUE NOT NULL,
                            idx_plan INTEGER UNIQUE NOT NULL);
    ''')
    conn.commit()
    return conn


def close_db(conn, commit=True):
    if commit:
        conn.commit()
    conn.close()
