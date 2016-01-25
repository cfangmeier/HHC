import sqlite3


def init_db(issuer_id):
    conn = sqlite3.connect("{}.db".format(issuer_id))
    cur = conn.cursor()
    ex = cur.execute
    ex('''CREATE TABLE Issuer (idx_issuer INTEGER PRIMARY KEY AUTOINCREMENT,
                               name TEXT,
                               id INTEGER,
                               marketplace_category TEXT,
                               state TEXT);''')
    ex('''CREATE TABLE Plan (idx_plan INTEGER PRIMARY KEY AUTOINCREMENT,
                             network_tier TEXT,
                             plan_id TEXT,
                             plan_id_type TEXT,
                             idx_issuer INTEGER,
                             FOREIGN KEY(idx_issuer) REFERENCES Issuer(idx_issuer);''')
