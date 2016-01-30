#!/usr/bin/env python3
import io
import os
import json
import logging
import zipfile
import multiprocessing
import concurrent.futures
import urllib.request as request

import openpyxl

from json_list_parser import json_list_parser
import models
import db

NULL_URL = "NOT SUBMITTED"
LOGGER = logging.getLogger('HHC')
LOGGER.addHandler(logging.FileHandler("healthcare_cms_pull.log"))


def get_cms_spreadsheet(url="http://download.cms.gov/marketplace-puf/2016/machine-readable-url-puf.zip"):
    LOGGER.info("Pulling CMS Spreadsheet from {}".format(url))
    with request.urlopen(url) as f:
        b = io.BytesIO(f.read())
    zf = zipfile.ZipFile(b)
    fname = zf.namelist()[0]
    nb = openpyxl.load_workbook(zf.open(fname))
    LOGGER.info("Sucessfully pulled CMS Spreadsheet".format(url))
    return nb.active


def get_issuers_from_cms_spreadsheet():
    ws = get_cms_spreadsheet()
    issuers = []
    current_row = 1
    LOGGER.info("BEGIN PARSING CMS SPREADSHEET")
    while ws.cell(row=current_row+1, column=1).value:
        current_row += 1
        i = models.Issuer(
                          id_issuer            = ws.cell(row=current_row, column=3).value,
                          name                 = ws.cell(row=current_row, column=4).value,
                          marketplace_category = ws.cell(row=current_row, column=1).value,
                          url_submitted        = ws.cell(row=current_row, column=5).value,
                          state                = ws.cell(row=current_row, column=2).value,
                          plans = []
                         )
        if i.url_submitted == NULL_URL:
            LOGGER.warning("Missing json url for Issuer: {}, {}".format(i.name, i.id_issuer))
        LOGGER.info("Issuer Parsed: {}".format(i.name))
        issuers.append(i)
    LOGGER.info("FINISH PARSING CMS SPREADSHEET")
    return issuers


def pull_issuer_index(issuer):
    if issuer.url_submitted == NULL_URL:
        return (issuer, [], [])
    LOGGER.info("PULLING JSON INDEX FOR ISSUER {} @ {}".format(issuer.name,
                                                                issuer.url_submitted))
    plan_urls = []
    provider_urls = []
    try:
        with request.urlopen(issuer.url_submitted, timeout=20) as conn:
            js = json.loads(conn.read().decode('utf8'))
            plan_urls = js['plan_urls']
            provider_urls = js['provider_urls']
            LOGGER.info("FINISHED PULLING JSON INDEX FOR {}".format(issuer.name))
    except Exception as e:
        LOGGER.error("ERROR LOADING JSON INDEX FOR {}, {}".format(issuer.name, e))
    return (issuer, plan_urls, provider_urls)


def pull_plan_fulldata(conn, issuer, plan_url):
    LOGGER.info("Pulling plan page at {}".format(plan_url))
    plans = {}
    try:
        req = request.urlopen(plan_url)
        json_objs = json_list_parser(req)
        for plan_raw in json_objs:
            plan = models.build_plan_from_dict(issuer, plan_raw)
            plans[(plan.id_plan,plan.id_issuer)] = plan
        db.insert_plans(conn, plans.values())
    except Exception as e:
        LOGGER.warning("Error loading plan plan data from {}, {}".format(issuer.name, e))
    return plans

def pull_provider_fulldata(conn, issuer, plans, provider_url):
    LOGGER.info("Pulling provider page at {}".format(provider_url))
    try:
        req = request.urlopen(provider_url)
        LOGGER.info("Downloading provider json\n\tFile Size: {}Bytes".format(req.info().get('Content-Length')))
        json_objs = json_list_parser(req)
        providers = []
        i = 0
        for provider_dict in json_objs:
            providers.append(models.build_provider_from_dict(issuer, plans, provider_dict))
            i+=1
            if(len(providers)>1000):
                LOGGER.info("Parsed {} Providers".format(i))
                db.insert_providers(conn, providers)
                conn.commit()
                providers.clear()
        db.insert_providers(conn, providers)
        conn.commit()
    except Exception as e:
        LOGGER.exception(e)
        LOGGER.warning("Error loading plan plan data from {}".format(issuer.name))

def pull_issuer_fulldata(id_issuer):
    global LOGGER
    LOGGER = logging.Logger(str(id_issuer))
    log_name = os.path.join("db","{}.log".format(id_issuer))
    LOGGER.addHandler(logging.FileHandler(log_name))
    LOGGER.info("PULLING FULLDATA FOR ISSUER {}".format(id_issuer))
    conn = db.open_db(id_issuer)
    issuer = db.query_issuer(conn, id_issuer)
    plan_urls, provider_urls = db.query_issuer_urls(conn, issuer)
    plans = {}
    for plan_url in plan_urls:
        plans.update(pull_plan_fulldata(conn, issuer, plan_url))
    LOGGER.info("PULLED {} PLANS FOR ISSUER {}".format(len(plans),id_issuer))
    LOGGER.info("BEGIN PULLING PROVIDER DATA")
    for provider_url in provider_urls:
        pull_provider_fulldata(conn, issuer, plans, provider_url)
    db.close_db(conn)
    LOGGER.info("FINISHED PULLING PROVIDER DATA")


def pull_issuers_fulldata():
    db_filenames = [os.path.splitext(fname) for fname in os.listdir("db")]
    ids_issuer = [base for (base,ext) in db_filenames if ext == ".sqlite3"]
    with multiprocessing.Pool(5) as p:
        p.map(pull_issuer_fulldata, ids_issuer)


def pull_issuers_metadata():
    issuers = get_issuers_from_cms_spreadsheet()[:5]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(pull_issuer_index, issuer) for issuer in issuers]
        for future in concurrent.futures.as_completed(futures):
            issuer, plan_urls, provider_urls = future.result()
            conn = db.init_db(issuer.id_issuer)
            db.insert_issuer(conn, issuer)
            db.insert_issuer_urls(conn, issuer, plan_urls, provider_urls)
            db.close_db(conn)


def main():
    pull_issuers_metadata()
    pull_issuers_fulldata()

if __name__ == "__main__":
    main()
