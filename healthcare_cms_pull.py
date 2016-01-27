#!/usr/bin/env python3
import io
import os
import json
import csv
import logging
import zipfile
import multiprocessing
import concurrent.futures
import urllib.request as request

import openpyxl

from models import Issuer, Plan, Individual, Facility, Address
from models import *
from utils import json_objects_from_file

NULL_URL = "NOT SUBMITTED"


def get_cms_spreadsheet(url="http://download.cms.gov/marketplace-puf/2016/machine-readable-url-puf.zip"):
    logging.info("Pulling CMS Spreadsheet from {}".format(url))
    with request.urlopen(url) as f:
        b = io.BytesIO(f.read())
    zf = zipfile.ZipFile(b)
    fname = zf.namelist()[0]
    nb = openpyxl.load_workbook(zf.open(fname))
    logging.info("Sucessfully pulled CMS Spreadsheet".format(url))
    return nb.active


def get_issuers_from_cms_spreadsheet():
    ws = get_cms_spreadsheet()
    issuers = []
    current_row = 1
    logging.info("BEGIN PARSING CMS SPREADSHEET")
    while ws.cell(row=current_row+1, column=1).value:
        current_row += 1
        i = Issuer(
            id_issuer            = ws.cell(row=current_row, column=3).value,
            name                 = ws.cell(row=current_row, column=4).value,
            marketplace_category = ws.cell(row=current_row, column=1).value,
            url_submitted        = ws.cell(row=current_row, column=5).value,
            state                = ws.cell(row=current_row, column=2).value,
                   )
        if i.url_submitted == NULL_URL:
            logging.warning("Missing json url for Issuer: {}, {}".format(i.name, i.id_issuer))
            continue
        logging.info("Issuer Parsed: {}".format(i.name))
        issuers.append(i)
    logging.info("FINISH PARSING CMS SPREADSHEET")
    return issuers


def pull_issuer_index(issuer):
    logging.info("PULLING JSON INDEX FOR ISSUER {} @ {}".format(issuer.name,
                                                                issuer.url_submitted))
    plan_urls = []
    provider_urls = []
    try:
        with request.urlopen(issuer.url_submitted, timeout=20) as conn:
            js = json.loads(conn.read().decode('utf8'))
            plan_urls = js['plan_urls']
            provider_urls = js['provider_urls']
            logging.info("FINISHED PULLING JSON INDEX FOR {}".format(issuer.name))
    except Exception as e:
        logging.error("ERROR LOADING JSON INDEX FOR {}, {}".format(issuer.name, e))
    return (issuer, plan_urls, provider_urls)


def pull_providers_page(issuer, providers_url):
    logging.info("Pulling provider page at {}".format(providers_url))
    try:
        fname = "json/{}/{}".format(issuer.issuer_id,os.path.split(providers_url)[1])
        if os.path.isfile(fname):
            logging.info("file \"{}\" already exists, Skipping download.".format(fname))
        else:
            request.urlretrieve(providers_url, fname)
    except Exception as e:
        logging.warning("Error loading provider data from {}, {}".format(issuer.issuer_name, e))

IDX_PLAN = 1
def build_plan_from_raw(issuer, plan_raw):
    global IDX_PLAN
    p = Plan(idx_plan=IDX_PLAN,
             id_plan=plan_raw['plan_id'],
             id_issuer=issuer.id_issuer,
             plan_id_type=plan_raw['plan_id_type'],
             marketing_name=plan_raw['marketing_name'],
             summary_url=plan_raw['summary_url'])
    IDX_PLAN+=1
    return p

def build_provider_from_raw(issuer, plans, provider_raw):
    p = Individual(npi = provider_raw['npi'],
                   name_first="Gerald",
                   name_last="Ford",
                   last_updated_on=0,
                   accepting="No, stop asking!")
    if provider_raw['type'] == 'FACILITY':
        pass
    else:
        pass
    return (p, [])

def pull_plan_fulldata(conn, issuer, plan_url):
    logging.info("Pulling plan page at {}".format(plan_url))
    plans = {}
    try:
        req = request.urlopen(plan_url)
        json_objs = json_objects_from_file(req)
        for plan_raw in json_objs:
            plan = build_plan_from_raw(issuer, plan_raw)
            insert_plan(conn, plan)
            plans[(plan.id_plan,plan.id_issuer)] = plan
    except Exception as e:
        logging.warning("Error loading plan plan data from {}, {}".format(issuer.name, e))
    return plans

def pull_provider_fulldata(conn, issuer, plans, provider_url):
    logging.info("Pulling provider page at {}".format(provider_url))
    try:
        req = request.urlopen(provider_url)
        logging.info("Downloading provider json\n\tFile Size: {}Bytes".format(req.info().get('Content-Length')))
        json_objs = json_objects_from_file(req)
        for provider_raw in json_objs:
            logging.info("Parsed Provider with npi {}".format(provider_raw['npi']))
            provider, provider_plans = build_provider_from_raw(issuer, plans, provider_raw)
            insert_provider(conn, provider)
            for provider_plan in provider_plans:
                insert_provider_plan(provider_plan)
    except Exception as e:
        logging.warning("Error loading plan plan data from {}, {}".format(issuer.name, e))

def pull_issuer_fulldata(id_issuer):
    logging.basicConfig(filename="{}.log".format(id_issuer), level=logging.INFO)
    logging.info("PULLING FULLDATA FOR ISSUER {}".format(id_issuer))
    conn = open_db(id_issuer)
    issuer = query_issuer(conn)
    plan_urls = query_plan_urls(conn, issuer)
    provider_urls = query_provider_urls(conn, issuer)
    plans = {}
    for plan_url in plan_urls:
        plans.update(pull_plan_fulldata(conn, issuer, plan_url))
    logging.info("PULLED {} PLANS FOR ISSUER {}".format(len(plans),id_issuer))
    for provider_url in provider_urls:
        pull_provider_fulldata(conn, issuer, plans, provider_url)
    close_db(conn)


def pull_issuers_fulldata():
    db_filenames = [os.path.splitext(fname) for fname in os.listdir("db")]
    ids_issuer = [base for (base,ext) in db_filenames if ext == ".sqlite3"]
    pull_issuer_fulldata(ids_issuer[0])
    # with multiprocessing.Pool(5) as p:
    #     p.map(pull_issuer_fulldata, ids_issuer)


def pull_issuers_metadata():
    issuers = get_issuers_from_cms_spreadsheet()[:10]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(pull_issuer_index, issuer) for issuer in issuers]
        for future in concurrent.futures.as_completed(futures):
            issuer, plan_urls, provider_urls = future.result()
            conn = init_db(issuer.id_issuer)
            insert_issuer(conn, issuer)
            for plan_url in plan_urls:
                insert_plan_url(conn, issuer, plan_url)
            for provider_url in provider_urls:
                insert_provider_url(conn, issuer, provider_url)
            close_db(conn)


def main():
    pull_issuers_metadata()
    pull_issuers_fulldata()

if __name__ == "__main__":
    main()
