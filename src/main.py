#!/usr/bin/env python3
import io
import os
import json
import logging
import zipfile
import argparse
import collections
import multiprocessing
import concurrent.futures
import urllib.request as request

import openpyxl

from json_list_parser import json_list_parser, format_progress
import models
import db

NULL_URL = "NOT SUBMITTED"
LOGGER = logging.getLogger('main')
ch = logging.FileHandler("main.log", mode='w')
ch.setFormatter(logging.Formatter(fmt="%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
LOGGER.addHandler(ch)

PARTIAL_DATA = False


def get_cms_spreadsheet(url):
    LOGGER.info("Pulling CMS Spreadsheet from {}".format(url))
    with request.urlopen(url) as f:
        b = io.BytesIO(f.read())
    zf = zipfile.ZipFile(b)
    fname = zf.namelist()[0]
    nb = openpyxl.load_workbook(zf.open(fname))
    LOGGER.info("Sucessfully pulled CMS Spreadsheet".format(url))
    return nb.active

def init_index_db(cms_url):
    ws = get_cms_spreadsheet(cms_url)
    issuer_groups = collections.defaultdict(list)
    current_row = 1
    LOGGER.info("BEGIN PARSING CMS SPREADSHEET")
    while ws.cell(row=current_row+1, column=1).value:
        current_row += 1
        i = models.Issuer()
        i.id_issuer            = ws.cell(row=current_row, column=3).value
        i.name                 = ws.cell(row=current_row, column=4).value
        i.marketplace_category = ws.cell(row=current_row, column=1).value
        i.url_submitted        = ws.cell(row=current_row, column=5).value
        i.state                = ws.cell(row=current_row, column=2).value.lower()
        i.plans = []
        url_submitted = ws.cell(row=current_row, column=5).value
        if url_submitted == NULL_URL:
            LOGGER.warning("Missing json url for Issuer: {}, {}".format(i.name, i.id_issuer))
        LOGGER.info("Issuer Parsed: {}".format(i.name))
        issuer_groups[url_submitted].append(i)
    LOGGER.info("FINISH PARSING CMS SPREADSHEET")

    group_objs = []
    for url, issuers in issuer_groups.items():
        issuer_group = models.IssuerGroup()
        issuer_group.url_submitted = url
        issuer_group.issuers = issuers
        group_objs.append(issuer_group)
    conn = db.create_index_db()
    db.insert_issuer_groups(conn, group_objs)
    return conn

def pull_issuer_group_index(issuer_group):
    if issuer_group.url_submitted == NULL_URL:
        return issuer_group
    LOGGER.info("PULLING JSON INDEX FOR ISSUER GROUP {} @ {}".format(issuer_group.idx_issuer_group,
                                                                     issuer_group.url_submitted))
    try:
        with request.urlopen(issuer_group.url_submitted, timeout=20) as conn:
            js = json.loads(conn.read().decode('utf8'))
            issuer_group.plan_urls = js['plan_urls']
            issuer_group.provider_urls = js['provider_urls']
            issuer_group.formulary_urls = js['formulary_urls']
            issuer_group.url_status = "good"
            LOGGER.info("FINISHED PULLING JSON INDEX FOR ISSUER GROUP {}".format(issuer_group.idx_issuer_group))
    except Exception as e:
        issuer_group.url_status = "bad"
        LOGGER.exception(e)
        LOGGER.error("FAILED TO LOAD JSON INDEX FOR ISSUER GROUP {} FROM URL \"{}\" ".format(issuer_group.idx_issuer_group, issuer_group.url_submitted, e))
    return issuer_group

def pull_plan_fulldata(conn, issuers, plan_url):
    LOGGER.info("Pulling plan page at {}".format(plan_url))
    plans = {}
    try:
        req = request.urlopen(plan_url, timeout=20)
        file_size = req.info().get('Content-Length')
        try:
            file_size = int(file_size)
        except ValueError:
            file_size = 0
        LOGGER.info("Downloading plan json\n\tFile Size: {}Bytes".format(file_size))
        json_objs = json_list_parser(req, file_size)
        for i,((bytes_read,file_size),plan_dict) in enumerate(json_objs):
            try:
                plan, fake_issuer = models.build_plan_from_dict(issuers, plan_dict)
                if fake_issuer:
                    LOGGER.warning("unknown issuer ID Specified {}".format(fake_issuer.id_issuer))
                    db.insert_issuer(conn, fake_issuer)
                    issuers[fake_issuer.id_issuer] = fake_issuer
                if plan.id_plan in plans:
                    LOGGER.warning("duplicate plan id: {}".format(plan.id_plan))
                plans[plan.id_plan] = plan
            except Exception as e:
                LOGGER.exception(e)
                LOGGER.info(plan_dict)
                continue
        db.insert_plans(conn, plans.values())
        conn.commit()
    except Exception as e:
        LOGGER.exception(e)
        LOGGER.warning("ERROR LOADING PLAN DATA FROM {}".format(plan_url))
    return plans

def pull_provider_fulldata(conn, issuers, plans, provider_url):
    try:
        req = request.urlopen(provider_url)
        json_objs = json_list_parser(req)
        providers = []
        for i,((bytes_read,file_size),provider_dict) in enumerate(json_objs):
            try:
                provider, fake_plans, fake_issuers = models.build_provider_from_dict(issuers, plans, provider_dict)
                if fake_issuers:
                    LOGGER.warning("Found provider {} with undocumented issuer: {}".format(provider.name,list(fake_issuers.keys())))
                    db.insert_issuers(conn, fake_issuers.values())
                    issuers.update(fake_issuers)
                if fake_plans:
                    LOGGER.warning("Found provider {} with undocumented plans: {}".format(provider.name,list(fake_plans.keys())))
                    db.insert_plans(conn, fake_plans.values())
                    plans.update(fake_plans)
                if fake_plans:
                    LOGGER.warning("Found provider {} with undocumented plans: {}".format(provider.name,fake_plans.keys()))
                providers.append(provider)
            except Exception as e:
                LOGGER.exception(e)
                LOGGER.info(str(provider_dict))
                continue
            if((i%1000) == 0):
                s = format_progress(bytes_read, file_size)
                LOGGER.info("Downloaded {}".format(s))
                LOGGER.info("Parsed {} Providers".format(i))
                db.insert_providers(conn, providers)
                conn.commit()
                providers.clear()
                if PARTIAL_DATA and i>500: break;
        db.insert_providers(conn, providers)
        conn.commit()
    except Exception as e:
        LOGGER.exception(e)
        LOGGER.warning("ERROR LOADING PROVIDER DATA FROM {}".format(provider_url))

def pull_formulary_fulldata(conn, issuers, plans, formulary_url):
    try:
        req = request.urlopen(formulary_url)
        json_objs = json_list_parser(req)
        drugs = []
        for i,((bytes_read,file_size),drug_dict) in enumerate(json_objs):
            try:
                drug, fake_plans, fake_issuers = models.build_drug_from_dict(issuers, plans, drug_dict)
                if fake_issuers:
                    LOGGER.warning("Found drug {} with undocumented issuer: {}".format(drug.drug_name,list(fake_issuers.keys())))
                    db.insert_issuers(conn, fake_issuers.values())
                    issuers.update(fake_issuers)
                if fake_plans:
                    LOGGER.warning("Found drug {} with undocumented plans: {}".format(drug.drug_name,list(fake_plans.keys())))
                    db.insert_plans(conn, fake_plans.values())
                    plans.update(fake_plans)
                drugs.append(drug)
            except Exception as e:
                LOGGER.exception(e)
                continue
            if((i%100) == 0):
                s = format_progress(bytes_read, file_size)
                LOGGER.info("Downloaded {}".format(s))
                LOGGER.info("Parsed {} Drugs".format(i))
                db.insert_drugs(conn, drugs)
                conn.commit()
                drugs.clear()
                if PARTIAL_DATA and i>500: break;
        db.insert_drugs(conn, drugs)
        conn.commit()
    except Exception as e:
        LOGGER.exception(e)
        LOGGER.warning("ERROR LOADING DRUG DATA FROM {}".format(formulary_url))

def init_full_pull_logger(id_issuer):
    global LOGGER
    LOGGER = logging.Logger("FULLPULL:"+str(id_issuer))
    log_name = os.path.join("db","{}.log".format(id_issuer))
    ch = logging.FileHandler(log_name, mode='w')
    ch.setFormatter(logging.Formatter(fmt="%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
    LOGGER.addHandler(ch)

def pull_issuer_group_fulldata(args):
    issuer_group, issuers_all = args
    init_full_pull_logger(issuer_group.idx_issuer_group)
    conn = db.init_issuer_group_db(issuer_group.idx_issuer_group)
    for issuer in issuer_group.issuers:
        db.insert_issuer(conn, issuer)
    LOGGER.info("PULLING FULLDATA FOR ISSUER GROUP {}".format(issuer_group.idx_issuer_group))
    LOGGER.info("WITH ISSUERS {}".format([i.id_issuer for i in issuer_group.issuers]))
    plans = {}

    LOGGER.info("BEGIN PULLING PLAN DATA")
    for plan_url in issuer_group.plan_urls:
        plans.update(pull_plan_fulldata(conn, issuers_all, plan_url))
    LOGGER.info("PULLED {} PLANS".format(len(plans)))

    LOGGER.info("BEGIN PULLING FORMULARY DATA")
    n = len(issuer_group.formulary_urls)
    for i, formulary_url in enumerate(issuer_group.formulary_urls):
        LOGGER.info("Pulling formulary page {} of {} at {}".format(i+1, n, formulary_url))
        pull_formulary_fulldata(conn, issuers_all, plans, formulary_url)
    LOGGER.info("FINISHED PULLING FORMULARY DATA")

    LOGGER.info("BEGIN PULLING PROVIDER DATA")
    n = len(issuer_group.provider_urls)
    for i, provider_url in enumerate(issuer_group.provider_urls):
        LOGGER.info("Pulling provider page {} of {} at {}".format(i+1, n, provider_url))
        pull_provider_fulldata(conn, issuers_all, plans, provider_url)
    LOGGER.info("FINISHED PULLING PROVIDER DATA")


    conn.close()
    LOGGER.info("FINISHED PULLING FULLDATA")

def pull_issuer_groups_fulldata(requested_ids, requested_states, processes):
    conn = db.open_index_db()
    groups = db.query_requested_groups(conn, requested_ids, requested_states)
    for group in groups:
        db.query_issuer_group_issuers(conn,group)
        db.query_issuer_group_urls(conn,group)
    LOGGER.info("PULLING DATA FOR ISSUER GROUPS")
    for group in groups:
        LOGGER.info("ISSUER GROUP ID: {}".format(group.idx_issuer_group))
        for issuer in group.issuers:
            LOGGER.info("\tISSUER ID: {}".format(issuer.id_issuer))
    issuers = {i.id_issuer:i for i in db.query_issuers(conn)}
    groups = [(g,issuers) for g in groups]
    conn.close()
    with multiprocessing.Pool(processes) as p:
        p.map(pull_issuer_group_fulldata, groups)

def pull_issuers_metadata(cms_url, requested_ids, requested_states):
    conn = init_index_db(cms_url)
    groups = db.query_requested_groups(conn, requested_ids, requested_states)

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(pull_issuer_group_index, group) for group in groups]
        for future in concurrent.futures.as_completed(futures):
            issuer_group = future.result()
            db.insert_issuer_group_urls(conn, issuer_group)
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Utility to Download Insurance Acceptance Data")
    add = parser.add_argument
    add('--cmsurl', default="http://download.cms.gov/marketplace-puf/2016/machine-readable-url-puf.zip",
            help = "the url to \"machine-readable-url-puf.zip\"")
    add('--issuer_ids', default = None, nargs='+', type=int,
            help="Specify specific issuers to do fulldata pull on")
    add('--states', default = None, nargs='+', type=str,
            help="Specify a list of specific states to download fulldata on")
    add('--processes', default = 1, type=int,
            help="Set the number of processes to use in the full data pull")
    add('action', choices=['init','download', 'all'],
            help="Action to undertake")
    args = parser.parse_args()

    if args.action in ('init', 'all'):
        pull_issuers_metadata(args.cmsurl, args.issuer_ids, args.states)
    if args.action in ('download', 'all'):
        pull_issuer_groups_fulldata(args.issuer_ids, args.states, args.processes)


if __name__ == "__main__":
    main()
