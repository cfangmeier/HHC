#!/usr/bin/env python3
import io
import os
import json
import csv
import logging
import zipfile
import concurrent.futures
import urllib.request as request
from collections import namedtuple

import openpyxl

NULL_URL = "NOT SUBMITTED"


Issuer = namedtuple('Issuer', ['marketplace_category',
                               'state',
                               'issuer_id',
                               'issuer_name',
                               'url_submitted',
                               'provider_urls',
                               'formulary_urls',
                               'plan_urls',
                               'providers',
                               'tech_poc_email'])

Facility = namedtuple('Facility', ['addresses',
                                   'facility_name',
                                   'facility_type',
                                   'last_updated_on',
                                   'npi',
                                   'plans'])

Individual = namedtuple('Individual', ['addresses',
                                       'accepting',
                                       'gender',
                                       'languages',
                                       'last_updated_on',
                                       'name',
                                       'npi',
                                       'plans',
                                       'speciality'])

Plan = namedtuple('Plan', ['network_tier',
                           'plan_id',
                           'plan_id_type'])

Address = namedtuple('Address', ['address',
                                 'city',
                                 'state',
                                 'zip',
                                 'phone'])

Name = namedtuple('Name', ['first','middle', 'last'])

logging.basicConfig(filename='healthcare_cms_pull.log', level=logging.INFO)


def get_zip_file(url="http://download.cms.gov/marketplace-puf/2016/machine-readable-url-puf.zip"):
    logging.info("Pulling CMS Spreadsheet from {}".format(url))
    with request.urlopen(url) as f:
        b = io.BytesIO(f.read())
    return zipfile.ZipFile(b)


def read_cms_data():
    zf = get_zip_file()
    fname = zf.namelist()[0]
    nb = openpyxl.load_workbook(zf.open(fname))
    ws = nb.active
    issuers = []
    current_row = 1
    logging.info("Parsing CMS Spreadsheet")
    while ws.cell(row=current_row+1, column=1).value:
        current_row += 1
        i = Issuer(marketplace_category = ws.cell(row=current_row, column=1).value,
                   state                = ws.cell(row=current_row, column=2).value,
                   issuer_id            = ws.cell(row=current_row, column=3).value,
                   issuer_name          = ws.cell(row=current_row, column=4).value,
                   url_submitted        = ws.cell(row=current_row, column=5).value,
                   tech_poc_email       = ws.cell(row=current_row, column=6).value,
                   provider_urls  =[],
                   formulary_urls =[],
                   plan_urls      =[],
                   providers      =[],
                   )
        if i.url_submitted == NULL_URL:
            logging.error("Missing json url for Issuer: {}, {}".format(i.issuer_name, i.issuer_id))
            continue
        logging.info("Issuer Parsed: {}".format(i.issuer_name))
        issuers.append(i)
    return issuers


def pull_issuer_urls(issuer):
    logging.info("pulling urls for issuer {}".format(issuer.issuer_name))
    try:
        with request.urlopen(issuer.url_submitted, timeout=20) as conn:
            try:
                js = json.loads(conn.read().decode('utf8'))
            except Exception:
                logging.warning("Error parsing redirect data from {}, {}".format(issuer.name, e))
            else:
                issuer.provider_urls.extend(js['provider_urls'])
                issuer.formulary_urls.extend(js['formulary_urls'])
                issuer.plan_urls.extend(js['plan_urls'])
    except Exception as e:
        logging.warning("Error loading redirect data from {}, {}".format(issuer.name, e))
    logging.info("finished pulling issuer urls for {}".format(issuer.issuer_name))


def parse_addresses(addresses_raw):
    addresses = []
    for address_raw in addresses_raw:
        address = Address(address=address_raw['address'],
                          city=address_raw['city'],
                          state=address_raw['state'],
                          zip=address_raw['zip'],
                          phone=address_raw['phone'])
        addresses.append(address)
    return addresses


def parse_plans(plans_raw):
    plans = []
    for plan_raw in plans_raw:
        plan = Plan(network_tier=plan_raw['network_tier'],
                    plan_id     =plan_raw['plan_id'],
                    plan_id_type=plan_raw['plan_id_type'])
        plans.append(plan)
    return plans


def parse_name(name_raw):
    first = name_raw.get('first','')
    middle = name_raw.get('middle','')
    last = name_raw.get('last','')
    return ' '.join([first,middle,last])


def parse_individual_provider(provider_raw):
    individual = Individual(accepting      =provider_raw.get('accepting', []),
                            gender         =provider_raw.get('gender'),
                            languages      =provider_raw.get('languages', []),
                            last_updated_on=provider_raw.get('last_updated_on'),
                            npi            =provider_raw.get('npi'),
                            speciality     =provider_raw.get('speciality'),
                            name           =parse_name(provider_raw.get('name',{})),
                            addresses      =parse_addresses(provider_raw.get('addresses',[])),
                            plans          =parse_plans(provider_raw.get('plans',[])))
    return individual


def parse_facility_provider(provider_raw):
    facility = Facility(facility_name  =provider_raw['facility_name'],
                        facility_type  =provider_raw['facility_type'],
                        last_updated_on=provider_raw['last_updated_on'],
                        npi            =provider_raw['npi'],
                        addresses      =parse_addresses(provider_raw['addresses']),
                        plans          =parse_plans(provider_raw['plans']))
    return facility

def parse_providers(providers_raw):
    providers = []
    for provider_raw in providers_raw:
        provider_type = provider_raw['type']
        if provider_type=='INDIVIDUAL':
            providers.append(parse_individual_provider(provider_raw))
        elif provider_type=='FACILITY':
            providers.append(parse_facility_provider(provider_raw))
        else:
            logging.error("unknown provider type {}".format(provider_type))
    return providers


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


def pull_issuer(issuer):
    logging.info("pulling issuer data for {}".format(issuer.issuer_name))
    pull_issuer_urls(issuer)
    logging.info("pulled issuer urls {}".format(issuer.provider_urls))
    dir = "json/{}".format(issuer.issuer_id)
    if not os.path.isdir(dir):
        os.mkdir(dir)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(pull_providers_page, issuer, url) for url in issuer.provider_urls]
        concurrent.futures.wait(futures)
    return issuer


def pull_all():
    if not os.path.isdir("json"):
        os.mkdir("json")

    issuers = read_cms_data()
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(pull_issuer, i) for i in issuers]
        concurrent.futures.wait(futures)


CSV_FIELDS = [
              'ISSUER_NAME',
              'PROVIDER_NAME',
              'PROVIDER_TYPE',
              'PLAN_ID'
             ]
def save_to_csv(issuer):
    out_file = open(out_filename,'w')
    csv_writer = csv.DictWriter(out_file, CSV_FIELDS)
    csv_writer.writeheader()
    logging.info("Writing issuer \"{}\" data to csv".format(issuer.issuer_name))
    for provider in issuer.providers:
        for plan in provider.plans:
            if type(provider) == Facility:
                name = provider.facility_name
                type_ = "Facility"
            else:
                name = provider.name
                type_ = "Individual"
            all_fields = {'ISSUER_NAME':issuer.issuer_name,
                          'PROVIDER_NAME': name,
                          'PROVIDER_TYPE': type_,
                          'PLAN_ID': plan.plan_id,
                          }
            csv_writer.writerow(all_fields)
    out_file.close()


def main():
    pull_all()

if __name__ == "__main__":
    main()
