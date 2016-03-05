#!/usr/bin/env python3
import io
import json
import logging
import zipfile
import argparse
import collections
import multiprocessing as mp
import urllib.parse as parse
import urllib.request as request

import openpyxl

from json_list_parser import json_list_parser, download_formatter
import models
import db

NULL_URL = "NOT SUBMITTED"


def init_logger(logger_name):
    logger = logging.Logger(logger_name)
    log_name = "{}.log".format(logger_name)
    ch = logging.FileHandler(log_name, mode='w')
    fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    ch.setFormatter(logging.Formatter(fmt=fmt))
    logger.addHandler(ch)
    return logger


class Downloader:
    url_limit = 10
    data_limit = 500
    download_attempts = 3

    def __init__(self, issuer_group, queue, label):
        self.issuer_group = issuer_group
        self.issuer_group.idx_issuer_group = label
        for issuer in self.issuer_group.issuers:
            issuer.idx_issuer_group = label
        self.q = queue
        self.logger = init_logger("DL_{}".format(label))

    def run(self):
        log = self.logger
        success = self._download_index()
        status = "finished" if success else "failed"
        self.issuer_group.index_status = status
        self.q.put(self.issuer_group)
        for issuer in self.issuer_group.issuers:
            self.q.put(issuer)
        if not success:
            return

        data_urls = self.issuer_group.data_urls
        if self.url_limit:
            data_urls = data_urls[:self.url_limit]
        n = len(data_urls)
        for i, url in enumerate(data_urls):
            log.info("Downloading from URL {}/{}".format(i+1, n))
            if url.url_type == models.URLType.plan:
                success = self._download_objects(url.url, models.Plan)
            elif url.url_type == models.URLType.drug:
                success = self._download_objects(url.url, models.Drug)
            elif url.url_type == models.URLType.prov:
                success = self._download_objects(url.url, models.Provider)
            url.status = "finished" if success else "failed"
            self.q.put(url)

    def _download_index(self):
        log = self.logger
        iss_grp = self.issuer_group
        idx = iss_grp.idx_issuer_group
        if iss_grp.index_url == NULL_URL:
            return
        log.info("Pulling JSON index from \"{}\"".format(iss_grp.index_url))
        try:
            with request.urlopen(iss_grp.index_url, timeout=20) as conn:
                js = json.loads(conn.read().decode('utf8'))
                plans = [models.IssuerGroupURL(idx, url, models.URLType.plan)
                         for url in js['plan_urls']]
                provs = [models.IssuerGroupURL(idx, url, models.URLType.prov)
                         for url in js['provider_urls']]
                drugs = [models.IssuerGroupURL(idx, url, models.URLType.drug)
                         for url in js['formulary_urls']]
                iss_grp.data_urls = (plans+provs+drugs)

                fmt = "Finished pulling JSON index from URL \"{}\""
                log.info(fmt.format(iss_grp.index_url))
                return True
        except Exception as e:
            log.exception(e)
            fmt = "Failed to load JSON index from URL \"{}\""
            log.error(fmt.format(iss_grp.index_url))
            return False

    def _download_objects(self, url, class_):
        log = self.logger
        formatter = download_formatter()

        def status(bytes_read, file_size, i):
            s = formatter(bytes_read, file_size)
            log.info("Downloaded {}".format(s))
            log.info("Parsed {} data objects".format(i))
        try:
            json_objs = json_list_parser(url)
            for i, ((bytes_read, file_size), obj_dict) in enumerate(json_objs):
                try:
                    obj = class_(obj_dict)
                    self.q.put(obj)
                except Exception as e:
                    log.exception(e)
                    log.info(obj_dict)
                    continue
                if i > 0 and ((i % 1000) == 0):
                    status(bytes_read, file_size, i)
                if self.data_limit and i > self.data_limit:
                    log.info("Hit data limit, breaking...")
                    status(bytes_read, file_size, i)
                    break
            fmt = "Finished download of url: {}, size: {} MB"
            log.info(fmt.format(url, file_size/2**20))
            return True
        except Exception as e:
            log.exception(e)
            log.warning("Error loading data from {}".format(url))
            return False


class Consumer:
    def __init__(self, queue, label="CS"):
        self.logger = init_logger("DL_{}".format(label))
        self.q = queue
        self.conn = db.init_db()

        # Declare a few auxillary lookup tables
        self.provs = {}  # maps npi to idx_provider
        self.drugs = {}  # maps rx_norm_id to idx_drug

    def _process_issuer_group(self, issuer_group):
        id_ = issuer_group.idx_issuer_group
        self.logger.info("Inserting Issuer Group: {}".format(id_))
        db.insert_issuer_group(self.conn, issuer_group)

    def _process_url(self, url):
        self.logger.info("Inserting URL: {}".format(url))
        db.insert_data_url(self.conn, url)

    def _process_issuer(self, issuer):
        self.logger.info("Inserting Issuer: {}".format(issuer.id_issuer))
        db.insert_issuer(self.conn, issuer)

    def _process_plan(self, plan):
        self.logger.info("Inserting Plan: {}".format(plan.marketing_name))
        db.insert_plan(self.conn, plan)

    def _process_provider(self, prov):
        conn = self.conn
        log = self.logger
        log.info("Inserting Provider: {},{}".format(prov.npi, prov.name))
        if not prov.npi:
            return
        if prov.npi not in self.provs:
            self.provs[prov.npi] = db.insert_provider(conn, prov)
        idx_prov = self.provs[prov.npi]

        for language in prov.languages:
            db.insert_language(conn, language, idx_prov)
        for specialty in prov.specialties:
            db.insert_specialty(conn, specialty, idx_prov)
        for facility_type in prov.facility_types:
            db.insert_facility_type(conn, facility_type, idx_prov)
        for address in prov.addresses:
            db.insert_address(conn, address, idx_prov)
        for plan in prov.plans:
            db.insert_provider_plan(conn, plan, idx_prov)

    def _process_drug(self, drug):
        conn = self.conn
        log = self.logger
        log.info("Inserting Drug: {}".format(drug.name))
        if not drug.rxnorm_id:
            return
        if drug.rxnorm_id not in self.drugs:
            self.drugs[drug.rxnorm_id] = db.insert_drug(conn, drug)
        idx_drug = self.drugs[drug.rxnorm_id]
        for plan in drug.plans:
            db.insert_drug_plan(conn, plan, idx_drug)

    def run(self):
        log = self.logger
        map_ = {models.IssuerGroup:      self._process_issuer_group,
                models.IssuerGroupURL:   self._process_url,
                models.Issuer:           self._process_issuer,
                models.Provider:         self._process_provider,
                models.Plan:             self._process_plan,
                models.Drug:             self._process_drug}
        while True:
            obj = self.q.get()
            if obj == "QUIT":
                log.info("Consumer received quit signal. closing db...")
                self.conn.close()
                break
            else:
                try:
                    map_[type(obj)](obj)
                    self.conn.commit()
                except Exception as e:
                    log.exception(e)
                    self.conn.rollback()


def consume(q):
    consumer = Consumer(q)
    consumer.run()


def produce(args):
    issuer_group, label = args
    producer = Downloader(issuer_group, produce.q, label)
    producer.run()


def init_produce(q):
    produce.q = q


class Manager:
    def __init__(self, cms_url, filters, num_processes=10):
        self.logger = init_logger("MANAGER")
        self.cms_url = cms_url
        self.requested_issuer_ids = filters['issuer_ids']
        self.requested_states = filters['states']
        self.num_processes = num_processes

    def _apply_filters(self):
        log = self.logger
        ids = self.requested_issuer_ids
        states = self.requested_states
        if ids:
            log.info("Filtering on ids: {}".format(ids))
        else:
            log.info("No ids specified, keeping all.")
        if states:
            log.info("Filtering on states: {}".format(states))
        else:
            log.info("No states specified, keeping all.")

        def filt(issuers):
            return [i for i in issuers
                    if ((not states or i.state in states) and
                        (not ids or i.id_issuer in ids))]
        grp_filt = [group
                    for group in self.issuer_groups if filt(group.issuers)]
        log.info("After filtering, will download the following groups:")
        for i, grp in enumerate(grp_filt):
            log.info("Group #{}, index-url: \"{}\"".format(i, grp.index_url))
            iss_ids = [iss.id_issuer for iss in grp.issuers]
            log.info("\tWith Issuers: {}".format(iss_ids))
        self.issuer_groups = grp_filt

    def _find_issuer_groups(self):
        log = self.logger
        parse_result = parse.urlparse(self.cms_url)
        if parse_result.scheme == "file":
            fmt = "Opening local CMS Spreadsheet from {}"
            log.info(fmt.format(parse_result.path))
            nb = openpyxl.load_workbook(open(parse_result.path, 'rb'))
        else:  # Web URL
            log.info("Pulling CMS Spreadsheet from {}".format(self.cms_url))
            with request.urlopen(self.cms_url) as f:
                b = io.BytesIO(f.read())
            zf = zipfile.ZipFile(b)
            fname = zf.namelist()[0]
            nb = openpyxl.load_workbook(zf.open(fname))
            log.info("Sucessfully pulled CMS Spreadsheet".format(self.cms_url))

        issuer_groups = collections.OrderedDict()
        current_row = 1
        log.info("BEGIN PARSING CMS SPREADSHEET")
        ws = nb.active
        while ws.cell(row=current_row+1, column=1).value:
            current_row += 1
            i = models.Issuer()
            i.state = ws.cell(row=current_row, column=1).value.lower()
            i.id_issuer = ws.cell(row=current_row, column=2).value
            i.name = ws.cell(row=current_row, column=3).value
            index_url = ws.cell(row=current_row, column=4).value
            if index_url == NULL_URL:
                fmt = "Missing JSON url for Issuer: {}, {}"
                log.warning(fmt.format(i.name, i.id_issuer))
            log.info("Issuer Parsed: {}".format(i.name))
            if index_url not in issuer_groups:
                issuer_groups[index_url] = []
            issuer_groups[index_url].append(i)
        log.info("FINISH PARSING CMS SPREADSHEET")

        group_objs = []
        for url, issuers in issuer_groups.items():
            issuer_group = models.IssuerGroup()
            issuer_group.index_url = url
            issuer_group.issuers = issuers
            group_objs.append(issuer_group)
        self.issuer_groups = group_objs

    def run(self):
        self._find_issuer_groups()
        self._apply_filters()
        q = mp.Queue()
        consume_proc = mp.Process(target=consume, args=(q,))
        consume_proc.start()
        pool = mp.Pool(self.num_processes, init_produce, [q])
        pool.map(produce, [(grp, i)
                           for i, grp in enumerate(self.issuer_groups)])
        q.put("QUIT")
        consume_proc.join()


def main():
    desc = "Utility to Download Insurance Acceptance Data"
    parser = argparse.ArgumentParser(description=desc)
    add = parser.add_argument
    urlhelp = ("the url to \"machine-readable-url-puf.zip\", or the path "
               "to \"Machine_Readable_PUF_*.xlsx\" in the form "
               "\"file:/path/to/file\"")
    urldefault = ("http://download.cms.gov/marketplace-puf/2016/"
                  "machine-readable-url-puf.zip")
    add('--cmsurl', default=urldefault, help=urlhelp)
    add('--issuerids', default=None, nargs='+', type=int,
        help="Specify specific issuers to do fulldata pull on")
    add('--states', default=None, nargs='+', type=str,
        help="Specify a list of specific states to download fulldata on")
    add('--processes', default=1, type=int,
        help="Set the number of processes to use in the full data pull")
    args = parser.parse_args()

    filters = {'issuer_ids': args.issuerids,
               'states': args.states}
    manager = Manager(args.cmsurl, filters, args.processes)
    manager.run()


if __name__ == "__main__":
    main()
