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
import cProfile

import openpyxl

from json_list_parser import json_list_parser, format_progress
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
    data_limit = 50
    download_attempts = 3

    def __init__(self, issuer_group, queue, label):
        self.issuer_group = issuer_group
        self.q = queue
        self.logger = init_logger("DL_{}".format(label))

    def run(self):
        log = self.logger
        success = self._download_index()
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


    def _download_index(self):
        log = self.logger
        iss_grp = self.issuer_group
        if iss_grp.index_url == NULL_URL:
            return
        log.info("Pulling JSON index from \"{}\"".format(iss_grp.index_url))
        try:
            with request.urlopen(iss_grp.index_url, timeout=20) as conn:
                js = json.loads(conn.read().decode('utf8'))
                iss_grp.data_urls = ([models.IssuerURL(url, models.URLType.plan, 'n/a')
                                      for url in js['plan_urls']] +
                                     [models.IssuerURL(url, models.URLType.prov, 'n/a')
                                      for url in js['provider_urls']] +
                                     [models.IssuerURL(url, models.URLType.drug, 'n/a')
                                      for url in js['formulary_urls']])

                iss_grp.url_status = "good"
                log.info("Finished pulling JSON index from URL \"{}\"".format(iss_grp.index_url))
                self.q.put(iss_grp)
                return True
        except Exception as e:
            iss_grp.url_status = "bad"
            log.exception(e)
            log.error("Failed to load JSON index from URL \"{}\"".format(iss_grp.index_url))
            self.q.put(iss_grp)
            return False

    def _download_objects(self, url, class_):
        log = self.logger

        def status(bytes_read, file_size, i):
            s = format_progress(bytes_read, file_size)
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
            log.info("Finished download of url: {}, size: {} bytes".format(url, file_size))
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

    def _process_issuer_group(self, issuer_group):
        conn = self.conn
        idx = db.insert_issuer_group(conn, issuer_group)
        for issuer in issuer_group.issuers:
            db.insert_issuer(conn, issuer, idx)
        for url in issuer_group.data_urls:
            db.insert_data_url(conn, url, idx)
        conn.commit()

    def _process_url(self, url):
        pass

    def _process_plan(self, plan):
        pass

    def _process_prov(self, prov):
        pass

    def _process_drug(self, drug):
        pass

    def run(self):
        map_ = {models.IssuerGroup: self._process_issuer_group,
                models.IssuerURL:   self._process_url,
                models.Provider:    self._process_prov,
                models.Plan:        self._process_plan,
                models.Drug:        self._process_drug}
        while True:
            obj = self.q.get()
            if obj == "QUIT":
                self.conn.close()
                break
            # self.logger.info(obj)
            map_[type(obj)](obj)


def consume(q):
    consumer = Consumer(q)
    consumer.run()


def produce(args):
    issuer_group, label = args
    producer = Downloader(issuer_group, produce.q, label)
    cProfile.runctx("producer.run()", globals(), locals(),
                    "prof_results.txt")
    # producer.run()


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
            log.info("Filtering on states: {}".format(ids))
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
            log.info("Opening local CMS Spreadsheet from {}".format(parse_result.path))
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
                log.warning("Missing JSON url for Issuer: {}, {}".format(i.name, i.id_issuer))
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
        pool.map(produce, [(grp, str(i))
                           for i, grp in enumerate(self.issuer_groups)])
        q.put("QUIT")
        consume_proc.join()


def main():
    parser = argparse.ArgumentParser(description="Utility to Download Insurance Acceptance Data")
    add = parser.add_argument
    add('--cmsurl', default="http://download.cms.gov/marketplace-puf/2016/machine-readable-url-puf.zip",
        help= ("the url to \"machine-readable-url-puf.zip\", or the path "
               "to \"Machine_Readable_PUF_*.xlsx\" in the form \"file:/path/to/file\""))
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
