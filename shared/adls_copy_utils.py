#!/usr/bin/env python

import logging, subprocess, json, datetime, os.path, itertools, threading, argparse
from collections import deque
try:
    import queue
except ImportError:
    import Queue as queue

log = logging.getLogger(__name__)

class AdlsCopyUtils():

    IDENTITY_USER = "user"
    IDENTITY_GROUP = "group"

    METDATA_PERMISSIONS = "hdi_permission"
    METADATA_ISFOLDER = "hdi_isfolder"

    @staticmethod
    def configureLogging(log_config, log_level, log_file):
        if log_config:
            logging.config.fileConfig(log_config)
        else:
            logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=getattr(logging, log_level.upper()), filename=log_file)

    @staticmethod
    def createCommandArgsParser(description):
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument('-s', '--source-account', required=True, help="The name of the storage account to process")
        parser.add_argument('-k', '--source-key', required=True, help="The storage account key")
        parser.add_argument('-c', '--source-container', required=True, help="The name of the storage account container")
        parser.add_argument('-i', '--identity-map', default="./identity_map.json", help="The name of the JSON file containing the initial map of source identities to target identities")
        parser.add_argument('-p', '--prefix', default='""', help="A prefix that constrains the processing. Use this option to process entire account on multiple instances")
        parser.add_argument('-t', '--max-parallelism', type=int, default=10, help="The number of threads to process this work in parallel")
        parser.add_argument('-f', '--log-config', help="The name of a configuration file for logging.")
        parser.add_argument('-l', '--log-file', help="Name of file to have log output written to (default is stdout/stderr)")
        parser.add_argument('-v', '--log-level', default="INFO", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Level of log information to output. Default is 'INFO'.")
        return parser

    @staticmethod
    def getSasToken(account, key):
        log.info("Acquiring SAS token")
        sas_token_bytes = subprocess.check_output("az storage account generate-sas --account-name {0} --account-key {1} --services b --resource-types sco --permissions lwr --expiry {2} --output json".format(
                account, 
                key, 
                (datetime.datetime.utcnow() + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%MZ")),
            shell=True)
        return json.loads(sas_token_bytes.decode("utf-8"))

    @staticmethod
    def getSourceFileList(account, key, container, prefix=None):
        log.info("Fetching complete file list")
        process = subprocess.Popen("az storage blob list --account-name {0} --account-key {1} --container-name {2} --prefix {3} --output json --num-results 1000000000 --include m".format(
                account, 
                key, 
                container, 
                prefix),
            stdout=subprocess.PIPE,
            shell=True)
        return [{
                "name": x["name"], 
                "parent_directory": os.path.dirname(x["name"]),
                "is_folder": AdlsCopyUtils.METADATA_ISFOLDER in x["metadata"],
                "permissions": json.loads(x["metadata"][AdlsCopyUtils.METDATA_PERMISSIONS]),
                "length": x["properties"]["contentLength"],
                "metadata": {k: v for k, v in x["metadata"].items()
                    if k not in {AdlsCopyUtils.METADATA_ISFOLDER, AdlsCopyUtils.METDATA_PERMISSIONS}}
            } 
            for x 
            in json.load(process.stdout)]

    @staticmethod
    def loadIdentityMap(map_file_name):
        log.info("Loading identity map from: %s", map_file_name)
        with open(map_file_name) as f:
            return {t: {s["source"]: s["target"] for s in i} 
                for t, i 
                in itertools.groupby(json.load(f), lambda x: x["type"])}

    @staticmethod
    def lookupIdentity(identity_type, identity, identity_map):
        retval = ""
        if identity in identity_map[identity_type]:
            retval = identity_map[identity_type][identity]
        else:
            # TODO: Lookup identity in AAD
            retval=identity
        return retval

    class WorkQueue:
        stop_event = threading.Event()
        work_queue = queue.Queue()

        def __init__(self, work_items):
            for item in work_items:
                self.work_queue.put(item)

        def nextItem(self, timeout=5):
            try:
                return self.work_queue.get(True, timeout)
            except queue.Empty:
                return None

        def itemDone(self):
            self.work_queue.task_done()

        def isDone(self):
            return self.stop_event.is_set()

        def size(self):
            return self.work_queue.qsize()

    @staticmethod
    def processWorkQueue(target, args, work_items, max_parallelism):
        work_queue = AdlsCopyUtils.WorkQueue(work_items)
        log.debug("Processing %d files using %d threads", work_queue.size(), max_parallelism)
        args.extend([work_queue])
        args_tuple = tuple(args)
        threads = [threading.Thread(target=target, args=args_tuple, daemon=True) for _ in range(max_parallelism)]
        for thread in threads:
            thread.start()
        # Wait for the queue to be drained
        work_queue.work_queue.join()
        log.debug("Queue has been drained")
        # Kill the threads
        work_queue.stop_event.set()
        for thread in threads:
            thread.join()

