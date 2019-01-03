#!/usr/bin/env python

import requests
import sys, subprocess, datetime, json, itertools, os.path, threading, argparse, logging
try:
    import queue
except ImportError:
    import Queue as queue

log = logging.getLogger(__name__)

def lookup_identity(identity_type, identity, identity_map):
    retval = ""
    if identity in identity_map[identity_type]:
        retval = identity_map[identity_type][identity]
    else:
        # TODO: Lookup identity in AAD
        retval=identity
    return retval

def update_files_owners(files_queue, account, container, sas_token, stop_event):
    log = logging.getLogger(threading.currentThread().name)
    log.debug("Thread starting: %d", threading.currentThread().ident)
    while not stop_event.is_set():
        try:
            file = files_queue.get(True, 5)
            file["permissions"]["owner"] = lookup_identity("user", file["permissions"]["owner"], identity_map)
            file["permissions"]["group"] = lookup_identity("group", file["permissions"]["group"], identity_map)
            # Merge the updated information into the other metadata properties, so that we can update in 1 call
            file["metadata"]["hdi_permission"] = json.dumps(file["permissions"])
            url = "http://{0}.blob.core.windows.net/{1}/{2}?comp=metadata&{3}".format(account, container, file["name"], sas_token)
            log.debug(url)
            # No portable way to combine 2 dicts
            metadata_headers = {
                "x-ms-version": "2018-03-28",
                "x-ms-date": datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
            }
            metadata_headers.update({"x-ms-meta-" + name: value for (name, value) in file["metadata"].items()})
            with requests.put(url, headers=metadata_headers) as response:
                if not response:
                    log.warning("Failed to set metadata on file: %s. Error: %s", url, response.text)
                else:
                    files_queue.task_done()
        except queue.Empty:
            pass
    log.debug("Thread ending")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Remaps identities on HDFS sourced data")
    parser.add_argument('-s', '--source-account', required=True, help="The name of the storage account to process")
    parser.add_argument('-k', '--source-key', required=True, help="The storage account key")
    parser.add_argument('-c', '--source-container', required=True, help="The name of the storage account container")
    parser.add_argument('-i', '--identity-map', default="./identity_map.json", help="The name of the JSON file containing the initial map of source identities to target identities")
    parser.add_argument('-p', '--prefix', default='""', help="A prefix that constrains the processing. Use this option to process entire account on multiple instances")
    parser.add_argument('-g', '--generate-identity-map', action='store_true', help="Specify this flag to generate a based identity mapping file using the unique identities in the source account. The identity map will be written to the file specified by the --identity-map argument.")
    parser.add_argument('-t', '--max-parallelism', type=int, default=10, help="The number of threads to process this work in parallel")
    parser.add_argument('-f', '--log-config', help="The name of a configuration file for logging.")
    parser.add_argument('-l', '--log-file', help="Name of file to have log output written to (default is stdout/stderr)")
    parser.add_argument('-v', '--log-level', default="INFO", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Level of log information to output. Default is 'INFO'.")
    args = parser.parse_known_args()[0]

    if args.log_config:
        logging.config.fileConfig(args.log_config)
    else:
        logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=getattr(logging, args.log_level.upper()), filename=args.log_file)
    print("Remapping identities for file owners in account: " + args.source_account)
    # Acquire SAS token, so that we don't have to sign each request (construct as string as Python 2.7 on linux doesn't marshall the args correctly with shell=True)
    log.debug("Acquiring SAS token")
    sas_token_bytes = subprocess.check_output("az storage account generate-sas --account-name {0} --account-key {1} --services b --resource-types sco --permissions lwr --expiry {2} --output json".format(
            args.source_account, 
            args.source_key, 
            (datetime.datetime.utcnow() + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%MZ")),
        shell=True)
    sas_token = json.loads(sas_token_bytes.decode("utf-8"))

    # Get the full account list 
    log.debug("Fetching complete file list")
    process = subprocess.Popen("az storage blob list --account-name {0} --account-key {1} --container-name {2} --prefix {3} --output json --num-results 1000000000 --include m".format(
            args.source_account, 
            args.source_key, 
            args.source_container, 
            args.prefix),
        stdout=subprocess.PIPE,
        shell=True)
    inventory = [{
            "name": x["name"], 
            "metadata": x["metadata"],
            "permissions": json.loads(x["metadata"]["hdi_permission"])
        } 
        for x 
        in json.load(process.stdout)]
    if args.generate_identity_map:
        print("Generating identity map from source account to file: " + args.identity_map)
        unique_users = set([x["permissions"]["owner"] for x in inventory])
        unique_groups = set([x["permissions"]["group"] for x in inventory])
        identities = [{
            "type": identity_type["type"],
            "source": identity,
            "target": ""
        } for identity_type in [{"type": "user", "identities": unique_users}, {"type": "group", "identities": unique_groups}]
        for identity in identity_type["identities"]]
        with open(args.identity_map, "w+") as f:
            json.dump(identities, f)
    else:
        # Load identity map
        with open(args.identity_map) as f:
            identity_map = {t: {s["source"]: s["target"] for s in i} 
                for t, i 
                in itertools.groupby(json.load(f), lambda x: x["type"])}

        # Fire up the processing in args.max_parallelism threads, co-ordinated via a thread-safe queue
        stop_event = threading.Event()
        files_to_process = queue.Queue()
        log.debug("Processing %d files using %d threads", len(inventory), args.max_parallelism)
        threads = [threading.Thread(target=update_files_owners, args=(files_to_process, args.source_account, args.source_container, sas_token, stop_event)) for _ in range(args.max_parallelism)]
        for file in inventory:
            files_to_process.put(file)
        for thread in threads:
            thread.daemon = True
            thread.start()
        # Wait for the queue to be drained
        files_to_process.join()
        log.debug("Queue has been drained")
        # Kill thr threads
        stop_event.set()
        for thread in threads:
            thread.join()
    print("All work processed. Exiting")

