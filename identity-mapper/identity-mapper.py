#!/usr/bin/env python

import requests
import subprocess, datetime, json, itertools, os.path, queue, threading, argparse

def lookup_identity(identity_type, identity, identity_map):
    retval = ""
    if identity in identity_map[identity_type]:
        retval = identity_map[identity_type][identity]
    else:
        # TODO: Lookup identity in AAD
        retval=identity
    return retval

def update_files_owners(files_queue, account, container, sas_token, stop_event):
    print("Thread starting")
    while not stop_event.is_set():
        try:
            file = files_queue.get(True, 5)
            permissions = json.loads(file["metadata"]["hdi_permission"])
            permissions["owner"] = lookup_identity("user", permissions["owner"], identity_map)
            permissions["group"] = lookup_identity("group", permissions["group"], identity_map)
            file["metadata"]["hdi_permission"] = json.dumps(permissions)
            url = "http://{0}.blob.core.windows.net/{1}/{2}?comp=metadata&{3}".format(account, container, file["name"], sas_token)
            print(url)
            with requests.put(url, 
                headers={**{
                        "x-ms-version": "2018-03-28",
                        "x-ms-date": datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
                    },
                    **{"x-ms-meta-" + name: value for (name, value) in file["metadata"].items()}
                }
            ) as response:
                if not response:
                    print(response.text)
                else:
                    files_queue.task_done()
        except queue.Empty as e:
            print(e)
    print("Thread ending")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Remaps identities on HDFS sourced data")
    parser.add_argument('-s', '--source-account', required=True, help="The name of the storage account to process")
    parser.add_argument('-k', '--source-key', required=True, help="The storage account key")
    parser.add_argument('-c', '--source-container', required=True, help="The name of the storage account container")
    parser.add_argument('-i', '--identity-map', default="./identity_map.json", help="The name of the JSON file containing the initial map of source identities to target identities")
    parser.add_argument('-p', '--prefix', default='', help="A prefix that constrains the processing. Use this option to process entire account on multiple instances")
    parser.add_argument('-t', '--max-parallelism', type=int, default=10, help="The number of threads to process this work in parallel")
    args = parser.parse_args()

    # Acquire SAS token, so that we don't have to sign each request
    sas_token_bytes = subprocess.check_output(["az", "storage", "account", "generate-sas", 
        "--account-name", args.source_account, 
        "--account-key", args.source_key, 
        "--services", "b", 
        "--resource-types", "sco", 
        "--permissions", "lwr", 
        "--expiry", (datetime.datetime.utcnow() + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%MZ")], shell=True)
    sas_token = sas_token_bytes.decode("utf-8")[1:-3]

    # Get the full account list
    process = subprocess.Popen(["az", "storage", "blob", "list", 
        "--account-name", args.source_account,
        "--account-key", args.source_key,
        "--container-name", args.source_container,
        "--prefix", args.prefix,
        "--output", "json", 
        "--num-results", "1000000000",
        "--include", "m"],
        stdout=subprocess.PIPE,
        shell=True)
    inventory = [{
            "name": x["name"], 
            "metadata": x["metadata"]
        } 
        for x 
        in json.load(process.stdout)]
    # Load identity map
    with open(args.identity_map) as f:
        identity_map = {t: {s["source"]: s["target"] for s in i} 
            for t, i 
            in itertools.groupby(json.load(f), lambda x: x["type"])}

    # Fire up the processing in args.max_parallelism threads, co-ordinated via a thread-safe queue
    stop_event = threading.Event()
    files_to_process = queue.Queue()
    threads = [threading.Thread(target=update_files_owners, args=(files_to_process, args.source_account, args.source_container, sas_token, stop_event)) for _ in range(args.max_parallelism)]
    for file in inventory:
        files_to_process.put(file)
    for thread in threads:
        thread.daemon = True
        thread.start()
    # Wait for the queue to be drained
    files_to_process.join()
    print("Queue has been drained")
    # Kill thr threads
    stop_event.set()
    for thread in threads:
        thread.join()
    print("All work processed. Exiting")

