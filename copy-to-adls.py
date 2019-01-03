#!/usr/bin/env python

import requests
import subprocess, datetime, json, itertools, os.path, queue, threading

access_token=""
token_refresh_time=datetime.datetime.utcnow()

def check_access_token(client_id, client_secret):
    # Check for renewal
    global token_refresh_time, access_token
    if datetime.datetime.utcnow() > token_refresh_time:
        auth_request = requests.post("https://login.microsoftonline.com/common/oauth2/v2.0/token", 
            data={
                "client_id": client_id, 
                "client_secret": client_secret,
                "scope": "https://storage.azure.com/.default",
                "grant_type": "client_credentials"
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            })
        token_response = auth_request.json()
        if auth_request:
            token_refresh_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=token_response["expires_in"])
            access_token = token_response["access_token"]
        else:
            raise IOError(token_response)
    return "Bearer " + access_token

def add_identity_header(headers, identity_type, identity, header, identity_map):
    if identity in identity_map[identity_type]:
        headers[header] = identity_map[identity_type][identity]
    else:
        # TODO: Lookup identity in AAD
        test=0

def copy_files(files_queue, stop_event):
    print("Thread starting")
    while not stop_event.is_set():
        try:
            file = files_queue.get(True, 5)
            # TODO: Copy the file
            print(file["name"])
            files_queue.task_done()
        except queue.Empty as e:
            print(e)
    print("Thread ending")

source_account="adlsgen2nohnswest2"
source_key=""
source_container="databox1"
dest_account="adlsgen2hnswestus2"
dest_container="databox1"
dest_spn_id=""
dest_spn_secret=""
identity_map_file="./identity_map.json"

sas_token = subprocess.check_output(["az", "storage", "account", "generate-sas", 
    "--account-name", source_account, 
    "--account-key", source_key, 
    "--services", "b", 
    "--resource-types", "s", 
    "--permissions", "lr", 
    "--expiry", (datetime.datetime.utcnow() + datetime.timedelta(2)).strftime("%Y-%m-%dT%H:%MZ")], shell=True)

process = subprocess.Popen(["az", "storage", "blob", "list", 
    "--account-name", source_account,
    "--account-key", source_key,
    "--container-name", source_container,
    "--output", "json", 
    "--num-results", "1000000000",
    "--include", "m"],
    stdout=subprocess.PIPE,
    shell=True)
inventory = [{
        "name": x["name"], 
        "parent_directory": os.path.dirname(x["name"]),
        "metadata": {k: v for k, v in x["metadata"].items()
            if k not in {"hdi_isfolder", "hdi_permission"}}, 
        "is_folder": "hdi_isfolder" in x["metadata"], 
        "permissions": json.loads(x["metadata"]["hdi_permission"])
    } 
    for x 
    in json.load(process.stdout)]
# Load identity map
with open(identity_map_file) as f:
    identity_map = {t: {s["source"]: s["target"] for s in i} 
        for t, i 
        in itertools.groupby(json.load(f), lambda x: x["type"])}
    
# Create the directories first
for directory in [x for x in inventory 
                    if x["is_folder"]]:
    dir_base_url = "https://{0}.dfs.core.windows.net/{1}/{2}".format(dest_account, dest_container, directory["name"])
    dir_url = "{0}?resource=directory".format(dir_base_url)
    print(dir_url)
    dir_request = requests.put(dir_url,
        headers = {
            "x-ms-version": "2018-06-17",
            "content-length": "0", 
            "x-ms-permissions": directory["permissions"]["permissions"],
            "x-ms-umask": "0000",
            "Authorization": check_access_token(dest_spn_id, dest_spn_secret)
        })
    if not dir_request:
        raise IOError(dir_request.json())
    else:
        headers = {
            "x-ms-version": "2018-06-17",
            "content-length": "0",
            "Authorization": check_access_token(dest_spn_id, dest_spn_secret)
        }
        add_identity_header(headers, "user", directory["permissions"]["owner"], "x-ms-owner", identity_map)
        add_identity_header(headers, "group", directory["permissions"]["group"], "x-ms-group", identity_map)
        dir_url = "{0}?action=setAccessControl".format(dir_base_url)
        print(dir_url)
        print(headers)
        dir_request = requests.patch(dir_url, headers=headers)
        if not dir_request:
            raise IOError(dir_request.json())
        
max_parallelism=1
stop_event = threading.Event()
files_to_copy = queue.Queue()
threads = [threading.Thread(target=copy_files, args=(files_to_copy, stop_event)) for _ in range(max_parallelism)]
for file in inventory:
    if not file["is_folder"]:
        files_to_copy.put(file)

for thread in threads:
    thread.daemon = True
    thread.start()

files_to_copy.join()
stop_event.set()

for thread in threads:
    thread.join()


