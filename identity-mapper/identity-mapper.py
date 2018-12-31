#!/usr/bin/env python

import requests
import subprocess, datetime, json, itertools, os.path, queue, threading, xml.dom.minidom

source_account="adlsgen2nohnswest2"
source_key="B3dTbA7T5KF2YlbdEX3A6Emxa4QfVthQwifYFNqzJIzBCdQXHu3fEX2hZgzne9FwdYzF9QTxdfelG7rIi4dRMg=="
source_container="databox1"
identity_map_file="./identity_map.json"

sas_token_bytes = subprocess.check_output(["az", "storage", "account", "generate-sas", 
    "--account-name", source_account, 
    "--account-key", source_key, 
    "--services", "b", 
    "--resource-types", "sco", 
    "--permissions", "lwr", 
    "--expiry", (datetime.datetime.utcnow() + datetime.timedelta(2)).strftime("%Y-%m-%dT%H:%MZ")], shell=True)
sas_token = sas_token_bytes.decode("utf-8")[1:-3]

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
        "metadata": x["metadata"]
    } 
    for x 
    in json.load(process.stdout)]
# Load identity map
with open(identity_map_file) as f:
    identity_map = {t: {s["source"]: s["target"] for s in i} 
        for t, i 
        in itertools.groupby(json.load(f), lambda x: x["type"])}

def lookup_identity(identity_type, identity, identity_map):
    retval = ""
    if identity in identity_map[identity_type]:
        retval = identity_map[identity_type][identity]
    else:
        # TODO: Lookup identity in AAD
        retval=identity
    return retval

for item in inventory:
    permissions = json.loads(item["metadata"]["hdi_permission"])
    permissions["owner"] = lookup_identity("user", permissions["owner"], identity_map)
    permissions["group"] = lookup_identity("group", permissions["group"], identity_map)
    item["metadata"]["hdi_permission"] = json.dumps(permissions)
    url = "http://{0}.blob.core.windows.net/{1}/{2}?comp=metadata&{3}".format(source_account, source_container, item["name"], sas_token)
    print(url)
    response = requests.put(url, 
        headers={**{
                "x-ms-version": "2018-03-28",
                "x-ms-date": datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
            },
            **{"x-ms-meta-" + name: value for (name, value) in item["metadata"].items()}
        }
    )
    if not response:
        print(response.text)

