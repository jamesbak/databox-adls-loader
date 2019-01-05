#!/bin/sh

while read file; do
    echo $file
    file=$(echo $file | cut -d / -f 4-)
    aclspec=()
    for i in 1 2
    do
        read owner
        ownertype=$(echo $owner | cut -d ':' -f 1 | cut -c 3-)
        owner=$(echo $owner | cut -d ':' -f 2 | sed -e 's/^[ \t]*//')
        aclspec+=("$ownertype::$owner")
    done
    read aclentry
    while [[ $aclentry ]]
    do
        aclspec+=($aclentry)
        read aclentry
    done
    echo "File: $file, ${aclspec[*]}"
done < <(hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -getfacl -R abfs://databox1@adlsgen2hnswestus2.dfs.core.windows.net/)