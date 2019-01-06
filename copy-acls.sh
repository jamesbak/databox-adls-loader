#!/usr/bin/env bash

source_path=""

noop1() {
    echo $1
}

process_acl_entries() {
    source_path=$1

    while read file; do
        file=$(echo $file | cut -d / -f 4-)
        aclspec=()
        owner=""
        group=""
        for i in 1 2
        do
            read identity
            ownertype=$(echo $identity | cut -d ':' -f 1 | cut -c 3-)
            identity=$(echo $identity | cut -d ':' -f 2 | sed -e 's/^[ \t]*//')
            if [[ $ownertype == "owner" ]]
            then
                owner=$identity
            else
                group=$identity
            fi
        done
        read aclentry
        while [[ $aclentry ]]
        do
            entry_type=$(echo $aclentry | cut -d ':' -f 1)
            entry_identity=$(echo $aclentry | cut -d ':' -f 2)
            permissions=$(echo $aclentry | cut -d ':' -f 3)
            aclspec+=("$entry_type:$entry_identity:$permissions")
            read aclentry
        done
        echo "'$file'" "'$owner'" "'$group'" "${aclspec[@]}"
    done < <(hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -getfacl -R $source_path)
}

while getopts "s:" option; 
do
    case "${option}" in
        s)
            source_path=${OPTARG}
            ;;
    esac
done
if [[ -z $source_path ]]
then
    echo "Usage: $0 {-s source_path}" >&2
    exit 1
fi

echo "Copying ACLs from $source_path" >&2
process_acl_entries $source_path | jq -R 'split(" ") | {file:.[0], owner:.[1], group:.[2], acl:.[3:]}' | jq -s '.' | tr -d "'" 
