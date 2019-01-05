#!/usr/bin/env bash

identity_map=()
identity_map_file=""
source_path=""
dest_path=""
generate_identity_map=""

noop1() {
    echo $1
}

map_identity_type() {
    local identity=$1
    local identity_type=$2

    echo $(jq -r --arg identity $identity --arg identity_type $identity_type '.[] | select(.type == $identity_type and .source == $identity) | .target' $identity_map_file)
}

assign_acls() {
    dest_path=$1
    filename=$2
    shift
    shift
    aclspec=(${@})

    hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -setfacl --set "$(IFS=","; echo "${aclspec[*]}")" $destpath$filename 
}

process_acls() {
    source_path=$1
    dest_path=$2
    assign_file_acl=$3
    map_identity=$4

    while read file; do
        file=$(echo $file | cut -d / -f 4-)
        aclspec=()
        for i in 1 2
        do
            read owner
            ownertype=$(echo $owner | cut -d ':' -f 1 | cut -c 3-)
            identity_type="group"
            if [[ $ownertype == "owner" ]]
            then
                identity_type="user"
            fi
            owner=$($map_identity $(echo $owner | cut -d ':' -f 2 | sed -e 's/^[ \t]*//') $identity_type)
            aclspec+=("$ownertype::$owner")
            identity_map+=("$identity_type:$owner")
        done
        read aclentry
        while [[ $aclentry ]]
        do
            entry_type=$(echo $aclentry | cut -d ':' -f 1)
            entry_identity=$(echo $aclentry | cut -d ':' -f 2)
            permissions=$(echo $aclentry | cut -d ':' -f 3)
            if [[ $entry_identity ]]
            then
                entry_identity=$($map_identity $entry_identity $entry_type)
            fi
            aclspec+=("$entry_type:$entry_identity:$permissions")
            if [[ $entry_identity ]]
            then
                identity_map+=("$entry_type:$entry_identity")
            fi
            read aclentry
        done
        $assign_file_acl "$dest_path" "$file" "${aclspec[@]}"
    done < <(hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -getfacl -R $source_path)
}

while getopts "s:d:i:g" option; 
do
    case "${option}" in
        s)
            source_path=${OPTARG}
            ;;
        d)
            dest_path=${OPTARG}
            ;;
        i)
            identity_map_file=${OPTARG}
            ;;
        g)
            generate_identity_map=true
            ;;
    esac
done
if [[ -z $source_path ]] || [[ -z $identity_map_file ]]
then
    echo "Usage: $0 {-s source_path} [{-d dest_path}] {-i identity_map_file} [-g]"
    exit 1
fi

if [[ $generate_identity_map ]]
then
    echo "Generating identity map file: $identity_map_file"
    process_acls $source_path "" noop1 noop1 
    unique_identity_map=($(echo ${identity_map[*]} | tr ' ' '\n' | sort -u))
    echo "${unique_identity_map[@]}" | tr ' ' '\n' | jq -R 'split(":") | {type:.[0], source:.[1], target:""}' | jq -s '.' > $identity_map_file
else
    echo "Copying ACLs from $source_path to $dest_path"
    process_acls $source_path $dest_path assign_acls map_identity_type 
fi