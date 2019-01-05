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

    retval=$(jq -r --arg identity $identity --arg identity_type $identity_type '.[] | select(.type == $identity_type and .source == $identity) | .target' $identity_map_file)
    if [[ $retval ]]
    then
        echo $retval
    else
        echo $identity
    fi
}

assign_acls() {
    destpath=$1
    filename=$2
    owner=$3
    group=$4
    shift
    shift
    shift
    shift
    aclspec=(${@})


    if [[ $owner ]] && [[ $group ]]
    then
        hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -chown $owner:$group $destpath$filename 
    fi
    if [[ ${#aclspec[@]} -gt 0 ]]
    then
        hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -setfacl --set $(IFS=","; echo "${aclspec[*]}") $destpath$filename 
    fi
    echo "Assigned Owner:$owner, Group:$group, ACL:${aclspec[*]} to file: $destpath$filename"
}
export -f assign_acls

process_acl_entries() {
    source_path=$1
    map_identity=$2

    while read file; do
        file=$(echo $file | cut -d / -f 4-)
        aclspec=()
        owner=""
        group=""
        for i in 1 2
        do
            read identity
            ownertype=$(echo $identity | cut -d ':' -f 1 | cut -c 3-)
            identity_type="group"
            if [[ $ownertype == "owner" ]]
            then
                identity_type="user"
            fi
            identity=$($map_identity $(echo $identity | cut -d ':' -f 2 | sed -e 's/^[ \t]*//') $identity_type)
            identity_map+=("$identity_type:$identity")
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
        echo "'$file'" "'$owner'" "'$group'" "${aclspec[@]}"
    done < <(hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -getfacl -R $source_path)
}

process_acls() {
    source_path=$1
    dest_path=$2
    assign_file_acl=$3
    map_identity=$4

    echo "Reading file & ACLs list from source"
    process_acl_entries $source_path $map_identity | tr '\n' '\0' | xargs -0 -n1 -P 10 -I % bash -c "$assign_file_acl $dest_path %"
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