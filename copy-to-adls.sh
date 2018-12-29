#!/bin/sh

SOURCE_ACCOUNT=$1
SOURCE_KEY=$2
SOURCE_CONTAINER=$3
DEST_ACCOUNT=$4
DEST_CONTAINER=$5
DEST_SPN_ID=$6
DEST_SPN_SECRET=$7
IDENTITY_MAP=$8

DEST_SPN_SECRET='9Nkz*-j;2US/aDj0e'
TOKEN_REFRESH_TIME=0

check_access_token() {
    # Check for renewal
    if [ $(date -u +%s) -ge $TOKEN_REFRESH_TIME ];
    then
        local response=$(curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "client_id=$DEST_SPN_ID&client_secret=$DEST_SPN_SECRET&scope=https%3A%2F%2Fstorage.azure.com%2F.default&grant_type=client_credentials" "https://login.microsoftonline.com/common/oauth2/v2.0/token")
        TOKEN_REFRESH_TIME=$(date -u -d "+$(echo $response | jq '.expires_in')seconds" +%s)
        ACCESS_TOKEN=$(echo $response | jq -r '.access_token')
    fi
}

# Build identity map. Format is: { "type": "user|group", "source": "", "target": "" }
declare -A identity_map
eval "identity_map=($(jq -r '.[] | "[\"" + .type + "-" + .source + "\"]=" + .target' $IDENTITY_MAP))"

map_identity() {
    echo ${identity_map[$1-$2]} 
}

map_identity_header() {
    local header=$1
    local identity_type=$2
    local identity_to_map=$3
    local mapped_identity=$(map_identity $identity_type $identity_to_map)
    if [ $mapped_identity ]
    then
        local -a header=(-H "'$header: $mapped_identity'")
        identity_headers=(${identity_headers[@]}, ${header[@]})
    fi
}

SAS=$(az storage account generate-sas --account-name $SOURCE_ACCOUNT --account-key $SOURCE_KEY --services b --resource-types s --permissions lr --expiry $(date -u -d '+10 days' '+%Y-%m-%dT%H:%MZ') | jq -r '.')

az storage blob list --account-name $SOURCE_ACCOUNT --account-key $SOURCE_KEY --container-name $SOURCE_CONTAINER --output json --num-results 1000000000 --include m | jq '.[] | {name: .name, metadata: .metadata}' > ~/inventory.json

# Make the directory structure first
for directory in $(jq -c '. | select(.metadata.hdi_isfolder == "true") | {name: .name, permissions: .metadata.hdi_permission | fromjson}' ~/inventory.json)
do
    check_access_token
    directory_name=$(echo $directory | jq -r '.name')
    # Create the directory
    #curl -i -X PUT -H "x-ms-version: 2018-06-17" -H "content-length: 0" -H "x-ms-permissions:$(echo $directory | jq -r '.permissions.permissions')" -H "x-ms-umask: 0000" -H "Authorization: Bearer $ACCESS_TOKEN" "https://$DEST_ACCOUNT.dfs.core.windows.net/$DEST_CONTAINER/$directory_name?resource=directory"
    # Assign owners 
    owner=$(echo $directory | jq -r '.permissions.owner')
    group=$(echo $directory | jq -r '.permissions.group')
    owner_header=$(map_identity_header x-ms-owner user $owner) 
    group_header=$(map_identity_header x-ms-group group $group)
    echo $owner_header
    curl -v -i -X PATCH -H "x-ms-version: 2018-06-17" -H "content-length: 0" $owner_header -H "Authorization: Bearer $ACCESS_TOKEN" "https://$DEST_ACCOUNT.dfs.core.windows.net/$DEST_CONTAINER/$directory_name?action=setAccessControl"
done

