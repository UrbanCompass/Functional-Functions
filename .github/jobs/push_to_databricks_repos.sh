reposResults=$(curl -s -X GET https://$ADB_WORKSPACE_URL/api/2.0/repos \
    -H "Authorization: Bearer $ADB_TOKEN")

reposResultsDetails=$(echo $(
    jq -r --arg TAGET_REPO "$TARGET_REPO_FOLDER" '
        .repos[] | select(.path | endswith($TAGET_REPO))' <<< $reposResults
))

targetReposId=$(echo $(
    jq -r '.id' <<< $reposResultsDetails
))

echo $targetReposId

curl -s --location --request PATCH https://$ADB_WORKSPACE_URL/api/2.0/repos/$targetReposId \
--header "Authorization: Bearer $ADB_TOKEN" \
-d "{\"branch\": \"$SOURCE_GIT_BRANCH\"}"