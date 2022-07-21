files=($(ls $GITHUB_WORKSPACE/dist/functional_functions-*.whl | sort -r))
latest_whl=$(echo ${files[0]})
echo $latest_whl
mainpy64=$(base64 -w 0 $latest_whl)

curl -s --location --request POST https://$ADB_WORKSPACE_URL/api/2.0/dbfs/put \
    --header "Authorization: Bearer $ADB_TOKEN" \
    --data "{ \"path\": \"dbfs:/FileStore/fbi_team/$TARGET_REPO_FOLDER/functional_functions-0.6-py3-none-any.whl\", 
    \"contents\": \"$mainpy64\", 
    \"overwrite\": true }"
    
# ls $GITHUB_WORKSPACE/functional_functions-*.whl | sort -r