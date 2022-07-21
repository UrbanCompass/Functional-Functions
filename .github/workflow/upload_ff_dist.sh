name: upload latest functional_functions dist

on:
  push:
    branches: [ main, xx/add_github_act ]
  pull_request:
    branches: [ main ]

jobs:
  build:
  # comment: update branch on databricks
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: run push_to_databricks_repos
        run: |
          chmod +x ".github/jobs/push_to_databricks_repos.sh"
          chmod +x ".github/jobs/upload_dist.sh"
          .github/jobs/push_to_databricks_repos.sh
          .github/jobs/upload_dist.sh
        env:
          ADB_TOKEN: ${{ secrets.ADB_TOKEN }}
          ADB_WORKSPACE_URL : "compass-di-infra-production.cloud.databricks.com"
          TARGET_REPO_FOLDER: "functional-functions"
          SOURCE_GIT_BRANCH : "xx/add_github_act"