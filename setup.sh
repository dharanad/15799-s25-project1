#!/usr/bin/env bash
set -euxo pipefail

# Install OpenJDK 17.
#sudo apt install openjdk-17-jdk

# Download and extract DuckDB.
wget --quiet https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-osx-universal.zip
unzip duckdb_cli-osx-universal.zip
rm duckdb_cli-osx-universal.zip

# Download the workload.
wget --quiet https://db.cs.cmu.edu/files/15799-s25/workload.tgz
