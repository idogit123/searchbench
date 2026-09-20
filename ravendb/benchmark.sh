#!/usr/bin/env bash
# RavenDB entrypoint: RQL queries POSTed to /databases/<db>/queries
set -e
cd "$(dirname "$0")"

export ENGINE_NAME="RavenDB"
export ENGINE_TAGS='["C#",".NET","Corax","RQL","REST"]'

export SEARCHBENCH_QUERIES=queries.dsl
exec ../lib/benchmark.sh "$@"
