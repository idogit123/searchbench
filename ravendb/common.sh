#!/usr/bin/env bash
# Shared configuration for the RavenDB adapter. Sourced by every verb script.
#
# Each verb runs as its own process, so nothing is shared at runtime except
# these environment variables -- the same pattern the harness itself uses for
# SEARCHBENCH_DATA_DIR.

: "${RVN_PORT:=8080}"
: "${RVN_HOST:=127.0.0.1}"
: "${RVN_URL:=http://${RVN_HOST}:${RVN_PORT}}"
: "${RVN_DATABASE:=searchbench}"
: "${RVN_CONTAINER:=searchbench-ravendb}"
: "${RVN_IMAGE:=ravendb/ravendb:7.2.6-ubuntu.24.04-x64}"

# Index and analyzer names. The analyzer is a custom one (config/analyzer.cs);
# see config/index.json and the README for why no built-in analyzer fits.
: "${RVN_INDEX:=Logs/Search}"
: "${RVN_ANALYZER:=SearchBench.NonAlphanumericLowerCaseAnalyzer}"
: "${RVN_COLLECTION:=Logs}"

# Datadir is bind-mounted, not a named volume: named volumes live on the small
# root disk and overflow at larger scales (same reasoning as postgres/common.sh).
# Path is relative to the adapter dir; .gitignore already reserves it.
: "${RVN_DATA_DIR:=${PWD}/raven-data}"

# A Developer license is free and full-featured, so RavenDB runs without the
# Community edition's 3-core / 6 GB cap and competes on the same hardware as the
# other engines. Drop the JSON at ravendb/license.json (gitignored); without it
# the server starts unlicensed and capped, and ./start says so.
: "${RVN_LICENSE_FILE:=${PWD}/license.json}"
