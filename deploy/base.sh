#!/bin/sh

set +x
set -e

pyt=python3

g_project_dir="$(cd -P "$(dirname "$0")/..";pwd)"
g_project="$(basename $g_project_dir)"

g_timestamp=`date +"%Y%m%d%H%M"`
