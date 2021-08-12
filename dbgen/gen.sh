#!/bin/sh

./dbgen "$@"

for t in *.tbl; do mkdir -p "/data/${t%.*}" && mv "$t" "/data/${t%.*}"; done