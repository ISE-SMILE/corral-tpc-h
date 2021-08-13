#!/bin/sh

if [ ! -d 1 ]; then
 mkdir -p 1
 docker run --rm -v "$(pwd)/1":/data tpch-dbgen -s 1
fi
