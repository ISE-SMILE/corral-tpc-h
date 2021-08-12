#!/bin/sh

mddir -p 1
docker run --rm -v $(pwd)/1:/data tpch-dbgen -s 1
