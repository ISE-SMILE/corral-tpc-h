#!/bin/zsh

echo "building data generator"
cd dbgen
docker build -t tpch-dbgen .
cd ..


echo "generating input files"
sh generate.sh

if [! -d "./runs"]; then
  mkdir runs
fi

go run main.go -v -config examples/test_tpc01.json