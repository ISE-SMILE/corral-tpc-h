#!/bin/zsh

echo "building data generator"
cd dbgen
docker build -t tpch-dbgen .
cd ..


echo "generating input files"
cd test
sh generate.sh
cd ..

echo "running local tests"
for f in experimentes/tes*.json; do
  go run main.go -v -config "$f"
done
