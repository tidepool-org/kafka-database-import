#!/bin/sh -eu

rm -rf dist
mkdir dist
go build -o dist/kafka-database-import kafka-database-import.go
