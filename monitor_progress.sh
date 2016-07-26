#!/usr/bin/env bash

function monitor_download {
  echo $2 "URLs ======================"
  sqlite3 $1 "SELECT download_status, COUNT(url) from ${2}URL GROUP BY download_status;" | tr "|" "\t"
}

while true
do
  clear
  monitor_download $1/data.sqlite3 Plan
  monitor_download $1/data.sqlite3 Provider
  monitor_download $1/data.sqlite3 Drug
	echo 
  ls -lht $1 | head
  sleep 1
done
