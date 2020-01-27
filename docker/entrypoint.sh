#!/usr/bin/env bash

# Make sure framework is installed
python3 test.py

if [[ $1 == "livy" ]]; then
  echo "Starting Livy on port 8998"
  /opt/apache-livy/apache-livy-0.6.0-incubating-bin/bin/livy-server
else
  exec "$@"
fi

