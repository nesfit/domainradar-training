#!/bin/bash

while IFS= read -r domain; do
  if ping -c 1 -W 1 "$domain" &> /dev/null; then
    echo $domain
  fi
done
