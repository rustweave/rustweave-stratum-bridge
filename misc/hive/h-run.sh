#!/usr/bin/env bash

[[ ! -e ./config.yaml ]] && echo "missing config.yaml" && pwd && exit 1

rwe_bridge  $(< rwe_bridge.conf)| tee --append $CUSTOM_LOG_BASENAME.log
