#!/usr/bin/env bash

go build -o mysql2proto mysql2proto.go && \
upx -qvf ./mysql2proto
cp mysql2proto /usr/local/bin/

exit 0
