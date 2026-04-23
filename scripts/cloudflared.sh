#!/usr/bin/env bash
if [ -x "./bin/cloudflared" ]; then
    ./bin/cloudflared tunnel --url http://localhost:8787
else
    echo "cloudflared not found. Download into ./bin/: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/"
fi