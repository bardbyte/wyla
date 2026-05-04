#!/usr/bin/env bash
# Source this to nuke every proxy env var in the current shell, then retry.
# Use after diag_network.sh confirms VPN is up and the direct curl works
# (Section 4 in the diag returned HTTP 2xx/4xx, NOT 407).
#
# Usage:
#   source lumi_final/scripts/no_proxy_shell.sh
#   python lumi_final/scripts/check_vertex_gemini.py --insecure

unset HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY FTP_PROXY
unset http_proxy https_proxy no_proxy all_proxy ftp_proxy

# Some Python libs (urllib, requests) consult NO_PROXY — set it permissively
# so anything we DO need to hit goes direct.
export NO_PROXY="*"
export no_proxy="*"

echo "Proxy env vars unset. NO_PROXY='*' set as belt-and-suspenders."
echo "If GCP calls now succeed, something in your shell init was injecting"
echo "the proxy vars. Check:"
echo "  ~/.zshrc  ~/.zprofile  ~/.bash_profile  /etc/profile.d/*"
echo "  any 'corp setup' or 'aexp' helper scripts"
