#!/usr/bin/env bash
# Network-state diagnostic. Run when GCP calls suddenly start returning
# 407 Proxy Authentication Required after previously working.
#
# Usage:
#   bash lumi_final/scripts/diag_network.sh

set -u

print_header() {
    echo
    echo "================================================================"
    echo "  $1"
    echo "================================================================"
}

print_header "1. Proxy env vars in current shell"
# Lowercase + uppercase, both forms used by different libraries.
for v in HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY \
         http_proxy https_proxy no_proxy all_proxy \
         FTP_PROXY ftp_proxy REQUESTS_CA_BUNDLE SSL_CERT_FILE \
         CURL_CA_BUNDLE GRPC_DEFAULT_SSL_ROOTS_FILE_PATH \
         PYTHONHTTPSVERIFY GOOGLE_APPLICATION_CREDENTIALS \
         GOOGLE_CLOUD_PROJECT GOOGLE_CLOUD_LOCATION \
         GOOGLE_GENAI_USE_VERTEXAI LUMI_BQ_KEY_FILE LUMI_BQ_BILLING_PROJECT
do
    val="${!v:-}"
    if [[ -n "$val" ]]; then
        # Mask any user:pass embedded in proxy URLs so it's safe to paste.
        masked=$(echo "$val" | sed -E 's|://[^:@/]+:[^@/]+@|://***:***@|g')
        printf "  %-40s = %s\n" "$v" "$masked"
    fi
done
echo "  (only set vars shown)"

print_header "2. macOS system proxy settings"
if command -v scutil >/dev/null 2>&1; then
    scutil --proxy 2>&1 | head -40
else
    echo "  scutil not found"
fi

print_header "3. VPN status"
if command -v scutil >/dev/null 2>&1; then
    scutil --nc list 2>&1 | head -10
fi
ifconfig 2>/dev/null | grep -E '^(utun|gpd|ppp|tun)' | head -5
echo
echo "  Active default route:"
route -n get default 2>&1 | grep -E 'interface|gateway' | head -4

print_header "4. Can we reach GCP without a proxy at all?"
echo "  Direct curl to oauth2.googleapis.com (--noproxy '*'):"
curl --noproxy '*' --connect-timeout 5 -sS -o /dev/null -w "  HTTP %{http_code}  (resolved: %{remote_ip}, %{time_total}s)\n" \
    https://oauth2.googleapis.com/ 2>&1 | head -3

echo
echo "  Direct curl to bigquery.googleapis.com (--noproxy '*'):"
curl --noproxy '*' --connect-timeout 5 -sS -o /dev/null -w "  HTTP %{http_code}  (resolved: %{remote_ip}, %{time_total}s)\n" \
    https://bigquery.googleapis.com/ 2>&1 | head -3

print_header "5. With current proxy env (whatever's set)"
echo "  curl to oauth2.googleapis.com:"
curl --connect-timeout 5 -sS -o /dev/null -w "  HTTP %{http_code}\n" \
    https://oauth2.googleapis.com/ 2>&1 | head -3

print_header "6. Shell init files that could be injecting proxy vars"
for f in ~/.zshrc ~/.zprofile ~/.bashrc ~/.bash_profile ~/.profile; do
    if [[ -f "$f" ]] && grep -i -E 'proxy|aexp|amex' "$f" >/dev/null 2>&1; then
        echo "  $f contains proxy/corp references:"
        grep -i -n -E 'proxy|aexp|amex' "$f" | head -10 | sed 's/^/    /'
    fi
done

echo
echo "================================================================"
echo "  Done. Paste output back; mask any usernames you see."
echo "================================================================"
