#!/usr/bin/env bash
# Puts Chronicle's secrets into AWS Secrets Manager (infra/terraform creates
# them empty). External Secrets Operator copies them into the cluster.
#
#   chronicle/config    CHRONICLE_API_TOKEN (generated once, then kept)
#   chronicle/postgres  password (the Supabase database password, URL-encoded)
#
# Values go through a private temp file, never the command line or Git.
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
tmp="$(mktemp)"
chmod 600 "$tmp"
trap 'rm -f "$tmp"' EXIT

read -rsp "Supabase database password: " DB_PASSWORD
echo
[ -n "$DB_PASSWORD" ] || { echo "empty password" >&2; exit 1; }

# It is placed inside POSTGRES_URL, so characters like @ : / # must be escaped.
DB_PASSWORD="$DB_PASSWORD" python3 -c '
import json, os, urllib.parse
print(json.dumps({"password": urllib.parse.quote(os.environ["DB_PASSWORD"], safe="")}))' > "$tmp"
aws secretsmanager put-secret-value --region "$REGION" \
  --secret-id chronicle/postgres --secret-string "file://$tmp" >/dev/null
echo "chronicle/postgres set"

# Keep an existing API token, so re-running does not log everyone out.
existing="$(aws secretsmanager get-secret-value --region "$REGION" \
  --secret-id chronicle/config --query SecretString --output text 2>/dev/null || true)"
EXISTING="$existing" NEW_TOKEN="$(openssl rand -hex 32)" python3 -c '
import json, os
try:
    cfg = json.loads(os.environ["EXISTING"])
except ValueError:
    cfg = {}
cfg.setdefault("CHRONICLE_API_TOKEN", os.environ["NEW_TOKEN"])
print(json.dumps(cfg))' > "$tmp"
aws secretsmanager put-secret-value --region "$REGION" \
  --secret-id chronicle/config --secret-string "file://$tmp" >/dev/null
echo "chronicle/config set"

echo
echo "To read the API token (for the console):"
echo "  aws secretsmanager get-secret-value --region $REGION --secret-id chronicle/config --query SecretString --output text"
