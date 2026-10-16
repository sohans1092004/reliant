#!/bin/bash
# Runs inside LocalStack once it's ready (/etc/localstack/init/ready.d). Creates what
# Terraform would create in a real account: the KMS key for endpoint secrets and archives,
# and the archive bucket. Idempotent, so a restart with persisted state is fine.
set -euo pipefail
ALIAS=alias/reliant-secrets
BUCKET=reliant-archive

if ! awslocal kms describe-key --key-id "$ALIAS" >/dev/null 2>&1; then
  KEY_ID=$(awslocal kms create-key --description "Reliant endpoint secrets and archives" --query KeyMetadata.KeyId --output text)
  awslocal kms create-alias --alias-name "$ALIAS" --target-key-id "$KEY_ID"
  awslocal kms enable-key-rotation --key-id "$KEY_ID"
fi
awslocal s3api head-bucket --bucket "$BUCKET" 2>/dev/null || awslocal s3api create-bucket --bucket "$BUCKET"
echo "reliant: KMS $ALIAS and s3://$BUCKET ready"
