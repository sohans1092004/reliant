# One IAM role per process that touches AWS, holding only what its code calls. EKS Pod
# Identity hands each role to pods running under the matching service account in the
# k8s/aws overlay, so no AWS keys exist anywhere.
#
#   process      service account   needs
#   consumer     consumer          kms:Decrypt          (endpoint secrets, to sign deliveries)
#   worker       worker            kms:Decrypt          (same, for retries)
#   dashboard    dashboard         kms:GenerateDataKey  (new and rotated secrets; never decrypts)
#   migrate      migrate           kms:GenerateDataKey  (secrets for new endpoints, re-wrapping)
#   retention    retention         s3:PutObject + kms:GenerateDataKey via S3 (SSE-KMS archives)
#   ingest, retry-publisher        nothing: no role at all

locals {
  # Must match lib/aws.js: KMS refuses any other context, and so does this policy.
  secret_context = { "kms:EncryptionContext:purpose" = "reliant-endpoint-secret" }

  services = {
    consumer  = data.aws_iam_policy_document.secrets_decrypt.json
    worker    = data.aws_iam_policy_document.secrets_decrypt.json
    dashboard = data.aws_iam_policy_document.secrets_encrypt.json
    migrate   = data.aws_iam_policy_document.secrets_encrypt.json
    retention = data.aws_iam_policy_document.archive_write.json
  }
}

data "aws_iam_policy_document" "pod_identity_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "secrets_decrypt" {
  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.app.arn]
    condition {
      test     = "StringEquals"
      variable = keys(local.secret_context)[0]
      values   = values(local.secret_context)
    }
  }
}

data "aws_iam_policy_document" "secrets_encrypt" {
  statement {
    actions   = ["kms:GenerateDataKey"]
    resources = [aws_kms_key.app.arn]
    condition {
      test     = "StringEquals"
      variable = keys(local.secret_context)[0]
      values   = values(local.secret_context)
    }
  }
}

data "aws_iam_policy_document" "archive_write" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.archive.arn}/events/*"]
  }
  statement {
    # SSE-KMS: S3 asks KMS for a data key on the caller's behalf, and only S3 may.
    actions   = ["kms:GenerateDataKey"]
    resources = [aws_kms_key.app.arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "service" {
  for_each           = local.services
  name               = "${var.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

resource "aws_iam_role_policy" "service" {
  for_each = local.services
  name     = "${var.name}-${each.key}"
  role     = aws_iam_role.service[each.key].id
  policy   = each.value
}

resource "aws_eks_pod_identity_association" "service" {
  for_each        = local.services
  cluster_name    = module.eks.cluster_name
  namespace       = var.k8s_namespace
  service_account = each.key
  role_arn        = aws_iam_role.service[each.key].arn
}
