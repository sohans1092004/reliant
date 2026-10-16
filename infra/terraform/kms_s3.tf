# The same KMS key and bucket localstack/init-aws.sh creates locally.

resource "aws_kms_key" "app" {
  description         = "Reliant endpoint signing secrets (envelope encryption) and archives"
  enable_key_rotation = true # yearly, transparent: old ciphertexts keep decrypting
}

resource "aws_kms_alias" "app" {
  name          = "alias/${var.name}-secrets"
  target_key_id = aws_kms_key.app.key_id
}

resource "aws_s3_bucket" "archive" {
  bucket_prefix = "${var.name}-archive-"
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.app.arn
    }
    bucket_key_enabled = true # one KMS call per bucket key, not per object
  }
}

# Archives are the only copy of events past the replay window: keep old versions too.
resource "aws_s3_bucket_versioning" "archive" {
  bucket = aws_s3_bucket.archive.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    id     = "age-out"
    status = "Enabled"
    filter {
      prefix = "events/"
    }
    transition {
      days          = 30
      storage_class = "GLACIER_IR" # rarely read, still readable in milliseconds
    }
    expiration {
      days = var.archive_expiry_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

data "aws_iam_policy_document" "archive_tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.archive.arn, "${aws_s3_bucket.archive.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "archive" {
  bucket = aws_s3_bucket.archive.id
  policy = data.aws_iam_policy_document.archive_tls_only.json
}
