# Terraform: Reliant on AWS

Everything Reliant needs in one AWS region. It's validated and planned in CI against LocalStack, and its KMS, S3 and IAM parts have been applied there too. **It has never been applied to a real AWS account**, which would cost money.

| File | Creates |
|---|---|
| `network.tf` | VPC over 3 AZs: public subnets (load balancers, NAT), private (EKS nodes), database (RDS, ElastiCache, MSK) |
| `eks.tf` | EKS cluster, managed node group, Pod Identity agent, ECR repository (immutable tags, scan on push) |
| `data.tf` | RDS Postgres 16 (Multi-AZ, encrypted, TLS required, 7-day point-in-time recovery, password managed by RDS in Secrets Manager), ElastiCache Redis (TLS, failover), MSK (3 brokers, TLS, `min.insync.replicas=2`: ADR-003). Each reachable only from the EKS nodes |
| `kms_s3.tf` | KMS key (yearly rotation) for endpoint secrets and archives; archive bucket (SSE-KMS, versioned, TLS-only, no public access, Glacier IR after 30 days, expiry after 2 years) |
| `iam.tf` | One least-privilege role per AWS-using process, bound to its Kubernetes service account by EKS Pod Identity |
| `outputs.tf` | Everything the `k8s/aws` overlay needs, as a ready `config.env` |

## Least privilege, per process

| Process | Allowed | Not allowed |
|---|---|---|
| consumer, worker | `kms:Decrypt`, only with encryption context `purpose=reliant-endpoint-secret` | creating data keys, S3 |
| dashboard, migrate | `kms:GenerateDataKey`, same context | decrypting any secret |
| retention | `s3:PutObject` under `events/`, and KMS data keys only through S3 (SSE-KMS) | reading archives, deleting them, decrypting secrets |
| ingest, retry-publisher | nothing: no role | everything |

The dashboard can create a secret but never read one back, and a stolen consumer token can decrypt endpoint secrets but can't touch the archive.

## Check it without an AWS account

```sh
cd infra/terraform
terraform fmt -check && terraform init && terraform validate
docker compose --profile aws up -d --wait reliant_localstack     # from the repo root
terraform plan -var localstack=true                              # 100 resources
```

`localstack = true` points every AWS call at LocalStack (`providers.tf`). Its free edition serves STS, IAM, EC2, KMS and S3, so the plan resolves fully. EKS, RDS, ElastiCache and MSK can be planned but not created there.

What has been verified:
- **Plan:** 100 resources, against LocalStack.
- **Apply:** the KMS key, archive bucket and IAM roles were applied to LocalStack, and `tests/aws.test.js` passed against that Terraform-created key and bucket.
- **IAM, partly:** LocalStack's policy simulator confirms each role's allowed and denied actions (e.g. the consumer can't create data keys, the dashboard can't decrypt). The simulator ignores conditions, so the encryption-context restriction can only be verified in a real account.

## Deploy (real account)

```sh
terraform init && terraform apply                    # uncomment the S3 backend in versions.tf first
aws eks update-kubeconfig --name "$(terraform output -raw cluster_name)"
terraform output -raw k8s_config_env > ../../k8s/aws/config.env
# then follow k8s/aws/kustomization.yaml: CA bundle, secrets.env, image tag, kubectl apply -k
```

Not covered here yet: exposing the dashboard (ALB + ACM certificate), installing KEDA with TLS triggers for MSK and RDS, and syncing the RDS password into Kubernetes automatically (External Secrets). Today it's copied once into `secrets.env`.
