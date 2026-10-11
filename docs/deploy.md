# Optional AWS deployment

Local use needs no cloud resources. The optional `pax-ops` CLI provisions a
namespaced AL2023 `c7a.xlarge` host in Tokyo, an artifact bucket, scoped IAM roles,
and optionally CloudFront. It uses the installed AWS CLI v2; no Terraform state,
Pulumi account, or other hosted deployment service is required.

CloudFormation stores stack state in your AWS account. Planning reconciles the
existing stack; repeating an unchanged plan does not recreate it. Releases use
immutable verified bundles, live health checks, and rollback on activation failure.
`--require-noop` verifies that an already-installed release is unchanged and healthy.

Build and install the optional CLI from the repository root (this does not
publish a package):

```sh
scripts/cargo.sh install --path crates/pax-ops --locked
pax-ops --help
```

AWS-specific scripts and their tests live in `infra/aws/scripts/` and
`infra/aws/tests/`. Only `build-al2023.sh` is a user-facing entry point; the other
scripts are invoked by that builder or embedded in `pax-ops`. To check them locally:

```sh
python3 -m unittest discover -s infra/aws/tests -p 'test_*.py'
scripts/cargo.sh test -p pax-ops --locked
```

## Credentials and namespace

Provide your own account and credential profile. The account is checked before
mutations. Account IDs in tests are synthetic examples. Bootstrap permissions are
separate from the normal deployer role.

```sh
pax-ops --admin-profile YOUR_ADMIN_PROFILE identity
# Replace YOUR_ACCOUNT_ID with the returned account.
export PAX_AWS_ACCOUNT_ID=YOUR_ACCOUNT_ID

pax-ops \
  --admin-profile YOUR_ADMIN_PROFILE --account "$PAX_AWS_ACCOUNT_ID" \
  bootstrap --trust-principal YOUR_OPERATOR_IAM_ARN
pax-ops \
  --admin-profile YOUR_ADMIN_PROFILE --account "$PAX_AWS_ACCOUNT_ID" verify-access
```

For normal operations, select your operator profile explicitly and export the
role created by bootstrap:

```sh
export PAX_AWS_DEPLOYER_ROLE_ARN="arn:aws:iam::$PAX_AWS_ACCOUNT_ID:role/pax-access/pax-deployer"
```

Alternatively, explicit `--admin-env` and `--operator-env` files are parsed as
literal data, never sourced as shell code. Keep them untracked and owner-readable.
The bootstrap `--create-operator --key-file PATH` can create an operator and write
its credentials once; do not put that output in logs or version control.

Choose `--namespace pax-demo` for a long-lived demo and a different `pax-` name
for temporary deployments. Compute, network, releases, and presentation belong
to their stack. The project IAM bootstrap/deployer boundary is account-wide and
shared by those namespaces; this is resource isolation, not an account security
boundary. The template retains its precision-time placement configuration; AZ
capacity/support must be resolved before launch.

## Host and HTTPS

Keep the account and role environment variables above set. Replace
`YOUR_OPERATOR_PROFILE` with your own AWS CLI profile. Resolve an available zone
and the official AMI, inspect the target, then plan:

```sh
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo resolve --zone-name ap-northeast-1a > /tmp/pax-target.json
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo plan --target /tmp/pax-target.json
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo apply --change-set CHANGE_SET_ARN
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo status
```

Use `--zone-id` when you know the physical AZ ID: AZ letters vary by account.
Existing host replacement requires `--allow-replacement`; an ordinary apply
refuses it. Save target/output receipts outside tracked source.

After deploying the application, enable the distribution:

```sh
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo demo-plan
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo demo-apply --change-set CHANGE_SET_ARN
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo demo-status
```

CloudFront supplies an HTTPS `*.cloudfront.net` hostname and certificate; you do
not need a domain. After checking that endpoint, `demo-plan --restrict-origin`
plans restriction of direct origin access. Presentation-only changes reject host
replacement. CloudFront propagation is asynchronous; inspect status before sharing.

## Build and release

On Linux x86_64 with Docker and Python 3.11+ available:

```sh
scripts/cargo.sh fetch --locked
infra/aws/scripts/build-al2023.sh /tmp/pax-demo-build x86-64
# Optional operator binary, built and checked in the same compatible userspace:
PAX_ARTIFACT_WORKLOAD=ops infra/aws/scripts/build-al2023.sh /tmp/pax-ops-build x86-64
```

The output directory must be new. The container receives the frozen source and
Rust toolchain, never credentials. Receipts retain compiler, source digest,
features, binary hash, and immutable image identity. The SDK workload builds only
the demo and runs an offline HTTP smoke check. `znver4` is an explicit target
alternative; it is not executed as a smoke test on an incompatible build CPU.

Use instance/bucket values from your own stack outputs and current market metadata:

```sh
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo deploy \
  --instance INSTANCE_ID --bucket ARTIFACT_BUCKET \
  --artifact /tmp/pax-demo-build/artifact --bundle /tmp/pax-demo-release.tar.gz \
  --markets MARKET_ID --activate-stable
pax-ops --profile YOUR_OPERATOR_PROFILE --namespace pax-demo release-status --instance INSTANCE_ID
```

Reuse the same artifact/bundle and add `--require-noop` to check idempotence. A
changed deployment stages immutable content, checks CPU/build compatibility,
switches the service, and requires healthy synchronized books. Failed activation
attempts to restore the previous release and verifies its health; missing or failed
restoration is reported as a failure. CPU affinity and an already-configured
hardware clock/interface are explicit options; see `deploy --help`.

`destroy --yes` removes the selected owned stack and reports retained artifacts.
The artifact bucket is intentionally retained; inspect and clean it separately
when no longer needed. No command should be pointed at another namespace merely
to reuse its instance or bucket.
