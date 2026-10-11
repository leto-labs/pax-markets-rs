# pax-ops

Optional AWS deployment CLI for the demo. It uses explicit account/credential
selection and namespaces, with an IAM bootstrap and CloudFormation change sets.
It can build a plan, reconcile a host, configure CloudFront HTTPS, deploy a verified
release through SSM, check health, and roll back a failed activation.

The account is supplied by the operator and verified before mutations. No private
account, instance, bucket, or AMI is embedded in the source. Host identity checks
require the current owned stack and its exact EC2 resource. Repeated unchanged
plans/releases preserve the running deployment; replacement and stable activation
are explicit operations. Runtime roles cannot remove their permissions boundary.

```sh
scripts/cargo.sh run -p pax-ops -- --help
scripts/cargo.sh test -p pax-ops
```

Tests use fake AWS responses and local release fixtures. They never provision
resources. See [deployment](../../docs/deploy.md) for the optional live workflow.
