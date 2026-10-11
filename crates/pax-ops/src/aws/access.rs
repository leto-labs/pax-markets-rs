//! Scoped IAM bootstrap and immutable access boundaries.
use super::Api;
use crate::{config::Scope, error::Error};
use serde_json::{Value, json};

const OWNERSHIP: &str = "pax-markets-rs";

/// Maximum permissions of any role created by the deployer. It cannot become an administrator.
pub fn workload_boundary(scope: &Scope) -> Value {
    json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":["ssm:UpdateInstanceInformation","ssm:ListInstanceAssociations","ssm:DescribeInstanceProperties","ssm:DescribeDocument","ssm:GetDocument","ssm:GetManifest","ssm:ListAssociations","ssm:PutInventory","ssm:PutComplianceItems","ssm:PutConfigurePackageResult","ssm:UpdateAssociationStatus","ssm:UpdateInstanceAssociationStatus","ssmmessages:CreateControlChannel","ssmmessages:CreateDataChannel","ssmmessages:OpenControlChannel","ssmmessages:OpenDataChannel","ec2messages:AcknowledgeMessage","ec2messages:DeleteMessage","ec2messages:FailMessage","ec2messages:GetEndpoint","ec2messages:GetMessages","ec2messages:SendReply"],"Resource":"*","Condition":{"StringEquals":{"aws:RequestedRegion":scope.region}}},
        {"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":format!("arn:aws:s3:::pax-{}-{}-*/releases/*",scope.account,scope.region)},
        {"Effect":"Deny","Action":["iam:*","sts:AssumeRole","organizations:*","cloudformation:*"],"Resource":"*"}
    ]})
}

/// Fixed deployer policy: runtime IAM paths, owned regional resources and project stacks only.
/// EC2 creation and mutation grants are separate because describe APIs are unscoped.
pub fn deployer_policy(scope: &Scope) -> Value {
    let iam = format!("arn:aws:iam::{}:", scope.account);
    let cloudfront = format!("arn:aws:cloudfront::{}:", scope.account);
    let ec2 = format!("arn:aws:ec2:{}:{}:", scope.region, scope.account);
    let mut statements = vec![
        json!({"Effect":"Allow","Action":["sts:GetCallerIdentity"],"Resource":"*"}),
        // Global presentation service. CloudFormation creates distributions with
        // ownership tags; foreign updates/deletes and direct tag takeover stay denied.
        json!({"Effect":"Allow","Action":"cloudfront:CreateDistribution","Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/pax:owner":OWNERSHIP},"ForAnyValue:StringEquals":{"aws:CalledVia":"cloudformation.amazonaws.com"}}}),
        json!({"Effect":"Allow","Action":["cloudfront:GetDistribution","cloudfront:GetDistributionConfig","cloudfront:ListTagsForResource","cloudfront:UpdateDistribution","cloudfront:DeleteDistribution"],"Resource":format!("{cloudfront}distribution/*"),"Condition":{"StringEquals":{"aws:ResourceTag/pax:owner":OWNERSHIP}}}),
        json!({"Effect":"Allow","Action":"cloudfront:TagResource","Resource":format!("{cloudfront}distribution/*"),"Condition":{"StringEquals":{"aws:RequestTag/pax:owner":OWNERSHIP},"ForAnyValue:StringEquals":{"aws:CalledVia":"cloudformation.amazonaws.com"}}}),
        // Origin request policies cannot carry resource tags. Restrict their
        // lifecycle to CloudFormation, whose stack APIs are already project-scoped.
        json!({"Effect":"Allow","Action":"cloudfront:CreateOriginRequestPolicy","Resource":"*","Condition":{"ForAnyValue:StringEquals":{"aws:CalledVia":"cloudformation.amazonaws.com"}}}),
        json!({"Effect":"Allow","Action":["cloudfront:GetOriginRequestPolicy","cloudfront:GetOriginRequestPolicyConfig","cloudfront:UpdateOriginRequestPolicy","cloudfront:DeleteOriginRequestPolicy"],"Resource":format!("{cloudfront}origin-request-policy/*"),"Condition":{"ForAnyValue:StringEquals":{"aws:CalledVia":"cloudformation.amazonaws.com"}}}),
        json!({"Effect":"Allow","Action":"servicequotas:GetServiceQuota","Resource":"*","Condition":{"StringEquals":{"aws:RequestedRegion":scope.region}}}),
        json!({"Effect":"Allow","Action":["ec2:Describe*","ssm:DescribeInstanceInformation","ssm:ListCommandInvocations","ssm:GetCommandInvocation","cloudformation:ValidateTemplate"],"Resource":"*","Condition":{"StringEquals":{"aws:RequestedRegion":scope.region}}}),
        json!({"Effect":"Allow","Action":["ssm:GetParameter"],"Resource":format!("arn:aws:ssm:{}::parameter/aws/service/ami-amazon-linux-latest/al2023-ami-*-x86_64",scope.region)}),
        json!({"Effect":"Allow","Action":["cloudformation:CreateStack","cloudformation:CreateChangeSet","cloudformation:DescribeChangeSet","cloudformation:DeleteChangeSet","cloudformation:ExecuteChangeSet","cloudformation:DescribeStacks","cloudformation:DescribeStackEvents","cloudformation:ListStackResources","cloudformation:DeleteStack","cloudformation:ContinueUpdateRollback"],"Resource":[format!("arn:aws:cloudformation:{}:{}:stack/pax-*/*",scope.region,scope.account),format!("arn:aws:cloudformation:{}:{}:changeSet/pax-*/*",scope.region,scope.account)]}),
        json!({"Effect":"Allow","Action":["iam:CreateRole"],"Resource":format!("{iam}role/pax/*"),"Condition":{"StringEquals":{"iam:PermissionsBoundary":scope.boundary_arn()}}}),
        json!({"Effect":"Allow","Action":["iam:GetRole","iam:GetRolePolicy","iam:ListRolePolicies","iam:ListAttachedRolePolicies","iam:PutRolePolicy","iam:DeleteRolePolicy","iam:DeleteRole","iam:TagRole","iam:UntagRole","iam:UpdateAssumeRolePolicy"],"Resource":format!("{iam}role/pax/*")}),
        // A lookup of a not-yet-created IAM name is authorized using its default
        // path, so CloudFormation must be able to read these reserved names too.
        json!({"Effect":"Allow","Action":["iam:GetRole","iam:GetRolePolicy","iam:ListRolePolicies","iam:ListAttachedRolePolicies","iam:ListInstanceProfilesForRole"],"Resource":[format!("{iam}role/pax-*-runtime"),format!("{iam}role/pax/*")]}),
        json!({"Effect":"Allow","Action":["iam:GetInstanceProfile"],"Resource":format!("{iam}instance-profile/pax-*-runtime")}),
        json!({"Effect":"Allow","Action":["iam:CreateInstanceProfile","iam:GetInstanceProfile","iam:AddRoleToInstanceProfile","iam:RemoveRoleFromInstanceProfile","iam:DeleteInstanceProfile","iam:TagInstanceProfile"],"Resource":format!("{iam}instance-profile/pax/*")}),
        json!({"Effect":"Allow","Action":["iam:PassRole"],"Resource":format!("{iam}role/pax/*"),"Condition":{"StringEquals":{"iam:PassedToService":"ec2.amazonaws.com"}}}),
        json!({"Effect":"Deny","Action":["iam:DeleteRolePermissionsBoundary","iam:PutRolePermissionsBoundary","iam:CreatePolicy","iam:CreatePolicyVersion","iam:SetDefaultPolicyVersion","iam:DeletePolicy","iam:CreateUser","iam:CreateAccessKey","iam:AttachRolePolicy"],"Resource":"*"}),
        json!({"Effect":"Allow","Action":["ssm:SendCommand"],"Resource":[format!("arn:aws:ssm:{}::document/AWS-RunShellScript",scope.region)]}),
        json!({"Effect":"Allow","Action":["ssm:SendCommand"],"Resource":format!("{ec2}instance/*"),"Condition":{"StringEquals":{"ssm:resourceTag/pax:owner":OWNERSHIP}}}),
        json!({"Effect":"Allow","Action":["s3:CreateBucket","s3:DeleteBucket","s3:GetBucketLocation","s3:GetBucketTagging","s3:PutBucketTagging","s3:GetBucketPublicAccessBlock","s3:PutBucketPublicAccessBlock","s3:GetEncryptionConfiguration","s3:PutEncryptionConfiguration","s3:GetLifecycleConfiguration","s3:PutLifecycleConfiguration","s3:GetBucketVersioning","s3:PutBucketVersioning","s3:ListBucket","s3:ListBucketVersions"],"Resource":format!("arn:aws:s3:::pax-{}-{}-*",scope.account,scope.region)}),
        json!({"Effect":"Allow","Action":["s3:PutObject","s3:GetObject","s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":format!("arn:aws:s3:::pax-{}-{}-*/releases/*",scope.account,scope.region)}),
    ];
    // Create requests have no ResourceTag yet. Every taggable created resource must
    // carry ownership, preventing unconstrained region-wide mutation afterwards.
    for (action, resource_type) in [
        ("CreateVpc", "vpc"),
        ("CreateSubnet", "subnet"),
        ("CreateInternetGateway", "internet-gateway"),
        ("CreateRouteTable", "route-table"),
        ("CreateSecurityGroup", "security-group"),
        ("CreatePlacementGroup", "placement-group"),
        ("AllocateAddress", "elastic-ip"),
    ] {
        statements.push(json!({"Effect":"Allow","Action":format!("ec2:{action}"),"Resource":format!("{ec2}{resource_type}/*"),"Condition":{"StringEquals":{"aws:RequestedRegion":scope.region,"aws:RequestTag/pax:owner":OWNERSHIP}}}));
    }
    // These create actions authorize both the new tagged object and its existing
    // parent VPC. The parent has ResourceTag, not RequestTag, in that evaluation.
    statements.push(json!({"Effect":"Allow","Action":["ec2:CreateSubnet","ec2:CreateRouteTable","ec2:CreateSecurityGroup"],"Resource":format!("{ec2}vpc/*"),"Condition":{"StringEquals":{"aws:RequestedRegion":scope.region,"ec2:ResourceTag/pax:owner":OWNERSHIP}}}));
    statements.push(json!({"Effect":"Allow","Action":"ec2:RunInstances","Resource":[format!("{ec2}instance/*"),format!("{ec2}volume/*")],"Condition":{"StringEquals":{"aws:RequestedRegion":scope.region,"aws:RequestTag/pax:owner":OWNERSHIP}}}));
    statements.push(json!({"Effect":"Allow","Action":"ec2:RunInstances","Resource":[format!("arn:aws:ec2:{}::image/ami-*",scope.region),format!("{ec2}subnet/*"),format!("{ec2}security-group/*"),format!("{ec2}network-interface/*")],"Condition":{"StringEquals":{"aws:RequestedRegion":scope.region}}}));
    statements.push(json!({"Effect":"Allow","Action":"ec2:RunInstances","Resource":format!("{ec2}placement-group/*"),"Condition":{"StringEquals":{"aws:RequestedRegion":scope.region,"ec2:ResourceTag/pax:owner":OWNERSHIP}}}));
    statements.push(json!({"Effect":"Allow","Action":"ec2:CreateTags","Resource":"*","Condition":{"StringEquals":{"aws:RequestedRegion":scope.region,"ec2:CreateAction":["CreateVpc","CreateSubnet","CreateInternetGateway","CreateRouteTable","CreateSecurityGroup","CreatePlacementGroup","AllocateAddress","RunInstances"]}}}));
    statements.push(json!({"Effect":"Allow","Action":["ec2:CreateTags","ec2:DeleteTags","ec2:ModifyVpcAttribute","ec2:ModifySubnetAttribute","ec2:AttachInternetGateway","ec2:DetachInternetGateway","ec2:CreateRoute","ec2:DeleteRoute","ec2:AssociateRouteTable","ec2:DisassociateRouteTable","ec2:AuthorizeSecurityGroupIngress","ec2:AuthorizeSecurityGroupEgress","ec2:RevokeSecurityGroupIngress","ec2:RevokeSecurityGroupEgress","ec2:AssociateAddress","ec2:DisassociateAddress","ec2:ReleaseAddress","ec2:TerminateInstances","ec2:StartInstances","ec2:StopInstances","ec2:DeleteVpc","ec2:DeleteSubnet","ec2:DeleteInternetGateway","ec2:DeleteRouteTable","ec2:DeleteSecurityGroup","ec2:DeletePlacementGroup"],"Resource":"*","Condition":{"StringEquals":{"aws:RequestedRegion":scope.region,"ec2:ResourceTag/pax:owner":OWNERSHIP}}}));
    json!({"Version":"2012-10-17","Statement":statements})
}

/// Reconcile the IAM-only access layer. An existing boundary must match exactly;
/// changing its maximum permissions requires a separate explicit administrator edit.
pub fn bootstrap(
    api: &mut impl Api,
    scope: &Scope,
    principal: &str,
    create_user: bool,
) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let expected_prefix = format!("arn:aws:iam::{}:", scope.account);
    if !principal.starts_with(&expected_prefix)
        || !(principal.contains(":user/") || principal.contains(":role/"))
    {
        return Err(Error::Config(
            "deployer trust must name a same-account IAM user or role".into(),
        ));
    }
    let deployment = serde_json::to_string(&deployer_policy(scope))?;
    if deployment.len() > 10240 {
        return Err(Error::Config(
            "generated deployer policy exceeds IAM's inline role quota; no IAM mutation performed"
                .into(),
        ));
    }
    if create_user {
        match api.call("iam", "get-user", json!({"UserName":"pax-operator"})) {
            Ok(value) => require_path(&value["User"], "/pax-access/")?,
            Err(e) if e.aws_code("NoSuchEntity") => {
                api.call("iam","create-user",json!({"UserName":"pax-operator","Path":"/pax-access/","Tags":[{"Key":"pax:owner","Value":OWNERSHIP}]}))?;
            }
            Err(e) => return Err(e),
        }
    }
    let boundary = workload_boundary(scope);
    match api.call(
        "iam",
        "get-policy",
        json!({"PolicyArn":scope.boundary_arn()}),
    ) {
        Ok(value) => {
            let version = value["Policy"]["DefaultVersionId"]
                .as_str()
                .ok_or(Error::Missing("DefaultVersionId"))?;
            let existing = api.call(
                "iam",
                "get-policy-version",
                json!({"PolicyArn":scope.boundary_arn(),"VersionId":version}),
            )?;
            if existing["PolicyVersion"]["Document"] != boundary {
                return Err(Error::Config(
                    "existing workload boundary differs; refusing implicit replacement".into(),
                ));
            }
        }
        Err(e) if e.aws_code("NoSuchEntity") => {
            api.call("iam","create-policy",json!({"PolicyName":"pax-workload-boundary","Path":"/pax-access/","PolicyDocument":serde_json::to_string(&boundary)?,"Tags":[{"Key":"pax:owner","Value":OWNERSHIP}]}))?;
        }
        Err(e) => return Err(e),
    }
    let trust = json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":principal},"Action":"sts:AssumeRole"}]});
    match api.call("iam", "get-role", json!({"RoleName":"pax-deployer"})) {
        Ok(value) => {
            require_path(&value["Role"], "/pax-access/")?;
            api.call(
                "iam",
                "update-assume-role-policy",
                json!({"RoleName":"pax-deployer","PolicyDocument":serde_json::to_string(&trust)?}),
            )?;
        }
        Err(e) if e.aws_code("NoSuchEntity") => {
            api.call("iam","create-role",json!({"RoleName":"pax-deployer","Path":"/pax-access/","AssumeRolePolicyDocument":serde_json::to_string(&trust)?,"MaxSessionDuration":3600,"Tags":[{"Key":"pax:owner","Value":OWNERSHIP}]}))?;
        }
        Err(e) => return Err(e),
    }
    api.call("iam","put-role-policy",json!({"RoleName":"pax-deployer","PolicyName":"pax-deployment","PolicyDocument":deployment}))?;
    if create_user {
        api.call("iam","put-user-policy",json!({"UserName":"pax-operator","PolicyName":"pax-assume-deployer","PolicyDocument":serde_json::to_string(&json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Resource":scope.deployer_arn()}]}))?}))?;
    }
    Ok(
        json!({"deployer_role":scope.deployer_arn(),"workload_boundary":scope.boundary_arn(),"operator_user":create_user.then_some("pax-operator")}),
    )
}
fn require_path(value: &Value, path: &str) -> Result<(), Error> {
    if value["Path"].as_str() != Some(path) {
        return Err(Error::Config(
            "existing IAM name is outside owned path".into(),
        ));
    }
    if !value["Tags"].as_array().is_some_and(|tags| {
        tags.iter()
            .any(|tag| tag["Key"] == "pax:owner" && tag["Value"] == OWNERSHIP)
    }) {
        return Err(Error::Config(
            "existing IAM object lacks project ownership tag".into(),
        ));
    }
    Ok(())
}

/// Exercise live IAM decisions without performing the simulated operations.
/// Simulation is supporting evidence; actual stack creation remains required.
pub fn verify_access(api: &mut impl Api, scope: &Scope) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let owned = format!("arn:aws:iam::{}:role/pax/pax-test-runtime", scope.account);
    let unrelated = format!("arn:aws:iam::{}:role/unrelated", scope.account);
    let context = |key: &str, value: &str| json!({"ContextKeyName":key,"ContextKeyValues":[value],"ContextKeyType":"string"});
    let cases = [
        (
            "update-owned-distribution",
            "cloudfront:UpdateDistribution",
            format!("arn:aws:cloudfront::{}:distribution/EXAMPLE", scope.account),
            vec![context("aws:ResourceTag/pax:owner", OWNERSHIP)],
            true,
        ),
        (
            "update-foreign-distribution",
            "cloudfront:UpdateDistribution",
            format!("arn:aws:cloudfront::{}:distribution/EXAMPLE", scope.account),
            vec![context("aws:ResourceTag/pax:owner", "unrelated")],
            false,
        ),
        (
            "delete-foreign-distribution",
            "cloudfront:DeleteDistribution",
            format!("arn:aws:cloudfront::{}:distribution/EXAMPLE", scope.account),
            vec![context("aws:ResourceTag/pax:owner", "unrelated")],
            false,
        ),
        (
            "claim-foreign-distribution-directly",
            "cloudfront:TagResource",
            format!("arn:aws:cloudfront::{}:distribution/EXAMPLE", scope.account),
            vec![context("aws:RequestTag/pax:owner", OWNERSHIP)],
            false,
        ),
        (
            "create-subnet-in-owned-vpc",
            "ec2:CreateSubnet",
            format!(
                "arn:aws:ec2:{}:{}:vpc/vpc-0123456789abcdef0",
                scope.region, scope.account
            ),
            vec![
                context("aws:RequestedRegion", &scope.region),
                context("ec2:ResourceTag/pax:owner", OWNERSHIP),
            ],
            true,
        ),
        (
            "create-subnet-in-foreign-vpc",
            "ec2:CreateSubnet",
            format!(
                "arn:aws:ec2:{}:{}:vpc/vpc-0123456789abcdef0",
                scope.region, scope.account
            ),
            vec![
                context("aws:RequestedRegion", &scope.region),
                context("ec2:ResourceTag/pax:owner", "unrelated"),
                context("aws:RequestTag/pax:owner", OWNERSHIP),
            ],
            false,
        ),
        (
            "create-role-with-boundary",
            "iam:CreateRole",
            owned.clone(),
            vec![context("iam:PermissionsBoundary", &scope.boundary_arn())],
            true,
        ),
        (
            "create-role-without-boundary",
            "iam:CreateRole",
            owned.clone(),
            vec![],
            false,
        ),
        (
            "create-unrelated-role",
            "iam:CreateRole",
            unrelated.clone(),
            vec![context("iam:PermissionsBoundary", &scope.boundary_arn())],
            false,
        ),
        (
            "remove-boundary",
            "iam:DeleteRolePermissionsBoundary",
            owned.clone(),
            vec![],
            false,
        ),
        (
            "rewrite-boundary",
            "iam:PutRolePermissionsBoundary",
            owned.clone(),
            vec![],
            false,
        ),
        (
            "pass-runtime-to-ec2",
            "iam:PassRole",
            owned.clone(),
            vec![context("iam:PassedToService", "ec2.amazonaws.com")],
            true,
        ),
        (
            "pass-runtime-to-other-service",
            "iam:PassRole",
            owned.clone(),
            vec![context("iam:PassedToService", "lambda.amazonaws.com")],
            false,
        ),
        (
            "pass-unrelated-role",
            "iam:PassRole",
            unrelated.clone(),
            vec![context("iam:PassedToService", "ec2.amazonaws.com")],
            false,
        ),
        (
            "edit-bootstrap-role",
            "iam:PutRolePolicy",
            scope.deployer_arn(),
            vec![],
            false,
        ),
        (
            "terminate-foreign-instance",
            "ec2:TerminateInstances",
            format!(
                "arn:aws:ec2:{}:{}:instance/i-0123456789abcdef0",
                scope.region, scope.account
            ),
            vec![
                context("aws:RequestedRegion", &scope.region),
                context("ec2:ResourceTag/pax:owner", "unrelated"),
            ],
            false,
        ),
    ];
    let mut receipts = Vec::new();
    for (name, action, resource, contexts, expected) in cases {
        let result=api.call("iam","simulate-principal-policy",json!({"PolicySourceArn":scope.deployer_arn(),"ActionNames":[action],"ResourceArns":[resource],"ContextEntries":contexts}))?;
        let decision = result["EvaluationResults"][0]["EvalDecision"]
            .as_str()
            .ok_or(Error::Missing("EvalDecision"))?;
        if (decision == "allowed") != expected {
            return Err(Error::Config(format!(
                "IAM simulation {name} produced unexpected {decision}"
            )));
        }
        receipts.push(json!({"case":name,"decision":decision}));
    }
    let boundary=api.call("iam","simulate-custom-policy",json!({"PolicyInputList":[serde_json::to_string(&workload_boundary(scope))?],"ActionNames":["iam:CreateUser","sts:AssumeRole","ssm:GetParameter"],"ResourceArns":["*"]}))?;
    let results = boundary["EvaluationResults"]
        .as_array()
        .ok_or(Error::Missing("boundary EvaluationResults"))?;
    if results.len() != 3 || results.iter().any(|r| r["EvalDecision"] == "allowed") {
        return Err(Error::Config(
            "runtime boundary permits an escalation or parameter-secret read".into(),
        ));
    }
    Ok(
        json!({"deployer_cases":receipts,"runtime_boundary_denies_escalation":true,"limitation":"simulation does not replace live service authorization checks"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn boundary_denies_escalation_and_deployer_cannot_remove_it() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let boundary = workload_boundary(&scope);
        assert_eq!(boundary["Statement"][2]["Effect"], "Deny");
        let policy = deployer_policy(&scope);
        let statements = policy["Statement"].as_array().unwrap();
        let create = statements
            .iter()
            .find(|s| s["Action"] == json!(["iam:CreateRole"]))
            .unwrap();
        assert_eq!(create["Resource"], "arn:aws:iam::123456789012:role/pax/*");
        assert_eq!(
            create["Condition"]["StringEquals"]["iam:PermissionsBoundary"],
            scope.boundary_arn()
        );
        let pass = statements
            .iter()
            .find(|s| s["Action"] == json!(["iam:PassRole"]))
            .unwrap();
        assert_eq!(
            pass["Condition"]["StringEquals"]["iam:PassedToService"],
            "ec2.amazonaws.com"
        );
        assert!(statements.iter().any(|s| {
            s["Effect"] == "Deny"
                && s["Action"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("iam:DeleteRolePermissionsBoundary"))
        }));
    }
    #[test]
    fn unrelated_principal_is_rejected_before_iam() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut api = super::super::fake::Fake::new([Ok(json!({"Account":scope.account}))]);
        assert!(bootstrap(&mut api, &scope, "arn:aws:iam::999999999999:root", false).is_err());
        assert_eq!(api.calls.len(), 1);
    }
    #[test]
    fn repeat_bootstrap_converges_without_creating_credentials() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"Policy":{"DefaultVersionId":"v1"}})),
            Ok(json!({"PolicyVersion":{"Document":workload_boundary(&scope)}})),
            Ok(
                json!({"Role":{"Path":"/pax-access/","Tags":[{"Key":"pax:owner","Value":OWNERSHIP}]}}),
            ),
            Ok(json!({})),
            Ok(json!({})),
        ]);
        bootstrap(
            &mut api,
            &scope,
            "arn:aws:iam::123456789012:user/pax-access/pax-operator",
            false,
        )
        .unwrap();
        assert!(
            !api.calls
                .iter()
                .any(|(_, operation, _)| operation.starts_with("create-"))
        );
        assert!(
            api.calls
                .iter()
                .filter(|(service, _, _)| service != "sts")
                .all(|(service, _, _)| service == "iam")
        );
    }
}
