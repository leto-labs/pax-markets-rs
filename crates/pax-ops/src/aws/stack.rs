//! CloudFormation-owned host lifecycle and account-specific placement.
mod image;
use super::Api;
use crate::{config::Scope, error::Error};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Reviewed single-host CloudFormation template, without credentials or local paths.
pub const HOST_TEMPLATE: &str = include_str!("../../../../infra/aws/host.json");

/// Resolved account-specific placement and pinned official AMI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    /// Account-specific AZ label.
    pub zone_name: String,
    /// Stable AZ ID.
    pub zone_id: String,
    /// Exact Amazon-owned AMI ID.
    pub ami: String,
    /// Allowed target size.
    pub instance_type: String,
}
/// Resolve zone and official AMI, retaining the deployed image unless overridden.
/// An explicit zone ID must agree with an explicitly supplied zone name.
/// No capacity or placement fallback is implicit.
pub fn resolve(
    api: &mut impl Api,
    scope: &Scope,
    name: Option<&str>,
    id: Option<&str>,
    ami: Option<&str>,
    instance_type: &str,
) -> Result<Target, Error> {
    super::verify_account(api, scope)?;
    if !["c7a.xlarge", "c7a.large"].contains(&instance_type) {
        return Err(Error::Config(
            "only c7a.xlarge or c7a.large targets are supported".into(),
        ));
    }
    let inventory = api.call(
        "ec2",
        "describe-availability-zones",
        json!({"AllAvailabilityZones":false}),
    )?;
    let zones = inventory["AvailabilityZones"]
        .as_array()
        .ok_or(Error::Missing("AvailabilityZones"))?;
    let selected = zones
        .iter()
        .find(|zone| {
            zone["State"] == "available"
                && zone["ZoneType"] == "availability-zone"
                && if let Some(id) = id {
                    zone["ZoneId"] == id
                } else {
                    zone["ZoneName"] == name.unwrap_or("ap-northeast-1a")
                }
        })
        .ok_or_else(|| {
            Error::PlacementUnavailable("requested availability zone is unavailable".into())
        })?;
    let zone_name = selected["ZoneName"]
        .as_str()
        .ok_or(Error::Missing("ZoneName"))?;
    let zone_id = selected["ZoneId"]
        .as_str()
        .ok_or(Error::Missing("ZoneId"))?;
    if name.is_some_and(|name| name != zone_name) {
        return Err(Error::Config("explicit AZ name and ID disagree".into()));
    }
    let offerings = api.call("ec2","describe-instance-type-offerings",json!({"LocationType":"availability-zone-id","Filters":[{"Name":"instance-type","Values":[instance_type]},{"Name":"location","Values":[zone_id]}]}))?;
    if !offerings["InstanceTypeOfferings"]
        .as_array()
        .is_some_and(|a| !a.is_empty())
    {
        return Err(Error::PlacementUnavailable(
            "requested instance type is not offered in the selected AZ; no implicit fallback"
                .into(),
        ));
    }
    let ami = image::select(api, scope, ami)?;
    let images = api.call(
        "ec2",
        "describe-images",
        json!({"ImageIds":[ami],"Owners":["amazon"]}),
    )?;
    let image = images["Images"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or(Error::Missing("official AMI"))?;
    if image["Architecture"] != "x86_64"
        || image["State"] != "available"
        || !image["Name"]
            .as_str()
            .is_some_and(|name| name.starts_with("al2023-ami-"))
    {
        return Err(Error::Config(
            "AMI must be available Amazon Linux 2023 x86_64 from Amazon".into(),
        ));
    }
    Ok(Target {
        zone_name: zone_name.into(),
        zone_id: zone_id.into(),
        ami,
        instance_type: instance_type.into(),
    })
}

/// Try only explicitly configured AZ-ID alternatives after a placement-unavailable result.
/// Authentication, conflicting inputs and AMI failures never trigger a fallback.
pub fn resolve_with_fallback(
    api: &mut impl Api,
    scope: &Scope,
    name: Option<&str>,
    id: Option<&str>,
    ami: Option<&str>,
    instance_type: &str,
    fallback_ids: &[String],
) -> Result<Value, Error> {
    if fallback_ids.len() > 3
        || fallback_ids.iter().any(|s| {
            !s.starts_with("apne") || !s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
        })
    {
        return Err(Error::Config(
            "at most three explicit valid AZ-ID alternatives are allowed".into(),
        ));
    }
    let mut attempts = Vec::new();
    let mut candidates = vec![(name, id)];
    candidates.extend(fallback_ids.iter().map(|id| (None, Some(id.as_str()))));
    for (name, id) in candidates {
        match resolve(api, scope, name, id, ami, instance_type) {
            Ok(target) => {
                let mut receipt = serde_json::to_value(target)?;
                receipt["placement_attempts"] = json!(attempts);
                receipt["explicit_fallback_used"] = json!(!attempts.is_empty());
                return Ok(receipt);
            }
            Err(Error::PlacementUnavailable(reason)) => {
                attempts.push(json!({"zone_name":name,"zone_id":id,"reason":reason}))
            }
            Err(error) => return Err(error),
        }
    }
    Err(Error::PlacementUnavailable(format!(
        "all {} explicitly selected placements are unavailable",
        attempts.len()
    )))
}

/// Read a named stack and reject foreign ownership, including retained failed stacks.
pub fn status(api: &mut impl Api, scope: &Scope) -> Result<Option<Value>, Error> {
    match api.call(
        "cloudformation",
        "describe-stacks",
        json!({"StackName":scope.stack}),
    ) {
        Ok(value) => {
            let stack = value["Stacks"]
                .as_array()
                .and_then(|a| a.first())
                .ok_or(Error::Missing("Stacks"))?
                .clone();
            if !owned(&stack) && stack["StackStatus"] != "REVIEW_IN_PROGRESS" {
                return Err(Error::Config(
                    "existing stack does not have project ownership tag".into(),
                ));
            }
            Ok(Some(stack))
        }
        Err(e) if e.aws_code("StackNotFound") => Ok(None),
        Err(e) => Err(e),
    }
}
/// Create a bounded inspectable change set. This creates planning metadata only.
pub fn plan(api: &mut impl Api, scope: &Scope, target: &Target) -> Result<Value, Error> {
    plan_parameters(
        api,
        scope,
        vec![
            json!({"ParameterKey":"AmiId","ParameterValue":target.ami}),
            json!({"ParameterKey":"ZoneName","ParameterValue":target.zone_name}),
            json!({"ParameterKey":"ZoneId","ParameterValue":target.zone_id}),
            json!({"ParameterKey":"InstanceType","ParameterValue":target.instance_type}),
            json!({"ParameterKey":"BoundaryArn","ParameterValue":scope.boundary_arn()}),
            json!({"ParameterKey":"TimingPlacement","ParameterValue":"precision-time"}),
        ],
    )
}

/// Preserve presentation parameters during ordinary host replanning.
pub(super) fn plan_parameters(
    api: &mut impl Api,
    scope: &Scope,
    mut parameters: Vec<Value>,
) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    api.call(
        "cloudformation",
        "validate-template",
        json!({"TemplateBody":HOST_TEMPLATE}),
    )?;
    let existing = status(api, scope)?;
    let kind = match existing.as_ref().and_then(|s| s["StackStatus"].as_str()) {
        None | Some("REVIEW_IN_PROGRESS") => "CREATE",
        Some("CREATE_COMPLETE" | "UPDATE_COMPLETE" | "UPDATE_ROLLBACK_COMPLETE") => "UPDATE",
        Some(other) => {
            return Err(Error::Config(format!(
                "stack status {other} requires recovery before planning"
            )));
        }
    };
    for parameter in existing
        .as_ref()
        .and_then(|s| s["Parameters"].as_array())
        .into_iter()
        .flatten()
    {
        if !parameters
            .iter()
            .any(|p| p["ParameterKey"] == parameter["ParameterKey"])
        {
            parameters
                .push(json!({"ParameterKey":parameter["ParameterKey"],"UsePreviousValue":true}));
        }
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let name = format!("pax-plan-{stamp}");
    let created = api.call(
        "cloudformation",
        "create-change-set",
        json!({
            "StackName":scope.stack,"ChangeSetName":name,"ChangeSetType":kind,
            "TemplateBody":HOST_TEMPLATE,"Capabilities":["CAPABILITY_NAMED_IAM"],
            "Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"}],
            "Parameters":parameters
        }),
    )?;
    let arn = created["Id"]
        .as_str()
        .ok_or(Error::Missing("change set Id"))?;
    let start = Instant::now();
    loop {
        let change = api.call(
            "cloudformation",
            "describe-change-set",
            json!({"ChangeSetName":arn,"StackName":scope.stack}),
        )?;
        match change["Status"].as_str() {
            Some("CREATE_COMPLETE") => return Ok(change),
            Some("FAILED")
                if change["StatusReason"].as_str().is_some_and(|s| {
                    s.contains("didn't contain changes") || s.contains("No updates")
                }) =>
            {
                api.call(
                    "cloudformation",
                    "delete-change-set",
                    json!({"ChangeSetName":arn,"StackName":scope.stack}),
                )?;
                return Ok(json!({"noop":true,"stack":scope.stack}));
            }
            Some("FAILED") => {
                return Err(Error::Config(format!(
                    "CloudFormation change set failed: {}",
                    change["StatusReason"].as_str().unwrap_or("unspecified")
                )));
            }
            _ if start.elapsed() > Duration::from_secs(180) => {
                return Err(Error::Config(
                    "change set creation timed out; inspect planning metadata before retry".into(),
                ));
            }
            _ => std::thread::sleep(Duration::from_secs(1)),
        }
    }
}
/// Apply only a ready change set belonging to the expected named owned stack.
/// Removals and potential replacements require explicit CLI authorization.
pub fn apply(
    api: &mut impl Api,
    scope: &Scope,
    change_set: &str,
    allow_replacement: bool,
) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let stack = status(api, scope)?.ok_or(Error::Missing("owned planning stack"))?;
    let change = api.call(
        "cloudformation",
        "describe-change-set",
        json!({"ChangeSetName":change_set,"StackName":scope.stack}),
    )?;
    if change["StackName"] != scope.stack
        || change["StackId"] != stack["StackId"]
        || change["Status"] != "CREATE_COMPLETE"
        || change["ExecutionStatus"] != "AVAILABLE"
    {
        return Err(Error::Config(
            "change set is not available for the expected stack".into(),
        ));
    }
    // CREATE change sets leave an untagged REVIEW_IN_PROGRESS placeholder until
    // execution. Ownership is carried by the change set, not that placeholder.
    if !owned(&change) {
        return Err(Error::Config(
            "change set lacks project ownership tag".into(),
        ));
    }
    let destructive = change["Changes"]
        .as_array()
        .ok_or(Error::Missing("Changes"))?
        .iter()
        .any(|c| {
            c["ResourceChange"]["Action"] == "Remove"
                || c["ResourceChange"]["Replacement"]
                    .as_str()
                    .is_some_and(|r| r != "False")
        });
    if destructive && !allow_replacement {
        return Err(Error::Config("change set removes or may replace resources; require --allow-replacement after inspecting it".into()));
    }
    api.call(
        "cloudformation",
        "execute-change-set",
        json!({"ChangeSetName":change_set,"StackName":scope.stack}),
    )?;
    Ok(json!({"execution_started":true,"stack":scope.stack,"change_set":change_set}))
}
/// Delete an owned static stack, returning the inventory first. S3 artifacts are retained.
pub fn destroy(api: &mut impl Api, scope: &Scope) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let stack = status(api, scope)?.ok_or(Error::Missing("owned stack"))?;
    if !owned(&stack) {
        return Err(Error::Config("untagged planning placeholder requires change-set reconciliation, not resource teardown".into()));
    }
    if stack["StackStatus"]
        .as_str()
        .is_some_and(|s| s.ends_with("IN_PROGRESS"))
    {
        return Err(Error::Config(
            "stack operation is in progress; wait before destroy".into(),
        ));
    }
    let resources = api.call(
        "cloudformation",
        "list-stack-resources",
        json!({"StackName":scope.stack}),
    )?;
    api.call(
        "cloudformation",
        "delete-stack",
        json!({"StackName":scope.stack}),
    )?;
    Ok(
        json!({"deletion_started":true,"resources":resources,"retained":"artifact bucket; inventory and remove retained releases explicitly"}),
    )
}
fn owned(value: &Value) -> bool {
    value["Tags"].as_array().is_some_and(|tags| {
        tags.iter()
            .any(|tag| tag["Key"] == "pax:owner" && tag["Value"] == "pax-markets-rs")
    })
}

/// Recover a failed owned stack without discarding retained data. Recreating a
/// failed initial stack is explicit and only removes empty owned artifact buckets.
pub fn recover(api: &mut impl Api, scope: &Scope, recreate_failed: bool) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let stack = status(api, scope)?.ok_or(Error::Missing("owned failed stack"))?;
    if !owned(&stack) {
        return Err(Error::Config("recovery requires owned stack tags".into()));
    }
    if stack["StackStatus"] == "UPDATE_ROLLBACK_FAILED" {
        api.call(
            "cloudformation",
            "continue-update-rollback",
            json!({"StackName":scope.stack}),
        )?;
        return Ok(json!({"rollback_resumed":true}));
    }
    if !["ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "DELETE_FAILED"]
        .iter()
        .any(|s| stack["StackStatus"] == *s)
        || !recreate_failed
    {
        return Err(Error::Config(
            "failed stack recreation requires completed/failed rollback or deletion and --recreate-failed"
                .into(),
        ));
    }
    let resources = api.call(
        "cloudformation",
        "list-stack-resources",
        json!({"StackName":scope.stack}),
    )?;
    let entries = resources["StackResourceSummaries"]
        .as_array()
        .ok_or(Error::Missing("StackResourceSummaries"))?;
    let mut ghosts = Vec::new();
    for resource in entries {
        if resource["ResourceStatus"] == "DELETE_COMPLETE" {
            continue;
        }
        if resource["ResourceType"] == "AWS::IAM::Role"
            && resource["ResourceStatus"] == "DELETE_FAILED"
        {
            let name = resource["PhysicalResourceId"]
                .as_str()
                .ok_or(Error::Missing("failed role name"))?;
            if name != format!("{}-runtime", scope.stack) {
                return Err(Error::Config(
                    "failed IAM role is outside exact stack runtime name".into(),
                ));
            }
            match api.call("iam","get-role",json!({"RoleName":name})) {
                Err(error) if error.aws_code("NoSuchEntity")=>ghosts.push(resource["LogicalResourceId"].as_str().ok_or(Error::Missing("ghost logical resource"))?.to_owned()),
                Err(error)=>return Err(error),
                Ok(_)=>return Err(Error::Config("failed IAM role actually exists; restore scoped deletion permissions before recreating".into())),
            }
            continue;
        }
        if resource["ResourceType"] != "AWS::S3::Bucket"
            || !["DELETE_SKIPPED", "CREATE_COMPLETE"]
                .iter()
                .any(|s| resource["ResourceStatus"] == *s)
        {
            return Err(Error::Config("failed stack still has nonempty or unrecognized retained resources; inspect before retry".into()));
        }
        let bucket = resource["PhysicalResourceId"]
            .as_str()
            .ok_or(Error::Missing("retained bucket"))?;
        let prefix = format!("pax-{}-{}-", scope.account, scope.region);
        if !bucket.starts_with(&prefix) {
            return Err(Error::Config(
                "retained bucket is outside project namespace".into(),
            ));
        }
        let tags = api.call(
            "s3api",
            "get-bucket-tagging",
            json!({"Bucket":bucket,"ExpectedBucketOwner":scope.account}),
        )?;
        if !tags["TagSet"].as_array().is_some_and(|tags| {
            tags.iter()
                .any(|t| t["Key"] == "pax:owner" && t["Value"] == "pax-markets-rs")
        }) {
            return Err(Error::Config("retained bucket is not project owned".into()));
        }
        let objects = api.call(
            "s3api",
            "list-objects-v2",
            json!({"Bucket":bucket,"MaxKeys":1,"ExpectedBucketOwner":scope.account}),
        )?;
        if objects["KeyCount"] != 0 || objects["IsTruncated"] == true {
            return Err(Error::Config(
                "retained artifact bucket contains data; refusing recovery deletion".into(),
            ));
        }
        api.call(
            "s3api",
            "delete-bucket",
            json!({"Bucket":bucket,"ExpectedBucketOwner":scope.account}),
        )?;
    }
    let mut deletion = json!({"StackName":scope.stack});
    let mut retained_retry = stack["StackStatus"] == "DELETE_FAILED" && !ghosts.is_empty();
    if stack["StackStatus"] == "DELETE_FAILED" && !ghosts.is_empty() {
        deletion["RetainResources"] = json!(ghosts);
    }
    api.call("cloudformation", "delete-stack", deletion)?;
    let start = Instant::now();
    loop {
        let current = status(api, scope)?;
        if current.is_none() {
            return Ok(
                json!({"failed_stack_removed":true,"retained_data_deleted":false,"next":"plan then apply the corrected template"}),
            );
        }
        if current
            .as_ref()
            .is_some_and(|s| s["StackStatus"] == "DELETE_FAILED")
        {
            if ghosts.is_empty() || retained_retry {
                return Err(Error::Config(
                    "stack deletion failed; inspect resource diagnostics".into(),
                ));
            }
            api.call(
                "cloudformation",
                "delete-stack",
                json!({"StackName":scope.stack,"RetainResources":ghosts}),
            )?;
            retained_retry = true;
        }
        if start.elapsed() > Duration::from_secs(180) {
            return Err(Error::Config(
                "failed stack deletion is still pending; inspect status before planning".into(),
            ));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope() -> Scope {
        Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap()
    }
    fn stack_state(state: &str) -> Value {
        json!({"Stacks":[{"StackStatus":state,"Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"}]}]})
    }
    #[test]
    fn configured_fallback_is_recorded_and_default_has_no_alternative() {
        let scope = scope();
        let primary = [
            Ok(json!({"Account":scope.account})),
            Ok(json!({"AvailabilityZones":[]})),
        ];
        let mut api = super::super::fake::Fake::new(primary);
        assert!(matches!(
            resolve_with_fallback(
                &mut api,
                &scope,
                None,
                None,
                Some("ami-test"),
                "c7a.xlarge",
                &[]
            ),
            Err(Error::PlacementUnavailable(_))
        ));
        assert_eq!(api.calls.len(), 2);
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"AvailabilityZones":[]})),
            Ok(json!({"Account":scope.account})),
            Ok(
                json!({"AvailabilityZones":[{"ZoneName":"ap-northeast-1c","ZoneId":"apne1-az1","State":"available","ZoneType":"availability-zone"}]}),
            ),
            Ok(json!({"InstanceTypeOfferings":[{}]})),
            Ok(
                json!({"Images":[{"Architecture":"x86_64","State":"available","Name":"al2023-ami-test"}]}),
            ),
        ]);
        let receipt = resolve_with_fallback(
            &mut api,
            &scope,
            None,
            None,
            Some("ami-test"),
            "c7a.xlarge",
            &["apne1-az1".into()],
        )
        .unwrap();
        assert_eq!(receipt["zone_id"], "apne1-az1");
        assert_eq!(receipt["explicit_fallback_used"], true);
        assert_eq!(receipt["placement_attempts"].as_array().unwrap().len(), 1);
    }
    #[test]
    fn authentication_failure_never_triggers_fallback() {
        let scope = scope();
        let mut api = super::super::fake::Fake::new([Err(Error::Aws {
            service: "sts".into(),
            operation: "get-caller-identity".into(),
            code: "AccessDenied".into(),
        })]);
        assert!(
            resolve_with_fallback(
                &mut api,
                &scope,
                None,
                None,
                None,
                "c7a.xlarge",
                &["apne1-az1".into()]
            )
            .is_err()
        );
        assert_eq!(api.calls.len(), 1);
    }
    #[test]
    fn in_progress_recovery_and_teardown_do_not_mutate() {
        let scope = scope();
        for recovery in [true, false] {
            let mut api = super::super::fake::Fake::new([
                Ok(json!({"Account":scope.account})),
                Ok(stack_state("CREATE_IN_PROGRESS")),
            ]);
            assert!(
                if recovery {
                    recover(&mut api, &scope, true)
                } else {
                    destroy(&mut api, &scope)
                }
                .is_err()
            );
            assert_eq!(api.calls.len(), 2);
        }
    }
    #[test]
    fn retained_data_prevents_recreation() {
        let scope = scope();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(stack_state("ROLLBACK_COMPLETE")),
            Ok(
                json!({"StackResourceSummaries":[{"ResourceType":"AWS::S3::Bucket","ResourceStatus":"DELETE_SKIPPED","PhysicalResourceId":"pax-123456789012-ap-northeast-1-pax-test"}]}),
            ),
            Ok(json!({"TagSet":[{"Key":"pax:owner","Value":"pax-markets-rs"}]})),
            Ok(json!({"KeyCount":1,"IsTruncated":false})),
        ]);
        assert!(recover(&mut api, &scope, true).is_err());
        assert!(!api.calls.iter().any(|(_, op, _)| op.starts_with("delete-")));
    }
    #[test]
    fn only_verified_absent_role_metadata_can_be_retained() {
        let scope = scope();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(stack_state("DELETE_FAILED")),
            Ok(
                json!({"StackResourceSummaries":[{"ResourceType":"AWS::IAM::Role","ResourceStatus":"DELETE_FAILED","PhysicalResourceId":"pax-test-runtime","LogicalResourceId":"RuntimeRole"}]}),
            ),
            Err(Error::Aws {
                service: "iam".into(),
                operation: "get-role".into(),
                code: "NoSuchEntity".into(),
            }),
            Ok(json!({})),
            Err(Error::Aws {
                service: "cloudformation".into(),
                operation: "describe-stacks".into(),
                code: "StackNotFound".into(),
            }),
        ]);
        assert_eq!(
            recover(&mut api, &scope, true).unwrap()["failed_stack_removed"],
            true
        );
        let deleted = api
            .calls
            .iter()
            .find(|(_, op, _)| op == "delete-stack")
            .unwrap();
        assert_eq!(deleted.2["RetainResources"], json!(["RuntimeRole"]));
    }
    #[test]
    fn template_has_no_extra_paid_network_fleet_or_credentials() {
        let template: Value = serde_json::from_str(HOST_TEMPLATE).unwrap();
        let resources = template["Resources"].as_object().unwrap();
        assert_eq!(
            resources
                .values()
                .filter(|r| r["Type"] == "AWS::EC2::Instance")
                .count(),
            1
        );
        assert!(!resources.values().any(|r| matches!(
            r["Type"].as_str(),
            Some(
                "AWS::EC2::NatGateway"
                    | "AWS::ElasticLoadBalancingV2::LoadBalancer"
                    | "AWS::EC2::VPCEndpoint"
            )
        )));
        assert_eq!(
            resources["Host"]["Properties"]["BlockDeviceMappings"][0]["Ebs"]["Encrypted"],
            true
        );
        assert_eq!(
            resources["Host"]["Properties"]["MetadataOptions"]["HttpTokens"],
            "required"
        );
        assert_eq!(
            resources["RuntimeRole"]["Properties"]["PermissionsBoundary"],
            json!({"Ref":"BoundaryArn"})
        );
        assert!(!HOST_TEMPLATE.contains("ACCESS_KEY"));
    }
    #[test]
    fn ordinary_host_plan_keeps_existing_https_and_origin_restriction() {
        let scope = scope();
        let host = json!({"StackStatus":"UPDATE_COMPLETE","Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"}],"Parameters":[{"ParameterKey":"DemoHttps","ParameterValue":"true"},{"ParameterKey":"OriginPrefixList","ParameterValue":"pl-123abc"}]});
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({})),
            Ok(json!({"Stacks":[host]})),
            Ok(json!({"Id":"change"})),
            Ok(json!({"Status":"CREATE_COMPLETE"})),
        ]);
        let target = Target {
            zone_name: "ap-northeast-1a".into(),
            zone_id: "apne1-az4".into(),
            ami: "ami-test".into(),
            instance_type: "c7a.xlarge".into(),
        };
        plan(&mut api, &scope, &target).unwrap();
        let rows = api
            .calls
            .iter()
            .find(|(_, op, _)| op == "create-change-set")
            .unwrap()
            .2["Parameters"]
            .as_array()
            .unwrap();
        for key in ["DemoHttps", "OriginPrefixList"] {
            assert!(
                rows.iter()
                    .any(|r| r["ParameterKey"] == key && r["UsePreviousValue"] == true)
            );
        }
    }
    #[test]
    fn conflicting_account_specific_zone_mapping_fails() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(
                json!({"AvailabilityZones":[{"ZoneName":"ap-northeast-1c","ZoneId":"apne1-az1","State":"available","ZoneType":"availability-zone"}]}),
            ),
        ]);
        assert!(
            resolve(
                &mut api,
                &scope,
                Some("ap-northeast-1a"),
                Some("apne1-az1"),
                None,
                "c7a.xlarge"
            )
            .is_err()
        );
        assert_eq!(api.calls.len(), 2);
    }
    #[test]
    fn foreign_stack_is_not_mutated() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"Stacks":[{"StackStatus":"CREATE_COMPLETE","Tags":[]}]})),
        ]);
        assert!(destroy(&mut api, &scope).is_err());
        assert_eq!(api.calls.len(), 2);
    }
    #[test]
    fn create_placeholder_uses_change_set_ownership() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        for ownership in [true, false] {
            let mut replies = vec![
                Ok(json!({"Account":scope.account})),
                Ok(json!({"Stacks":[{"StackId":"stack-id","StackStatus":"REVIEW_IN_PROGRESS"}]})),
                Ok(
                    json!({"StackName":scope.stack,"StackId":"stack-id","Status":"CREATE_COMPLETE","ExecutionStatus":"AVAILABLE","Tags":if ownership {json!([{"Key":"pax:owner","Value":"pax-markets-rs"}])} else {json!([])},"Changes":[{"ResourceChange":{"Action":"Add"}}]}),
                ),
            ];
            if ownership {
                replies.push(Ok(json!({})));
            }
            let mut api = super::super::fake::Fake::new(replies);
            assert_eq!(
                apply(&mut api, &scope, "pax-plan-test", false).is_ok(),
                ownership
            );
            assert_eq!(
                api.calls
                    .iter()
                    .any(|(_, operation, _)| operation == "execute-change-set"),
                ownership
            );
        }
    }
    #[test]
    fn replacements_are_refused_before_execution() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let tags = json!([{"Key":"pax:owner","Value":"pax-markets-rs"}]);
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(
                json!({"Stacks":[{"StackId":"stack-id","StackStatus":"CREATE_COMPLETE","Tags":tags}]}),
            ),
            Ok(
                json!({"StackName":scope.stack,"StackId":"stack-id","Status":"CREATE_COMPLETE","ExecutionStatus":"AVAILABLE","Tags":tags,"Changes":[{"ResourceChange":{"Action":"Modify","Replacement":"Conditional"}}]}),
            ),
        ]);
        assert!(apply(&mut api, &scope, "pax-plan-test", false).is_err());
        assert_eq!(api.calls.len(), 3);
    }
    #[test]
    fn no_op_plan_deletes_metadata_without_execution() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({})),
            Ok(
                json!({"Stacks":[{"StackStatus":"CREATE_COMPLETE","Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"}]}]}),
            ),
            Ok(json!({"Id":"change-id"})),
            Ok(
                json!({"Status":"FAILED","StatusReason":"The submitted information didn't contain changes."}),
            ),
            Ok(json!({})),
        ]);
        let target = Target {
            zone_name: "ap-northeast-1a".into(),
            zone_id: "apne1-az4".into(),
            ami: "ami-test".into(),
            instance_type: "c7a.xlarge".into(),
        };
        assert_eq!(plan(&mut api, &scope, &target).unwrap()["noop"], true);
        assert_eq!(api.calls.last().unwrap().1, "delete-change-set");
    }
}
