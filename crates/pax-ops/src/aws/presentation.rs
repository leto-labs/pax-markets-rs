//! CloudFront presentation lifecycle; the market-data socket never uses this path.
use super::{Api, stack};
use crate::{config::Scope, error::Error};
use serde_json::{Value, json};

fn output<'a>(stack: &'a Value, name: &str) -> Option<&'a str> {
    stack["Outputs"]
        .as_array()?
        .iter()
        .find(|o| o["OutputKey"] == name)?["OutputValue"]
        .as_str()
}

/// Inspect deployed versus propagating HTTPS and the explicit HTTP origin leg.
/// An assigned hostname is not evidence that the distribution is ready.
pub fn status(api: &mut impl Api, scope: &Scope) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let host = stack::status(api, scope)?.ok_or(Error::Missing("owned host stack"))?;
    let distribution = if let Some(id) = output(&host, "DemoDistributionId") {
        api.call("cloudfront", "get-distribution", json!({"Id":id}))?["Distribution"].clone()
    } else {
        Value::Null
    };
    let mut result = describe(&host, distribution);
    if let Some(group) = output(&host, "SecurityGroupId") {
        let effective = api.call(
            "ec2",
            "describe-security-groups",
            json!({"GroupIds":[group]}),
        )?;
        let rules = effective["SecurityGroups"][0]["IpPermissions"]
            .as_array()
            .ok_or(Error::Missing("effective IpPermissions"))?;
        let (public, matches) = origin_readback(&host, rules);
        result["direct_http_public"] = json!(public);
        result["origin_effective_rules"] = json!(rules);
        result["origin_configuration_matches"] = json!(matches);
    }
    Ok(result)
}

fn origin_readback(host: &Value, rules: &[Value]) -> (bool, bool) {
    let ingress = rules
        .iter()
        .filter(|r| {
            r["IpProtocol"] == "-1"
                || (r["IpProtocol"] == "tcp"
                    && r["FromPort"].as_i64().is_some_and(|p| p <= 80)
                    && r["ToPort"].as_i64().is_some_and(|p| p >= 80))
        })
        .collect::<Vec<_>>();
    let public = ingress.iter().any(|r| {
        r["IpRanges"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|ip| ip["CidrIp"] == "0.0.0.0/0")
            || r["Ipv6Ranges"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|ip| ip["CidrIpv6"] == "::/0")
    });
    let restricted = output(host, "OriginAccess") == Some("cloudfront-origin-facing-prefix-list");
    let expected = host["Parameters"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|p| p["ParameterKey"] == "OriginPrefixList")
        .and_then(|p| p["ParameterValue"].as_str())
        .filter(|p| !p.is_empty());
    let matches = if restricted {
        expected.is_some()
            && ingress.len() == 1
            && ingress.iter().all(|r| {
                r["IpProtocol"] == "tcp"
                    && r["FromPort"] == 80
                    && r["ToPort"] == 80
                    && ["IpRanges", "Ipv6Ranges", "UserIdGroupPairs"]
                        .iter()
                        .all(|name| r[*name].as_array().is_none_or(Vec::is_empty))
                    && r["PrefixListIds"]
                        .as_array()
                        .is_some_and(|p| p.len() == 1 && p[0]["PrefixListId"].as_str() == expected)
            })
    } else {
        public
    };
    (public, matches)
}

fn describe(host: &Value, distribution: Value) -> Value {
    let ready = distribution["Status"] == "Deployed"
        && distribution["DistributionConfig"]["Enabled"] == true;
    json!({"ready":ready,"stack_status":host["StackStatus"],"distribution":distribution,
        "canonical_url":ready.then(||output(host,"DemoUrl")).flatten(),
        "latency_url":ready.then(||output(host,"LatencyUrl")).flatten(),
        "assigned_url":output(host,"DemoUrl"),"origin_protocol":"http-only",
        "origin_access":output(host,"OriginAccess").unwrap_or("public-http"),
        "direct_http_public":output(host,"OriginAccess").unwrap_or("public-http")=="public-http",
        "certificate":"CloudFront supplied; default security policy",
        "ingestion":"direct Pax WSS; independent of presentation",
        "retained_presentation_resources":{
            "distribution_id":output(host,"DemoDistributionId"),
            "query_policy_id":distribution["DistributionConfig"]["DefaultCacheBehavior"]["OriginRequestPolicyId"],
            "distribution_status":distribution["Status"],
            "disposition":"kept for the running demo; inspect status/events after any interrupted teardown"}})
}

/// Plan only presentation parameters, retaining every host/placement/release value.
/// Restricting HTTP requires a deployed enabled distribution; HTTPS health must also
/// be checked by the operator before executing that second-stage change set.
pub fn plan(
    api: &mut impl Api,
    scope: &Scope,
    enabled: bool,
    restrict_origin: bool,
) -> Result<Value, Error> {
    if restrict_origin && !enabled {
        return Err(Error::Config(
            "origin restriction requires HTTPS enabled".into(),
        ));
    }
    super::verify_account(api, scope)?;
    let host = stack::status(api, scope)?.ok_or(Error::Missing("owned host stack"))?;
    let prefix = if restrict_origin {
        let id =
            output(&host, "DemoDistributionId").ok_or(Error::Missing("deployed distribution"))?;
        let d = api.call("cloudfront", "get-distribution", json!({"Id":id}))?;
        if d["Distribution"]["Status"] != "Deployed"
            || d["Distribution"]["DistributionConfig"]["Enabled"] != true
        {
            return Err(Error::Config(
                "HTTPS distribution is not deployed and enabled".into(),
            ));
        }
        origin_prefix(api, &host)?
    } else {
        String::new()
    };
    stack::plan_parameters(
        api,
        scope,
        vec![
            json!({"ParameterKey":"DemoHttps","ParameterValue":if enabled {"true"} else {"false"}}),
            json!({"ParameterKey":"OriginPrefixList","ParameterValue":prefix}),
        ],
    )
}

fn origin_prefix(api: &mut impl Api, host: &Value) -> Result<String, Error> {
    let lists = api.call(
        "ec2",
        "describe-managed-prefix-lists",
        json!({"Filters":[
        {"Name":"prefix-list-name","Values":["com.amazonaws.global.cloudfront.origin-facing"]},
        {"Name":"owner-id","Values":["AWS"]}]}),
    )?;
    let entries = lists["PrefixLists"]
        .as_array()
        .ok_or(Error::Missing("PrefixLists"))?;
    if entries.len() != 1 || entries[0]["AddressFamily"] != "IPv4" {
        return Err(Error::Config(
            "expected one AWS CloudFront IPv4 origin prefix list".into(),
        ));
    }
    let prefix = entries[0]["PrefixListId"]
        .as_str()
        .ok_or(Error::Missing("PrefixListId"))?;
    let quota = api.call(
        "service-quotas",
        "get-service-quota",
        json!({"ServiceCode":"vpc","QuotaCode":"L-0EA8095F"}),
    )?;
    let group = output(host, "SecurityGroupId").ok_or(Error::Missing(
        "SecurityGroupId; enable HTTPS before restriction",
    ))?;
    let groups = api.call(
        "ec2",
        "describe-security-groups",
        json!({"GroupIds":[group]}),
    )?;
    let rules = groups["SecurityGroups"][0]["IpPermissions"]
        .as_array()
        .ok_or(Error::Missing("IpPermissions"))?;
    // Include the existing CIDR rule during the update, avoiding transient quota
    // overflow. Unknown additional prefix lists require explicit review.
    let mut slots = 55usize;
    for rule in rules {
        for list in rule["PrefixListIds"].as_array().into_iter().flatten() {
            if list["PrefixListId"] != prefix {
                return Err(Error::Config(
                    "additional prefix-list rule requires quota review".into(),
                ));
            }
        }
        for field in ["IpRanges", "Ipv6Ranges", "UserIdGroupPairs"] {
            slots += rule[field].as_array().map_or(0, Vec::len);
        }
    }
    if quota["Quota"]["Value"]
        .as_f64()
        .is_none_or(|q| q < slots as f64)
    {
        return Err(Error::Config(format!(
            "CloudFront origin restriction needs at least {slots} inbound rule slots"
        )));
    }
    Ok(prefix.to_owned())
}

/// Reject changes outside presentation even when the generic host lifecycle could
/// accept them. Removal is limited to the distribution/query policy, never EC2.
pub fn apply(
    api: &mut impl Api,
    scope: &Scope,
    change_set: &str,
    allow_removal: bool,
) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let change = api.call(
        "cloudformation",
        "describe-change-set",
        json!({"ChangeSetName":change_set,"StackName":scope.stack}),
    )?;
    validate_changes(&change, allow_removal)?;
    stack::apply(api, scope, change_set, allow_removal)
}
fn validate_changes(change: &Value, allow_removal: bool) -> Result<(), Error> {
    for entry in change["Changes"]
        .as_array()
        .ok_or(Error::Missing("Changes"))?
    {
        let r = &entry["ResourceChange"];
        let allowed = matches!(
            (r["LogicalResourceId"].as_str(), r["ResourceType"].as_str()),
            (
                Some("DemoDistribution"),
                Some("AWS::CloudFront::Distribution")
            ) | (
                Some("DemoQueries"),
                Some("AWS::CloudFront::OriginRequestPolicy")
            ) | (Some("SecurityGroup"), Some("AWS::EC2::SecurityGroup"))
        );
        if !allowed
            || r["Replacement"].as_str().is_some_and(|v| v != "False")
            || (r["Action"] == "Remove"
                && (!allow_removal || r["LogicalResourceId"] == "SecurityGroup"))
        {
            return Err(Error::Config(
                "presentation change may alter/remove/replace host resources".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::fake::Fake;
    use super::*;
    #[test]
    fn ambiguous_execution_is_not_retried_or_followed_by_host_teardown() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let tags = json!([{"Key":"pax:owner","Value":"pax-markets-rs"}]);
        let change = json!({"StackName":scope.stack,"StackId":"owned-stack","Status":"CREATE_COMPLETE","ExecutionStatus":"AVAILABLE","Tags":tags,
            "Changes":[{"ResourceChange":{"LogicalResourceId":"DemoDistribution","ResourceType":"AWS::CloudFront::Distribution","Action":"Add"}}]});
        let mut api = Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(change.clone()),
            Ok(json!({"Account":scope.account})),
            Ok(
                json!({"Stacks":[{"StackId":"owned-stack","StackStatus":"UPDATE_COMPLETE","Tags":tags}]}),
            ),
            Ok(change),
            Err(Error::Timeout {
                service: "cloudformation".into(),
                operation: "execute-change-set".into(),
            }),
        ]);
        assert!(apply(&mut api, &scope, "owned-change", false).is_err());
        assert_eq!(api.calls.len(), 6);
        assert_eq!(api.calls.last().unwrap().1, "execute-change-set");
        assert!(!api.calls.iter().any(|(_, op, _)| op == "delete-stack"));
    }
    #[test]
    fn origin_readback_rejects_closed_wrong_prefix_and_public_ipv6_rules() {
        let host = json!({"Outputs":[{"OutputKey":"OriginAccess","OutputValue":"cloudfront-origin-facing-prefix-list"}],"Parameters":[{"ParameterKey":"OriginPrefixList","ParameterValue":"pl-123abc"}]});
        let rule = json!({"IpProtocol":"tcp","FromPort":80,"ToPort":80,"PrefixListIds":[{"PrefixListId":"pl-123abc"}]});
        assert_eq!(
            origin_readback(&host, std::slice::from_ref(&rule)),
            (false, true)
        );
        assert_eq!(origin_readback(&host, &[]), (false, false));
        let mut wrong = rule.clone();
        wrong["PrefixListIds"][0]["PrefixListId"] = json!("pl-456def");
        assert_eq!(origin_readback(&host, &[wrong]), (false, false));
        let mut public = rule;
        public["Ipv6Ranges"] = json!([{"CidrIpv6":"::/0"}]);
        assert_eq!(origin_readback(&host, &[public]), (true, false));
        assert_eq!(
            origin_readback(
                &host,
                &[json!({"IpProtocol":"-1","IpRanges":[{"CidrIp":"10.0.0.0/8"}]})]
            ),
            (false, false)
        );
    }
    #[test]
    fn provisioning_or_disabled_distribution_never_advertises_ready_https() {
        let host = json!({"Outputs":[{"OutputKey":"DemoUrl","OutputValue":"https://example.cloudfront.net/"}]});
        for (state, enabled, ready) in [
            ("InProgress", true, false),
            ("Deployed", false, false),
            ("Deployed", true, true),
        ] {
            let result = describe(
                &host,
                json!({"Status":state,"DistributionConfig":{"Enabled":enabled}}),
            );
            assert_eq!(result["ready"], ready);
            assert_eq!(result["canonical_url"].is_string(), ready);
        }
    }
    #[test]
    fn presentation_cannot_mutate_host_or_replace_security_group() {
        for (name, kind, action, replacement, allow, ok) in [
            (
                "DemoDistribution",
                "AWS::CloudFront::Distribution",
                "Add",
                "False",
                false,
                true,
            ),
            (
                "SecurityGroup",
                "AWS::EC2::SecurityGroup",
                "Modify",
                "False",
                false,
                true,
            ),
            ("Host", "AWS::EC2::Instance", "Modify", "False", true, false),
            (
                "SecurityGroup",
                "AWS::EC2::SecurityGroup",
                "Modify",
                "Conditional",
                true,
                false,
            ),
            (
                "SecurityGroup",
                "AWS::EC2::SecurityGroup",
                "Remove",
                "False",
                true,
                false,
            ),
            (
                "DemoDistribution",
                "AWS::CloudFront::Distribution",
                "Remove",
                "False",
                false,
                false,
            ),
            (
                "DemoDistribution",
                "AWS::CloudFront::Distribution",
                "Remove",
                "False",
                true,
                true,
            ),
        ] {
            let change = json!({"Changes":[{"ResourceChange":{"LogicalResourceId":name,"ResourceType":kind,"Action":action,"Replacement":replacement}}]});
            assert_eq!(validate_changes(&change, allow).is_ok(), ok);
        }
    }
    #[test]
    fn prefix_restriction_checks_aws_owner_and_transient_quota() {
        for (quota, ok) in [(55, false), (56, true)] {
            let mut api = Fake::new([
                Ok(json!({"PrefixLists":[{"PrefixListId":"pl-123abc","AddressFamily":"IPv4"}]})),
                Ok(json!({"Quota":{"Value":quota}})),
                Ok(
                    json!({"SecurityGroups":[{"IpPermissions":[{"IpRanges":[{"CidrIp":"0.0.0.0/0"}]}]}]}),
                ),
            ]);
            let host =
                json!({"Outputs":[{"OutputKey":"SecurityGroupId","OutputValue":"sg-123abc"}]});
            assert_eq!(origin_prefix(&mut api, &host).is_ok(), ok);
            assert_eq!(api.calls[0].2["Filters"][1]["Values"], json!(["AWS"]));
        }
    }
    #[test]
    fn demo_plan_retains_placement_and_cannot_restrict_before_deployment() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let host = json!({"StackStatus":"UPDATE_COMPLETE","Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"}],"Parameters":[{"ParameterKey":"AmiId","ParameterValue":"ami-existing"},{"ParameterKey":"ZoneId","ParameterValue":"apne1-az4"}]});
        let mut api = Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"Stacks":[host.clone()]})),
            Ok(json!({"Account":scope.account})),
            Ok(json!({})),
            Ok(json!({"Stacks":[host.clone()]})),
            Ok(json!({"Id":"change"})),
            Ok(json!({"Status":"CREATE_COMPLETE"})),
        ]);
        plan(&mut api, &scope, true, false).unwrap();
        let parameters = &api
            .calls
            .iter()
            .find(|(_, op, _)| op == "create-change-set")
            .unwrap()
            .2["Parameters"];
        let rows = parameters.as_array().unwrap();
        assert!(
            rows.iter()
                .any(|r| r["ParameterKey"] == "AmiId" && r["UsePreviousValue"] == true)
        );
        assert!(
            rows.iter()
                .any(|r| r["ParameterKey"] == "ZoneId" && r["UsePreviousValue"] == true)
        );
        assert!(
            rows.iter()
                .any(|r| r["ParameterKey"] == "DemoHttps" && r["ParameterValue"] == "true")
        );
        let mut api = Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"Stacks":[host]})),
        ]);
        assert!(plan(&mut api, &scope, true, true).is_err());
        assert_eq!(api.calls.len(), 2);
    }
    #[test]
    fn template_keeps_clock_routes_uncached_and_queries_without_cookies_or_viewer_host() {
        let t: Value = serde_json::from_str(stack::HOST_TEMPLATE).unwrap();
        let d = &t["Resources"]["DemoDistribution"]["Properties"]["DistributionConfig"];
        assert_eq!(
            d["ViewerCertificate"],
            json!({"CloudFrontDefaultCertificate":true})
        );
        assert_eq!(
            d["DefaultCacheBehavior"]["AllowedMethods"],
            json!(["GET", "HEAD"])
        );
        assert_eq!(
            d["DefaultCacheBehavior"]["CachePolicyId"],
            "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
        );
        assert_eq!(
            d["DefaultCacheBehavior"]["ViewerProtocolPolicy"],
            "redirect-to-https"
        );
        assert!(d.get("DefaultRootObject").is_none());
        assert!(
            d["CustomErrorResponses"]
                .as_array()
                .unwrap()
                .iter()
                .all(|e| e["ErrorCachingMinTTL"] == 0 && e.get("ResponseCode").is_none())
        );
        assert_eq!(
            d["Origins"][0]["CustomOriginConfig"]["OriginProtocolPolicy"],
            "http-only"
        );
        let p = &t["Resources"]["DemoQueries"]["Properties"]["OriginRequestPolicyConfig"];
        assert_eq!(p["QueryStringsConfig"]["QueryStringBehavior"], "all");
        assert_eq!(p["CookiesConfig"]["CookieBehavior"], "none");
        assert_eq!(p["HeadersConfig"]["HeaderBehavior"], "none");
    }
}
