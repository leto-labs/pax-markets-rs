//! Guest control requires the exact current host in the selected owned stack.
use super::{Api, query};
use crate::{config::Scope, error::Error};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn refuse() -> Error {
    Error::Config(
        "guest target does not match the selected namespace and current owned stack".into(),
    )
}

fn tags(value: &Value) -> Result<BTreeMap<&str, &str>, Error> {
    let mut tags = BTreeMap::new();
    for row in value["Tags"].as_array().ok_or_else(refuse)? {
        let key = row["Key"].as_str().ok_or_else(refuse)?;
        let value = row["Value"].as_str().ok_or_else(refuse)?;
        if tags.insert(key, value).is_some() {
            return Err(refuse());
        }
    }
    Ok(tags)
}

pub(super) fn verify(api: &mut impl Api, scope: &Scope, instance: &str) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let response = query(
        api,
        "ec2",
        "describe-instances",
        json!({"InstanceIds":[instance]}),
    )?;
    let reservations = response["Reservations"].as_array().ok_or_else(refuse)?;
    let mut hosts = Vec::new();
    for reservation in reservations {
        hosts.extend(reservation["Instances"].as_array().ok_or_else(refuse)?);
    }
    if hosts.len() != 1
        || hosts[0]["InstanceId"] != instance
        || hosts[0]["State"]["Name"] != "running"
    {
        return Err(refuse());
    }
    let host = hosts[0];
    let host_tags = tags(host)?;
    if host_tags.get("pax:owner") != Some(&"pax-markets-rs") {
        return Err(refuse());
    }
    let name = *host_tags
        .get("aws:cloudformation:stack-name")
        .ok_or_else(refuse)?;
    if name != scope.stack
        || ["pax:purpose", "pax:run", "pax:expires", "pax:parent"]
            .iter()
            .any(|key| host_tags.contains_key(key))
    {
        return Err(refuse());
    }
    let response = query(
        api,
        "cloudformation",
        "describe-stacks",
        json!({"StackName":name}),
    )?;
    let stacks = response["Stacks"].as_array().ok_or_else(refuse)?;
    if stacks.len() != 1
        || stacks[0]["StackName"] != name
        || !matches!(
            stacks[0]["StackStatus"].as_str(),
            Some("CREATE_COMPLETE" | "UPDATE_COMPLETE" | "UPDATE_ROLLBACK_COMPLETE")
        )
    {
        return Err(refuse());
    }
    let stack_tags = tags(&stacks[0])?;
    if stack_tags.get("pax:owner") != Some(&"pax-markets-rs") {
        return Err(refuse());
    }
    if ["pax:purpose", "pax:parent", "pax:run", "pax:expires"]
        .iter()
        .any(|key| stack_tags.contains_key(key))
    {
        return Err(refuse());
    }
    let response = query(
        api,
        "cloudformation",
        "list-stack-resources",
        json!({"StackName":name}),
    )?;
    let resources = response["StackResourceSummaries"]
        .as_array()
        .ok_or_else(refuse)?;
    let hosts: Vec<_> = resources
        .iter()
        .filter(|r| r["LogicalResourceId"] == "Host")
        .collect();
    if hosts.len() != 1
        || hosts[0]["ResourceType"] != "AWS::EC2::Instance"
        || hosts[0]["PhysicalResourceId"] != instance
        || !matches!(
            hosts[0]["ResourceStatus"].as_str(),
            Some("CREATE_COMPLETE" | "UPDATE_COMPLETE" | "UPDATE_ROLLBACK_COMPLETE")
        )
    {
        return Err(refuse());
    }
    Ok(host.clone())
}

#[cfg(test)]
mod tests;
