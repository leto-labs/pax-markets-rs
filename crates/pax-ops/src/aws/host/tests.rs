use super::*;
use crate::aws::fake::Fake;

fn scope() -> Scope {
    Scope::new(
        "123456789012".into(),
        "ap-northeast-1".into(),
        "pax-test".into(),
    )
    .unwrap()
}

fn fixture() -> (Value, Value, Value) {
    let name = "pax-test";
    let mut tags = vec![json!({"Key":"pax:owner","Value":"pax-markets-rs"})];
    let stack = json!({"Stacks":[{"StackName":name,"StackStatus":"CREATE_COMPLETE","Tags":tags}]});
    tags.push(json!({"Key":"aws:cloudformation:stack-name","Value":name}));
    let host = json!({"Reservations":[{"Instances":[{"InstanceId":"i-test","State":{"Name":"running"},"Tags":tags}]}]});
    let resources = json!({"StackResourceSummaries":[{"LogicalResourceId":"Host","ResourceType":"AWS::EC2::Instance","PhysicalResourceId":"i-test","ResourceStatus":"CREATE_COMPLETE"}]});
    (host, stack, resources)
}

fn fake(host: Value, stack: Value, resources: Value) -> Fake {
    Fake::new([
        Ok(json!({"Account":"123456789012"})),
        Ok(host),
        Ok(stack),
        Ok(resources),
    ])
}

fn set_tag(value: &mut Value, key: &str, text: &str) {
    let rows = value["Tags"].as_array_mut().unwrap();
    if let Some(row) = rows.iter_mut().find(|r| r["Key"] == key) {
        row["Value"] = json!(text);
    } else {
        rows.push(json!({"Key":key,"Value":text}));
    }
}

#[test]
fn accepts_current_owned_host() {
    {
        let (host, stack, resources) = fixture();
        let mut api = fake(host, stack, resources);
        let found = verify(&mut api, &scope(), "i-test").unwrap();
        assert_eq!(found["InstanceId"], "i-test");
        assert_eq!(api.calls.len(), 4);
        assert!(
            api.calls
                .iter()
                .all(|(_, op, _)| op.starts_with("describe-")
                    || op == "list-stack-resources"
                    || op == "get-caller-identity")
        );
    }
}

#[test]
fn foreign_namespace_or_ambiguous_metadata_fail_before_stack_queries() {
    for (key, value) in [
        ("aws:cloudformation:stack-name", "pax-foreign"),
        ("pax:run", "unexpected"),
        ("pax:parent", "pax-foreign"),
        ("pax:expires", "1000"),
        ("pax:expires", "not-an-integer"),
        ("pax:run", ""),
        ("pax:run", "fixture/invalid"),
        ("aws:cloudformation:stack-name", "pax-unrelated-0"),
        ("pax:purpose", "unrelated"),
        ("pax:owner", "unrelated"),
    ] {
        let (mut host, stack, resources) = fixture();
        set_tag(&mut host["Reservations"][0]["Instances"][0], key, value);
        let mut api = fake(host, stack, resources);
        assert!(
            verify(&mut api, &scope(), "i-test").is_err(),
            "{key}={value}"
        );
        assert_eq!(api.calls.len(), 2);
    }
    for duplicate in ["pax:owner", "aws:cloudformation:stack-name"] {
        let (mut host, stack, resources) = fixture();
        let tags = host["Reservations"][0]["Instances"][0]["Tags"]
            .as_array_mut()
            .unwrap();
        let row = tags.iter().find(|r| r["Key"] == duplicate).unwrap().clone();
        tags.push(row);
        let mut api = fake(host, stack, resources);
        assert!(verify(&mut api, &scope(), "i-test").is_err());
        assert_eq!(api.calls.len(), 2);
    }
}

#[test]
fn mismatched_run_stack_or_instance_inventory_fails_closed() {
    for key in ["pax:parent", "pax:run", "pax:expires", "pax:owner"] {
        let (host, mut stack, resources) = fixture();
        set_tag(&mut stack["Stacks"][0], key, "foreign");
        let mut api = fake(host, stack, resources);
        assert!(verify(&mut api, &scope(), "i-test").is_err());
        assert_eq!(api.calls.len(), 3);
    }
    for (key, wrong) in [
        ("StackName", "pax-other"),
        ("StackStatus", "UPDATE_IN_PROGRESS"),
    ] {
        let (host, mut stack, resources) = fixture();
        stack["Stacks"][0][key] = json!(wrong);
        let mut api = fake(host, stack, resources);
        assert!(verify(&mut api, &scope(), "i-test").is_err());
        assert_eq!(api.calls.len(), 3);
    }
    for (key, wrong) in [
        ("PhysicalResourceId", "i-replaced"),
        ("LogicalResourceId", "Other"),
        ("ResourceType", "AWS::EC2::NetworkInterface"),
        ("ResourceStatus", "DELETE_COMPLETE"),
    ] {
        let (host, stack, mut resources) = fixture();
        resources["StackResourceSummaries"][0][key] = json!(wrong);
        let mut api = fake(host, stack, resources);
        assert!(verify(&mut api, &scope(), "i-test").is_err());
        assert_eq!(api.calls.len(), 4);
    }
}

#[test]
fn duplicate_inventory_nonrunning_instance_and_wrong_account_are_refused() {
    for component in ["instance", "stack", "resource"] {
        let (mut host, mut stack, mut resources) = fixture();
        let rows = match component {
            "instance" => host["Reservations"][0]["Instances"].as_array_mut().unwrap(),
            "stack" => stack["Stacks"].as_array_mut().unwrap(),
            _ => resources["StackResourceSummaries"].as_array_mut().unwrap(),
        };
        rows.push(rows[0].clone());
        let mut api = fake(host, stack, resources);
        assert!(verify(&mut api, &scope(), "i-test").is_err());
    }
    let (mut host, stack, resources) = fixture();
    host["Reservations"][0]["Instances"][0]["State"]["Name"] = json!("stopped");
    let mut api = fake(host, stack, resources);
    assert!(verify(&mut api, &scope(), "i-test").is_err());
    assert_eq!(api.calls.len(), 2);
    let mut api = Fake::new([Ok(json!({"Account":"999999999999"}))]);
    assert!(verify(&mut api, &scope(), "i-test").is_err());
    assert_eq!(api.calls.len(), 1);
}
