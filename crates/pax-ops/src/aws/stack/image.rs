//! Retain the deployed literal image; latest discovery is only a first resolution.
use super::{Api, Error, Scope, status};
use serde_json::json;

pub(super) fn select(
    api: &mut impl Api,
    scope: &Scope,
    requested: Option<&str>,
) -> Result<String, Error> {
    if let Some(image) = requested {
        return Ok(image.to_owned()); // Parent resolver still validates the official image.
    }
    if let Some(stack) = status(api, scope)?
        && stack["StackStatus"] != "REVIEW_IN_PROGRESS"
    {
        let parameters = stack["Parameters"]
            .as_array()
            .ok_or(Error::Missing("stack Parameters"))?;
        let mut images = parameters.iter().filter(|p| p["ParameterKey"] == "AmiId");
        let image = images
            .next()
            .and_then(|p| p["ParameterValue"].as_str())
            .ok_or(Error::Missing("deployed AmiId parameter"))?;
        if images.next().is_some() || !literal(image) {
            return Err(Error::Config("deployed AmiId must be one literal image ID; supply an explicit reviewed --ami instead of silently selecting latest".into()));
        }
        return Ok(image.to_owned());
    }
    Ok(api.call(
        "ssm",
        "get-parameter",
        json!({"Name":"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"}),
    )?["Parameter"]["Value"]
        .as_str()
        .ok_or(Error::Missing("AMI Parameter.Value"))?
        .to_owned())
}

fn literal(image: &str) -> bool {
    image.strip_prefix("ami-").is_some_and(|id| {
        matches!(id.len(), 8 | 17)
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::fake::Fake;
    use serde_json::Value;

    const IMAGE: &str = "ami-0123456789abcdef0";
    fn scope() -> Scope {
        Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap()
    }
    fn deployed(parameters: Value) -> Value {
        json!({"Stacks":[{"StackStatus":"UPDATE_COMPLETE","Parameters":parameters,"Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"}]}]})
    }
    fn parameter(image: &str) -> Value {
        json!({"ParameterKey":"AmiId","ParameterValue":image})
    }

    #[test]
    fn existing_image_is_retained_without_latest_query() {
        let mut api = Fake::new([Ok(deployed(json!([parameter(IMAGE)])))]);
        assert_eq!(select(&mut api, &scope(), None).unwrap(), IMAGE);
        assert_eq!(api.calls.len(), 1);
        assert_eq!(api.calls[0].1, "describe-stacks");
    }

    #[test]
    fn missing_or_unprovisioned_stack_discovers_first_literal() {
        for missing in [
            Err(Error::Aws {
                service: "cloudformation".into(),
                operation: "describe-stacks".into(),
                code: "StackNotFound".into(),
            }),
            Ok(json!({"Stacks":[{"StackStatus":"REVIEW_IN_PROGRESS"}]})),
        ] {
            let mut api = Fake::new([missing, Ok(json!({"Parameter":{"Value":IMAGE}}))]);
            assert_eq!(select(&mut api, &scope(), None).unwrap(), IMAGE);
            assert_eq!(api.calls[1].1, "get-parameter");
        }
    }

    #[test]
    fn explicit_selection_avoids_default_discovery() {
        let mut api = Fake::new([]);
        assert_eq!(select(&mut api, &scope(), Some(IMAGE)).unwrap(), IMAGE);
        assert!(api.calls.is_empty());
    }

    #[test]
    fn invalid_deployed_parameters_never_fall_back_to_latest() {
        for parameters in [
            json!([]),
            json!([parameter(IMAGE), parameter(IMAGE)]),
            json!([parameter("/aws/service/latest")]),
            json!([parameter("ami-not-literal")]),
            json!([{"ParameterKey":"AmiId","ResolvedValue":IMAGE}]),
            json!(null),
        ] {
            let mut api = Fake::new([Ok(deployed(parameters))]);
            assert!(select(&mut api, &scope(), None).is_err());
            assert_eq!(api.calls.len(), 1);
        }
    }

    #[test]
    fn foreign_stack_and_query_errors_cannot_select_an_image() {
        let mut foreign = deployed(json!([parameter(IMAGE)]));
        foreign["Stacks"][0]["Tags"][0]["Value"] = json!("another-owner");
        for response in [
            Ok(foreign),
            Err(Error::Aws {
                service: "cloudformation".into(),
                operation: "describe-stacks".into(),
                code: "AccessDenied".into(),
            }),
        ] {
            let mut api = Fake::new([response]);
            assert!(select(&mut api, &scope(), None).is_err());
            assert_eq!(api.calls.len(), 1);
        }
    }

    #[test]
    fn retained_image_still_requires_official_available_al2023_validation() {
        let scope = scope();
        for image in [
            json!({"Architecture":"x86_64","State":"available","Name":"al2023-ami-test"}),
            json!({"Architecture":"arm64","State":"available","Name":"al2023-ami-test"}),
            json!({"Architecture":"x86_64","State":"deregistered","Name":"al2023-ami-test"}),
            json!({"Architecture":"x86_64","State":"available","Name":"another-os"}),
        ] {
            let expected = image["Architecture"] == "x86_64"
                && image["State"] == "available"
                && image["Name"] == "al2023-ami-test";
            let mut api = Fake::new([
                Ok(json!({"Account":scope.account})),
                Ok(
                    json!({"AvailabilityZones":[{"ZoneName":"ap-northeast-1a","ZoneId":"apne1-az4","State":"available","ZoneType":"availability-zone"}]}),
                ),
                Ok(json!({"InstanceTypeOfferings":[{}]})),
                Ok(deployed(json!([parameter(IMAGE)]))),
                Ok(json!({"Images":[image]})),
            ]);
            let resolved = super::super::resolve(&mut api, &scope, None, None, None, "c7a.xlarge");
            assert_eq!(resolved.is_ok(), expected);
            if let Ok(target) = resolved {
                assert_eq!(target.ami, IMAGE);
            }
            assert_eq!(api.calls.len(), 5);
            assert_eq!(api.calls[4].1, "describe-images");
            assert_eq!(
                api.calls[4].2,
                json!({"ImageIds":[IMAGE],"Owners":["amazon"]})
            );
        }
    }
}
