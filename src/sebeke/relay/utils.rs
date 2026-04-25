use anyhow::{Result, anyhow, bail};
use regex::Regex;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use url::Url;
use zenoh::bytes::Encoding;

#[derive(Debug)]
pub enum ResolverError {
    NoMatch(String),
    InvalidPattern,
    NonCanonicalPath,
}

impl std::fmt::Display for ResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolverError::NoMatch(topic) => {
                write!(f, "No matching pattern found for topic '{}'", topic)
            }
            ResolverError::InvalidPattern => {
                write!(
                    f,
                    "Invalid local pattern provided (must follow Zenoh syntax)"
                )
            }
            ResolverError::NonCanonicalPath => {
                write!(f, "Topic is not in canonical form")
            }
        }
    }
}

/// Validates Zenoh canonization.
fn is_canonical(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }
    if path.contains("//") {
        return false;
    }

    // It captures "misaligned" wildcards like 'events**' or 'a**'
    let misaligned_re = Regex::new(r"[^/]\*\*").unwrap();
    if misaligned_re.is_match(path) {
        return false;
    }
    true
}

fn is_valid_zenoh(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }
    if path.ends_with('/') || path.contains("//") {
        return false;
    }

    // It captures "misaligned" wildcards like 'events**' or 'a**'
    let misaligned_re = Regex::new(r"[^/]\*\*").unwrap();
    if misaligned_re.is_match(path) {
        return false;
    }
    true
}

pub fn resolve_zenoh_url(
    local_pattern: &str,
    remote_pattern: &str,
    topic: &str,
) -> Result<String, ResolverError> {
    // 1. Strict Canonization Check (The "Effective" Part)
    if !is_canonical(topic) {
        return Err(ResolverError::NonCanonicalPath);
    }

    if !is_valid_zenoh(&local_pattern) {
        return Err(ResolverError::InvalidPattern);
    }

    // 2. Prepare Regex with capturing groups
    // Note: We use non-greedy matching (.*?) for ** to allow specific segments to take priority
    let mut pattern = local_pattern
        .replace('.', r"\.")
        .replace("**", "___DOUBLE___")
        .replace("$*", "___FRAG___")
        .replace("*", "___SINGLE___");

    pattern = pattern
        .replace("___DOUBLE___", r"(.*)")
        .replace("___SINGLE___", r"([^/]+)")
        .replace("___FRAG___", r"([^/]*)");

    let re = Regex::new(&format!("^{}$", pattern)).map_err(|_| ResolverError::InvalidPattern)?;

    // 3. Match and Map
    if let Some(caps) = re.captures(topic) {
        let mut result = remote_pattern.to_string();

        // Replace wildcards in order of discovery in the remote pattern
        for i in 1..caps.len() {
            let captured_value = &caps[i];
            if let Some((wc, offset)) = find_next_wildcard(&result) {
                result.replace_range(offset..offset + wc.len(), captured_value);
            }
        }
        return Ok(result);
    }

    Err(ResolverError::NoMatch(topic.to_string()))
}

fn find_next_wildcard(s: &str) -> Option<(&'static str, usize)> {
    let wildcards = [("**", 2), ("$*", 2), ("*", 1)];
    let mut earliest: Option<(usize, &'static str)> = None;

    for (wc, _) in wildcards.iter() {
        if let Some(idx) = s.find(wc) {
            if earliest.is_none() || idx < earliest.unwrap().0 {
                earliest = Some((idx, wc));
            }
        }
    }
    earliest.map(|(idx, wc)| (wc, idx))
}

pub fn serialize<T: Serialize>(value: &T, encoding: Encoding) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(128);
    match encoding {
        Encoding::APPLICATION_CBOR => {
            ciborium::into_writer(value, &mut buf)
                .map_err(|e| anyhow!("CBOR serialization failed: {e}"))?;
            Ok(buf)
        }
        _ => Err(anyhow!("unsupported serialization encoding")),
    }
}

pub fn deserialize<T: DeserializeOwned>(bytes: &[u8], encoding: Encoding) -> Result<T> {
    match encoding {
        Encoding::APPLICATION_CBOR => ciborium::from_reader(bytes).map_err(|e| {
            anyhow!(
                "CBOR deserialization into {} failed: {e}",
                std::any::type_name::<T>()
            )
        }),
        _ => Err(anyhow!("unsupported serialization encoding")),
    }
}

pub fn to_worker_ws_url(base_url: &str, ws_path: &str, machine_id: &str) -> Result<Url> {
    let normalized = if ws_path.starts_with('/') {
        ws_path.to_string()
    } else {
        format!("/{ws_path}")
    };

    let mut url =
        Url::parse(base_url).map_err(|_| anyhow!("invalid base worker url: {base_url}"))?;

    match url.scheme() {
        "https" => {
            url.set_scheme("wss")
                .map_err(|_| anyhow!("unable to set scheme wss"))?;
        }
        "http" => {
            url.set_scheme("ws")
                .map_err(|_| anyhow!("unable to set scheme ws"))?;
        }
        "wss" | "ws" => {}
        scheme => bail!("unsupported worker url scheme for websocket: {scheme}"),
    }

    url.set_path(&normalized);
    url.set_query(Some(&format!("machine_id={machine_id}")));
    Ok(url)
}

pub fn sample_fingerprint(topic: &str, data: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    topic.hash(&mut hasher);
    data.hash(&mut hasher);
    hasher.finish()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let result = resolve_zenoh_url("demo/example", "http://target/v1", "demo/example").unwrap();
        assert_eq!(result, "http://target/v1");
    }

    #[test]
    fn test_double_wildcard_suffix() {
        // Should capture multiple segments
        let result = resolve_zenoh_url(
            "sensors/**",
            "http://api/v1/**",
            "sensors/building1/floor2/temp",
        )
        .unwrap();
        assert_eq!(result, "http://api/v1/building1/floor2/temp");
    }

    #[test]
    fn test_single_wildcard_middle() {
        // Should match exactly one segment
        let result = resolve_zenoh_url(
            "room/*/lighting",
            "http://api/electricity/*/power",
            "room/kitchen/lighting",
        )
        .unwrap();
        assert_eq!(result, "http://api/electricity/kitchen/power");

        // Should fail if there are extra segments
        assert!(
            resolve_zenoh_url(
                "room/*/lighting",
                "http://api/electricity/*/power",
                "room/kitchen/area1/lighting"
            )
            .is_err()
        );
    }

    #[test]
    fn test_fragment_wildcard() {
        let result = resolve_zenoh_url("dev-$*", "internal/device-$*", "dev-001").unwrap();
        assert_eq!(result, "internal/device-001");
    }

    #[test]
    fn test_mixed_wildcards() {
        let result = resolve_zenoh_url(
            "geo/*/building/**",
            "map/*/struct/**",
            "geo/london/building/mall/top_floor",
        )
        .unwrap();
        assert_eq!(result, "map/london/struct/mall/top_floor");
    }

    #[test]
    fn test_no_match_error() {
        let result = resolve_zenoh_url("a/b/c", "x/y/z", "a/b/d");
        match result {
            Err(ResolverError::NoMatch(t)) => assert_eq!(t, "a/b/d"),
            _ => panic!("Expected NoMatch error"),
        }
    }

    #[test]
    fn test_empty_wildcard_match() {
        // Zenoh ** can match zero segments if there's a trailing slash or logical break,
        // but typically matches at least one. Our regex (.+) requires at least one char.
        // If you want zero-length matches, change (.+) to (.*) in the code.
        assert_eq!(
            resolve_zenoh_url("data/**", "archive/**", "data/").unwrap(),
            "archive/"
        );
    }

    #[test]
    fn url_pattern_without_trailing_slash() {
        // trim_end_matches("/**") doesn't match, trim_end_matches("**") doesn't match
        // So this is treated as exact match, not wildcard — topic must equal "/events/**"
        assert_eq!(
            resolve_zenoh_url("/events/**", "https://api.example.com/v1", "/events/**").unwrap(),
            "https://api.example.com/v1"
        );
    }

    #[test]
    fn wildcard_local_prefix_without_slash() {
        // invalid local pattern per zenoh syntax. // -> /
        assert!(
            resolve_zenoh_url(
                "/events**",
                "https://api.example.com/v1/**",
                "/events/orders"
            )
            .is_err()
        );
    }

    #[test]
    fn topic_with_double_slash() {
        assert!(
            resolve_zenoh_url("/**", "https://api.example.com/**", "//anything/here/").is_err(),
        );
    }
}
