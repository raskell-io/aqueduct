//! AWS Signature Version 4 (ADR 0061): how a request to S3-compatible object storage is signed, as pure
//! functions of the request, the credentials and the time. Checked against AWS's own test suite.

use sha2::{Digest, Sha256};

/// Lowercase hex of bytes.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// SHA-256, as lowercase hex.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// HMAC-SHA256 (RFC 2104).
pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(data);
    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// URI-encodes a string as SigV4 asks: unreserved characters stay, everything else is `%XX`; `/` stays
/// only when `slash` is true (in paths).
pub(crate) fn uri_encode(text: &str, slash: bool) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            b'/' if slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// A request to sign.
pub(crate) struct Request<'a> {
    pub method: &'a str,
    /// The path, already URI-encoded.
    pub path: &'a str,
    /// Query parameters, not yet encoded.
    pub query: &'a [(String, String)],
    /// Headers to sign (names in any case), `host` among them.
    pub headers: &'a [(String, String)],
    /// The payload's SHA-256 as hex, or `UNSIGNED-PAYLOAD`.
    pub payload_hash: &'a str,
}

/// Where and when it is signed.
pub(crate) struct Scope<'a> {
    /// `YYYYMMDDTHHMMSSZ`.
    pub timestamp: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

/// The canonical query string: parameters encoded and sorted.
fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, false), uri_encode(v, false)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The canonical request, and the signed headers' names.
pub(crate) fn canonical_request(request: &Request<'_>) -> (String, String) {
    let mut headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|(name, value)| {
            let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
            (name.to_ascii_lowercase(), value)
        })
        .collect();
    headers.sort();
    let signed = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let mut canonical = format!(
        "{}\n{}\n{}\n",
        request.method,
        request.path,
        canonical_query(request.query)
    );
    for (name, value) in &headers {
        canonical.push_str(&format!("{name}:{value}\n"));
    }
    canonical.push_str(&format!("\n{signed}\n{}", request.payload_hash));
    (canonical, signed)
}

/// The string to sign for a canonical request.
pub(crate) fn string_to_sign(canonical: &str, scope: &Scope<'_>) -> String {
    let date = &scope.timestamp[..8];
    format!(
        "AWS4-HMAC-SHA256\n{}\n{date}/{}/{}/aws4_request\n{}",
        scope.timestamp,
        scope.region,
        scope.service,
        sha256_hex(canonical.as_bytes())
    )
}

/// The signature of a string to sign.
pub(crate) fn signature(secret: &str, scope: &Scope<'_>, to_sign: &str) -> String {
    let date = &scope.timestamp[..8];
    let key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let key = hmac_sha256(&key, scope.region.as_bytes());
    let key = hmac_sha256(&key, scope.service.as_bytes());
    let key = hmac_sha256(&key, b"aws4_request");
    hex(&hmac_sha256(&key, to_sign.as_bytes()))
}

/// The `Authorization` header for a request.
pub(crate) fn authorization(
    access_key: &str,
    secret: &str,
    scope: &Scope<'_>,
    request: &Request<'_>,
) -> String {
    let (canonical, signed) = canonical_request(request);
    let to_sign = string_to_sign(&canonical, scope);
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{}/{}/{}/aws4_request, SignedHeaders={signed}, Signature={}",
        &scope.timestamp[..8],
        scope.region,
        scope.service,
        signature(secret, scope, &to_sign)
    )
}

/// `YYYYMMDDTHHMMSSZ` for seconds since the Unix epoch.
pub(crate) fn timestamp(seconds: u64) -> String {
    let days = seconds / 86_400;
    let rest = seconds % 86_400;
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rest / 3_600,
        rest % 3_600 / 60,
        rest % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn scope() -> Scope<'static> {
        Scope {
            timestamp: "20150830T123600Z",
            region: "us-east-1",
            service: "service",
        }
    }

    /// AWS's test suite (aws-c-auth, tests/aws-signing-test-suite/v4/get-vanilla): every stage.
    #[test]
    fn get_vanilla_matches_the_test_suite() {
        let headers = vec![
            ("Host".to_owned(), "example.amazonaws.com".to_owned()),
            ("X-Amz-Date".to_owned(), "20150830T123600Z".to_owned()),
        ];
        let request = Request {
            method: "GET",
            path: "/",
            query: &[],
            headers: &headers,
            payload_hash: EMPTY,
        };
        let (canonical, signed) = canonical_request(&request);
        assert_eq!(
            canonical,
            "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(signed, "host;x-amz-date");
        let to_sign = string_to_sign(&canonical, &scope());
        assert_eq!(
            to_sign,
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
             bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );
        assert_eq!(
            signature(SECRET, &scope(), &to_sign),
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    /// The same suite's post-header-key-sort: headers sorted by lowercase name.
    #[test]
    fn post_header_key_sort_matches_the_test_suite() {
        let headers = vec![
            ("Host".to_owned(), "example.amazonaws.com".to_owned()),
            ("My-Header1".to_owned(), "value1".to_owned()),
            ("X-Amz-Date".to_owned(), "20150830T123600Z".to_owned()),
        ];
        let request = Request {
            method: "POST",
            path: "/",
            query: &[],
            headers: &headers,
            payload_hash: EMPTY,
        };
        let authorization = authorization("AKIDEXAMPLE", SECRET, &scope(), &request);
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;my-header1;x-amz-date, \
             Signature=c5410059b04c1ee005303aed430f6e6645f61f4dc9e1461ec8f8916fdf18852c"
        );
    }

    #[test]
    fn helpers() {
        // RFC 4231, test case 2.
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        assert_eq!(uri_encode("a b/c~", true), "a%20b/c~");
        assert_eq!(uri_encode("a b/c", false), "a%20b%2Fc");
        assert_eq!(timestamp(1_440_938_160), "20150830T123600Z");
        assert_eq!(timestamp(0), "19700101T000000Z");
    }
}
