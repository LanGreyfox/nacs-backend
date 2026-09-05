use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use dav_server::body::Body;
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH, WWW_AUTHENTICATE};
use hyper::{Method, Response, StatusCode, header::HeaderMap};

pub fn parse_basic_credentials(value: &str) -> Option<(String, String)> {
    let mut parts = value.split_whitespace();
    let scheme = parts.next()?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }

    let token = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let decoded = STANDARD.decode(token).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

fn method_allows_unauthenticated(method: &Method) -> bool {
    method.as_str() == "OPTIONS"
}

pub fn is_authorized(
    headers: &HeaderMap,
    expected_user: &str,
    expected_pass: &str,
    method: &Method,
) -> bool {
    if method_allows_unauthenticated(method) {
        return true;
    }

    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_basic_credentials)
        .map(|(u, p)| u == expected_user && p == expected_pass)
        .unwrap_or(false)
}

pub fn build_unauthorized_response(method: &Method) -> Response<Body> {
    let mut builder = Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            WWW_AUTHENTICATE,
            "Basic realm=\"webdav\", charset=\"UTF-8\"",
        )
        .header(CONTENT_LENGTH, "0");

    if method.as_str() == "OPTIONS" {
        builder = builder
            .header("DAV", "1,2")
            .header("MS-Author-Via", "DAV")
            .header(
                "Allow",
                "OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, COPY, MOVE, LOCK, UNLOCK",
            );
    }

    builder.body(Body::empty()).unwrap()
}

pub fn extract_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_basic_credentials)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderMap;

    #[test]
    fn parse_basic_credentials_valid() {
        let creds = parse_basic_credentials("Basic dXNlcjpwYXNz");
        assert_eq!(creds, Some(("user".to_string(), "pass".to_string())));
    }

    #[test]
    fn parse_basic_credentials_invalid_scheme() {
        let creds = parse_basic_credentials("Bearer token");
        assert_eq!(creds, None);
    }

    #[test]
    fn parse_basic_credentials_malformed() {
        let creds = parse_basic_credentials("Basic notbase64!");
        assert_eq!(creds, None);
    }

    #[test]
    fn is_authorized_correct() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Basic dXNlcjpwYXNz".parse().unwrap());
        assert!(is_authorized(&headers, "user", "pass", &Method::GET));
    }

    #[test]
    fn is_authorized_wrong_password() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Basic dXNlcjp3cm9uZw==".parse().unwrap());
        assert!(!is_authorized(&headers, "user", "pass", &Method::GET));
    }

    #[test]
    fn is_authorized_options_allowed() {
        let headers = HeaderMap::new();
        assert!(is_authorized(&headers, "user", "pass", &Method::OPTIONS));
    }
}
