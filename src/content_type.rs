//! Document content type definitions.
//!
//! This module defines the ContentType enum representing the supported
//! document formats for CRDT operations. Each type determines:
//! - How the Yrs document is structured (text, map, array, or xml fragment)
//! - The MIME type for HTTP responses
//! - The default content for new documents

/// Supported document content types.
///
/// Each variant maps to a specific Yrs CRDT structure and has associated
/// MIME types and default content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentType {
    /// JSON object (`{}`) using Y.Map
    Json,
    /// JSON array (`[]`) using Y.Array
    JsonArray,
    /// XML document using Y.XmlFragment
    Xml,
    /// Plain text using Y.Text
    Text,
}

impl ContentType {
    /// Parse a MIME type string to determine the ContentType.
    ///
    /// Supports parameters like `application/json;root=array` for JsonArray.
    ///
    /// # Examples
    /// ```
    /// use commonplace_doc::content_type::ContentType;
    ///
    /// assert_eq!(ContentType::from_mime("application/json"), Some(ContentType::Json));
    /// assert_eq!(ContentType::from_mime("application/json;root=array"), Some(ContentType::JsonArray));
    /// assert_eq!(ContentType::from_mime("text/plain"), Some(ContentType::Text));
    /// ```
    pub fn from_mime(mime: &str) -> Option<Self> {
        let mut parts = mime.split(';').map(|part| part.trim());
        let base = parts.next().unwrap_or_default();
        let params: Vec<&str> = parts.collect();

        match base {
            "application/json" => {
                if params.iter().any(|p| p.eq_ignore_ascii_case("root=array")) {
                    Some(ContentType::JsonArray)
                } else {
                    Some(ContentType::Json)
                }
            }
            "application/xml" | "text/xml" => Some(ContentType::Xml),
            "text/plain" => Some(ContentType::Text),
            _ => None,
        }
    }

    /// Convert to the appropriate MIME type string.
    ///
    /// # Examples
    /// ```
    /// use commonplace_doc::content_type::ContentType;
    ///
    /// assert_eq!(ContentType::Json.to_mime(), "application/json");
    /// assert_eq!(ContentType::JsonArray.to_mime(), "application/json;root=array");
    /// ```
    pub fn to_mime(&self) -> &'static str {
        match self {
            ContentType::Json => "application/json",
            ContentType::JsonArray => "application/json;root=array",
            ContentType::Xml => "application/xml",
            ContentType::Text => "text/plain",
        }
    }

    /// Get the default content for this type.
    ///
    /// - Json: `{}`
    /// - JsonArray: `[]`
    /// - Xml: `<?xml version="1.0" encoding="UTF-8"?><root/>`
    /// - Text: empty string
    pub fn default_content(&self) -> String {
        match self {
            ContentType::Json => "{}".to_string(),
            ContentType::JsonArray => "[]".to_string(),
            ContentType::Xml => r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#.to_string(),
            ContentType::Text => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_mime_json() {
        assert_eq!(
            ContentType::from_mime("application/json"),
            Some(ContentType::Json)
        );
    }

    #[test]
    fn test_from_mime_json_array() {
        assert_eq!(
            ContentType::from_mime("application/json;root=array"),
            Some(ContentType::JsonArray)
        );
        assert_eq!(
            ContentType::from_mime("application/json; root=array"),
            Some(ContentType::JsonArray)
        );
        assert_eq!(
            ContentType::from_mime("application/json;ROOT=ARRAY"),
            Some(ContentType::JsonArray)
        );
    }

    #[test]
    fn test_from_mime_xml() {
        assert_eq!(
            ContentType::from_mime("application/xml"),
            Some(ContentType::Xml)
        );
        assert_eq!(ContentType::from_mime("text/xml"), Some(ContentType::Xml));
    }

    #[test]
    fn test_from_mime_text() {
        assert_eq!(
            ContentType::from_mime("text/plain"),
            Some(ContentType::Text)
        );
    }

    #[test]
    fn test_from_mime_unknown() {
        assert_eq!(ContentType::from_mime("image/png"), None);
        assert_eq!(ContentType::from_mime("application/octet-stream"), None);
    }

    #[test]
    fn test_to_mime() {
        assert_eq!(ContentType::Json.to_mime(), "application/json");
        assert_eq!(
            ContentType::JsonArray.to_mime(),
            "application/json;root=array"
        );
        assert_eq!(ContentType::Xml.to_mime(), "application/xml");
        assert_eq!(ContentType::Text.to_mime(), "text/plain");
    }

    #[test]
    fn test_default_content() {
        assert_eq!(ContentType::Json.default_content(), "{}");
        assert_eq!(ContentType::JsonArray.default_content(), "[]");
        assert_eq!(ContentType::Text.default_content(), "");
        assert!(ContentType::Xml.default_content().contains("<?xml"));
    }

    #[test]
    fn test_roundtrip() {
        for ct in [
            ContentType::Json,
            ContentType::JsonArray,
            ContentType::Xml,
            ContentType::Text,
        ] {
            let mime = ct.to_mime();
            assert_eq!(ContentType::from_mime(mime), Some(ct));
        }
    }
}
