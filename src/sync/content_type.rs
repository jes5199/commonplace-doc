//! Content type detection for files.

use std::path::Path;

/// Known text file extensions and their MIME types.
const TEXT_EXTENSIONS: &[(&str, &str)] = &[
    // Plain text
    ("txt", "text/plain"),
    ("text", "text/plain"),
    ("log", "text/plain"),
    // Markdown
    ("md", "text/markdown"),
    ("markdown", "text/markdown"),
    // Code
    ("rs", "text/x-rust"),
    ("py", "text/x-python"),
    ("js", "text/javascript"),
    ("ts", "text/typescript"),
    ("jsx", "text/javascript"),
    ("tsx", "text/typescript"),
    ("go", "text/x-go"),
    ("rb", "text/x-ruby"),
    ("java", "text/x-java"),
    ("c", "text/x-c"),
    ("cpp", "text/x-c++"),
    ("h", "text/x-c"),
    ("hpp", "text/x-c++"),
    ("cs", "text/x-csharp"),
    ("swift", "text/x-swift"),
    ("kt", "text/x-kotlin"),
    ("scala", "text/x-scala"),
    ("sh", "text/x-shellscript"),
    ("bash", "text/x-shellscript"),
    ("zsh", "text/x-shellscript"),
    ("fish", "text/x-shellscript"),
    ("ps1", "text/x-powershell"),
    // Web
    ("html", "text/html"),
    ("htm", "text/html"),
    ("css", "text/css"),
    ("scss", "text/x-scss"),
    ("sass", "text/x-sass"),
    ("less", "text/x-less"),
    // Data
    ("json", "application/json"),
    ("jsonl", "application/x-ndjson"),
    ("ndjson", "application/x-ndjson"),
    ("xml", "application/xml"),
    ("xhtml", "application/xhtml+xml"),
    ("yaml", "text/yaml"),
    ("yml", "text/yaml"),
    ("toml", "text/x-toml"),
    ("ini", "text/x-ini"),
    ("csv", "text/csv"),
    ("tsv", "text/tab-separated-values"),
    // Config
    ("conf", "text/plain"),
    ("cfg", "text/plain"),
    ("config", "text/plain"),
    ("env", "text/plain"),
    ("properties", "text/x-java-properties"),
    // Documentation
    ("rst", "text/x-rst"),
    ("adoc", "text/x-asciidoc"),
    ("tex", "text/x-tex"),
    ("latex", "text/x-latex"),
    // Other text
    ("sql", "text/x-sql"),
    ("graphql", "text/x-graphql"),
    ("gql", "text/x-graphql"),
    ("dockerfile", "text/x-dockerfile"),
    ("makefile", "text/x-makefile"),
    ("cmake", "text/x-cmake"),
];

/// Known binary file extensions.
const BINARY_EXTENSIONS: &[&str] = &[
    // Images
    "png", "jpg", "jpeg", "gif", "bmp", "ico", "webp", "svg", "tiff", "tif", // Audio
    "mp3", "wav", "flac", "aac", "ogg", "m4a", "wma", // Video
    "mp4", "avi", "mkv", "mov", "wmv", "flv", "webm", // Archives
    "zip", "tar", "gz", "bz2", "xz", "7z", "rar", // Documents
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp",
    // Executables
    "exe", "dll", "so", "dylib", "bin", "app", // Other binary
    "wasm", "class", "pyc", "pyo", "o", "a", "lib", // Fonts
    "ttf", "otf", "woff", "woff2", "eot", // Database
    "db", "sqlite", "sqlite3",
];

/// Allowed file extensions for sync operations.
/// Only files with these extensions will be synced.
pub const ALLOWED_EXTENSIONS: &[&str] = &[
    "json", "jsonl", "ndjson", "txt", "xml", "xhtml", "bin", "md",
];

/// Check if a file path has an allowed extension for syncing.
/// Returns true if the file should be synced, false otherwise.
pub fn is_allowed_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ALLOWED_EXTENSIONS.contains(&ext.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Result of content type detection.
#[derive(Debug, Clone, PartialEq)]
pub struct ContentTypeInfo {
    /// MIME type string
    pub mime_type: String,
    /// Whether the file is binary (needs base64 encoding)
    pub is_binary: bool,
}

impl ContentTypeInfo {
    /// Create a text content type.
    pub fn text(mime_type: &str) -> Self {
        Self {
            mime_type: mime_type.to_string(),
            is_binary: false,
        }
    }

    /// Create a binary content type.
    pub fn binary(mime_type: &str) -> Self {
        Self {
            mime_type: mime_type.to_string(),
            is_binary: true,
        }
    }
}

/// Detect content type from file path (extension-based).
pub fn detect_from_path(path: &Path) -> ContentTypeInfo {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_lowercase());

    // Also check filename for extensionless files like "Makefile", "Dockerfile"
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_lowercase());

    // Check for known text extensions
    if let Some(ref ext) = extension {
        for (text_ext, mime) in TEXT_EXTENSIONS {
            if ext == *text_ext {
                return ContentTypeInfo::text(mime);
            }
        }
    }

    // Check for extensionless text files
    if let Some(ref name) = filename {
        match name.as_str() {
            "makefile" | "gnumakefile" => return ContentTypeInfo::text("text/x-makefile"),
            "dockerfile" => return ContentTypeInfo::text("text/x-dockerfile"),
            "rakefile" => return ContentTypeInfo::text("text/x-ruby"),
            "gemfile" => return ContentTypeInfo::text("text/x-ruby"),
            "cmakelists.txt" => return ContentTypeInfo::text("text/x-cmake"),
            ".gitignore" | ".gitattributes" | ".gitmodules" => {
                return ContentTypeInfo::text("text/plain")
            }
            ".env" | ".envrc" => return ContentTypeInfo::text("text/plain"),
            _ => {}
        }
    }

    // Check for known binary extensions
    if let Some(ref ext) = extension {
        if BINARY_EXTENSIONS.contains(&ext.as_str()) {
            // Try to get a more specific MIME type for common formats
            let mime = match ext.as_str() {
                "png" => "image/png",
                "jpg" | "jpeg" => "image/jpeg",
                "gif" => "image/gif",
                "webp" => "image/webp",
                "svg" => "image/svg+xml",
                "pdf" => "application/pdf",
                "zip" => "application/zip",
                "json" => "application/json",
                _ => "application/octet-stream",
            };
            return ContentTypeInfo::binary(mime);
        }
    }

    // Default: assume text/plain for unknown extensions
    // This is safer than assuming binary - worst case, text with weird chars
    ContentTypeInfo::text("text/plain")
}

/// Check if a string looks like it might be base64-encoded binary data.
/// Returns true only for strings that are likely base64-encoded binary,
/// not for short text that happens to be valid base64 (like "note" or "hello").
pub fn looks_like_base64_binary(content: &str) -> bool {
    // Minimum length: base64 of 12+ bytes = 16+ chars
    // This avoids treating short words like "note", "hello", "test" as base64
    if content.len() < 16 {
        return false;
    }

    // Must contain only valid base64 characters
    let valid_base64_chars = content
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=');
    if !valid_base64_chars {
        return false;
    }

    // Padding must only appear at end
    if let Some(eq_pos) = content.find('=') {
        let after_eq = &content[eq_pos..];
        if !after_eq.chars().all(|c| c == '=') {
            return false;
        }
        // At most 2 padding chars
        if after_eq.len() > 2 {
            return false;
        }
    }

    // Length must be multiple of 4 for valid base64
    content.len().is_multiple_of(4)
}

/// Check if file content appears to be binary by sampling first bytes.
/// Returns true if binary content detected.
pub fn is_binary_content(content: &[u8]) -> bool {
    // Check first 8KB for null bytes or high ratio of non-text bytes
    let sample_size = content.len().min(8192);
    let sample = &content[..sample_size];

    // Null byte is a strong indicator of binary
    if sample.contains(&0) {
        return true;
    }

    // Count non-printable, non-whitespace bytes
    let non_text_count = sample
        .iter()
        .filter(|&&b| {
            // Printable ASCII + common whitespace
            !((0x20..=0x7E).contains(&b) || b == b'\n' || b == b'\r' || b == b'\t')
        })
        .count();

    // If more than 30% non-text, probably binary
    non_text_count > sample_size / 3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_extensions() {
        assert!(!detect_from_path(Path::new("foo.txt")).is_binary);
        assert!(!detect_from_path(Path::new("foo.rs")).is_binary);
        assert!(!detect_from_path(Path::new("foo.json")).is_binary);
        assert!(!detect_from_path(Path::new("foo.md")).is_binary);
    }

    #[test]
    fn test_binary_extensions() {
        assert!(detect_from_path(Path::new("foo.png")).is_binary);
        assert!(detect_from_path(Path::new("foo.jpg")).is_binary);
        assert!(detect_from_path(Path::new("foo.pdf")).is_binary);
        assert!(detect_from_path(Path::new("foo.zip")).is_binary);
    }

    #[test]
    fn test_special_filenames() {
        assert!(!detect_from_path(Path::new("Makefile")).is_binary);
        assert!(!detect_from_path(Path::new("Dockerfile")).is_binary);
        assert!(!detect_from_path(Path::new(".gitignore")).is_binary);
    }

    #[test]
    fn test_binary_content_detection() {
        assert!(!is_binary_content(b"Hello, world!"));
        assert!(!is_binary_content(b"Line 1\nLine 2\nLine 3\n"));
        assert!(is_binary_content(b"\x00\x01\x02\x03"));
        assert!(is_binary_content(b"PNG\x00\x00\x00"));
    }

    #[test]
    fn test_mime_types() {
        assert_eq!(
            detect_from_path(Path::new("foo.json")).mime_type,
            "application/json"
        );
        assert_eq!(
            detect_from_path(Path::new("foo.rs")).mime_type,
            "text/x-rust"
        );
        assert_eq!(
            detect_from_path(Path::new("foo.png")).mime_type,
            "image/png"
        );
    }

    #[test]
    fn test_allowed_extensions() {
        // Allowed extensions
        assert!(is_allowed_extension(Path::new("foo.json")));
        assert!(is_allowed_extension(Path::new("foo.txt")));
        assert!(is_allowed_extension(Path::new("foo.xml")));
        assert!(is_allowed_extension(Path::new("foo.xhtml")));
        assert!(is_allowed_extension(Path::new("foo.bin")));
        assert!(is_allowed_extension(Path::new("foo.md")));

        // Case insensitive
        assert!(is_allowed_extension(Path::new("foo.JSON")));
        assert!(is_allowed_extension(Path::new("foo.TXT")));
        assert!(is_allowed_extension(Path::new("foo.Xml")));
        assert!(is_allowed_extension(Path::new("foo.MD")));

        // Disallowed extensions
        assert!(!is_allowed_extension(Path::new("foo.rs")));
        assert!(!is_allowed_extension(Path::new("foo.py")));
        assert!(!is_allowed_extension(Path::new("foo.png")));
        assert!(!is_allowed_extension(Path::new("foo.html")));

        // No extension
        assert!(!is_allowed_extension(Path::new("Makefile")));
        assert!(!is_allowed_extension(Path::new("no_extension")));
    }

    #[test]
    fn test_xhtml_mime_type() {
        assert_eq!(
            detect_from_path(Path::new("foo.xhtml")).mime_type,
            "application/xhtml+xml"
        );
        assert!(!detect_from_path(Path::new("foo.xhtml")).is_binary);
    }

    #[test]
    fn test_looks_like_base64_binary() {
        // Short strings that happen to be valid base64 should NOT be treated as binary
        assert!(!looks_like_base64_binary("note")); // 4 chars - too short
        assert!(!looks_like_base64_binary("hello")); // 5 chars - too short
        assert!(!looks_like_base64_binary("test")); // 4 chars - too short
        assert!(!looks_like_base64_binary("aGVsbG8=")); // 8 chars - too short

        // Strings with non-base64 characters should NOT be treated as binary
        assert!(!looks_like_base64_binary("Hello, world!")); // contains space and !
        assert!(!looks_like_base64_binary("This is a test.")); // contains spaces and .

        // Longer strings that look like base64 should be treated as binary
        assert!(looks_like_base64_binary("aGVsbG8gd29ybGQh")); // 16 chars, valid base64
        assert!(looks_like_base64_binary("YWJjZGVmZ2hpamtsbW5vcA==")); // 24 chars with padding

        // Invalid base64 (wrong length) should NOT be treated as binary
        assert!(!looks_like_base64_binary("aGVsbG8gd29ybGQ")); // 15 chars - not multiple of 4

        // Invalid padding should NOT be treated as binary
        assert!(!looks_like_base64_binary("aGVs=bG8gd29ybGQ=")); // = in middle
    }
}
