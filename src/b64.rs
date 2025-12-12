#[derive(Debug)]
pub struct DecodeError(pub &'static str);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for DecodeError {}

const ENCODE_TABLE: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn decode_val(b: u8) -> Option<u8> {
    match b {
        b'A'..=b'Z' => Some(b - b'A'),
        b'a'..=b'z' => Some(b - b'a' + 26),
        b'0'..=b'9' => Some(b - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

pub fn encode(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    let mut out = String::with_capacity(((bytes.len() + 2) / 3) * 4);
    let mut i = 0;

    while i + 3 <= bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        let b2 = bytes[i + 2];

        out.push(ENCODE_TABLE[(b0 >> 2) as usize] as char);
        out.push(ENCODE_TABLE[(((b0 & 0b11) << 4) | (b1 >> 4)) as usize] as char);
        out.push(ENCODE_TABLE[(((b1 & 0b1111) << 2) | (b2 >> 6)) as usize] as char);
        out.push(ENCODE_TABLE[(b2 & 0b111111) as usize] as char);

        i += 3;
    }

    let rem = bytes.len() - i;
    if rem == 1 {
        let b0 = bytes[i];
        out.push(ENCODE_TABLE[(b0 >> 2) as usize] as char);
        out.push(ENCODE_TABLE[((b0 & 0b11) << 4) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        out.push(ENCODE_TABLE[(b0 >> 2) as usize] as char);
        out.push(ENCODE_TABLE[(((b0 & 0b11) << 4) | (b1 >> 4)) as usize] as char);
        out.push(ENCODE_TABLE[((b1 & 0b1111) << 2) as usize] as char);
        out.push('=');
    }

    out
}

pub fn decode(input: &str) -> Result<Vec<u8>, DecodeError> {
    let mut cleaned: Vec<u8> = input
        .as_bytes()
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();

    if cleaned.is_empty() {
        return Ok(Vec::new());
    }

    let rem = cleaned.len() % 4;
    match rem {
        0 => {}
        2 => cleaned.extend_from_slice(b"=="),
        3 => cleaned.push(b'='),
        _ => return Err(DecodeError("invalid base64 length")),
    }

    let mut out = Vec::with_capacity((cleaned.len() / 4) * 3);

    let mut i = 0;
    while i < cleaned.len() {
        let c0 = cleaned[i];
        let c1 = cleaned[i + 1];
        let c2 = cleaned[i + 2];
        let c3 = cleaned[i + 3];

        let v0 = decode_val(c0).ok_or(DecodeError("invalid base64 character"))?;
        let v1 = decode_val(c1).ok_or(DecodeError("invalid base64 character"))?;

        if c2 == b'=' && c3 == b'=' {
            out.push((v0 << 2) | (v1 >> 4));
            if i + 4 != cleaned.len() {
                return Err(DecodeError("invalid base64 padding"));
            }
            break;
        }

        let v2 = decode_val(c2).ok_or(DecodeError("invalid base64 character"))?;

        if c3 == b'=' {
            out.push((v0 << 2) | (v1 >> 4));
            out.push((v1 << 4) | (v2 >> 2));
            if i + 4 != cleaned.len() {
                return Err(DecodeError("invalid base64 padding"));
            }
            break;
        }

        let v3 = decode_val(c3).ok_or(DecodeError("invalid base64 character"))?;

        out.push((v0 << 2) | (v1 >> 4));
        out.push((v1 << 4) | (v2 >> 2));
        out.push((v2 << 6) | v3);

        i += 4;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let bytes = b"\x00\x01\x02hello world\xff";
        let enc = encode(bytes);
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, bytes);
    }

    #[test]
    fn decode_unpadded() {
        let bytes = b"hello";
        let enc = encode(bytes);
        let unpadded = enc.trim_end_matches('=');
        let dec = decode(unpadded).unwrap();
        assert_eq!(dec, bytes);
    }
}

