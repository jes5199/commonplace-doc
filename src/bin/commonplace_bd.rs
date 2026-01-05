//! commonplace-bd - Commonplace Bug Database CLI
//!
//! Alias for `cbd`. See `commonplace_doc::cbd` for implementation.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    commonplace_doc::cbd::main()
}
