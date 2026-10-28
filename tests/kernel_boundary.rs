//! Kernel boundary guard (architecture test, v2).
//!
//! This crate is a **kernel**, not a business framework. The kernel owns the
//! generic plugin runtime (context/service/fiber/effects/events/registry/
//! reflect/config/exec/waterfall/l2 isolation/logger facility) and must stay
//! usable by *any* host. Business logic, I/O, transports, persistence and
//! domain policy belong to plugins/embedders.
//!
//! The guard is deliberately adversarial: it normalizes paths (so
//! `std :: fs` cannot hide), expands grouped `use` statements, rejects
//! environment/file-inclusion macros, parses dependency tables (including
//! `package = "..."` renames and `[target.*.dependencies]`), only scans
//! non-test sources recursively, and strips comments with a small lexer so a
//! comment cannot fake `#![forbid(unsafe_code)]`.
//!
//! See `docs/KERNEL-BOUNDARY.md` for the human-readable contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Runtime dependencies that a Cordis-style kernel may use (real package names,
/// after resolving `package = "..."` renames).
const ALLOWED_RUNTIME_DEPS: &[&str] = &["serde_json", "tracing", "futures-util"];

/// Crates that would drag I/O / runtimes / business concerns into the kernel.
const FORBIDDEN_DEPS: &[&str] = &[
    "tokio",
    "async-std",
    "smol",
    "futures-executor",
    "reqwest",
    "hyper",
    "axum",
    "actix-web",
    "sqlx",
    "diesel",
    "redis",
    "mongodb",
    "rusqlite",
    "rand",
    "chrono",
    "uuid",
    "serde_yaml",
    "toml",
    "tempfile",
    "walkdir",
    "libc",
    "nix",
    "winapi",
    "windows-sys",
    "clap",
];

/// Std APIs / macros that are not kernel concerns. Checked after whitespace
/// normalization and grouped-`use` expansion, so `use std::{fs, net};` and
/// `std :: fs` are caught as well.
const FORBIDDEN_SRC_TOKENS: &[&str] = &[
    "std::fs",
    "std::net",
    "std::process",
    "std::env",
    "std::os",
    "std::path",
    "std::io",
    "TcpStream",
    "TcpListener",
    "UdpSocket",
    "UnixStream",
    "File::open",
    "OpenOptions",
    "Command::new",
    "thread::spawn",
    "thread::Builder",
    "tokio::",
    "async_std::",
    "smol::",
    "reqwest::",
    "hyper::",
    "sqlx::",
    "env!",
    "option_env!",
    "include!",
    "include_str!",
    "include_bytes!",
];

/// Business / domain vocabulary that must not appear in kernel sources.
/// Kernel protocol words (`logger`, `internal/*`, `ctx.*`, `plugin`, `fiber`,
/// `embedder`, `user code`, ordering of effects, ...) are deliberately *not*
/// on this list.
const FORBIDDEN_DOMAIN_WORDS: &[&str] = &[
    "payment",
    "invoice",
    "tenant",
    "customer",
    "checkout",
    "warehouse",
    "database",
    "redis",
    "kafka",
    "rabbitmq",
    "http",
    "server",
    "client",
];

/// Business words that must not appear in *public documentation* either.
/// Generic deployment words (`server`, `client`, `http`) are allowed in docs
/// (the kernel documents its embedder boundary), but concrete domain nouns are
/// not: rustdoc is part of the kernel's public contract.
const FORBIDDEN_DOC_WORDS: &[&str] = &[
    "payment",
    "invoice",
    "tenant",
    "customer",
    "checkout",
    "warehouse",
    "database",
    "redis",
    "kafka",
    "rabbitmq",
];

/// Files whose names advertise a business concern.
const FORBIDDEN_FILE_STEMS: &[&str] = &[
    "payment",
    "invoice",
    "tenant",
    "customer",
    "checkout",
    "warehouse",
    "database",
    "http",
    "server",
    "client",
];

/// Public API growth is intentional, not accidental: a tripwire for
/// convenience/business APIs leaking into the kernel. Bump deliberately (with a
/// CHANGELOG entry) when adding kernel features.
const PUBLIC_ITEM_CAP: usize = 420;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

// ---------------------------------------------------------------------------
// Source scanning helpers
// ---------------------------------------------------------------------------

/// Strip comments with a minimal lexer, keeping string/char literals intact.
///
/// A naive `line.find("//")` would truncate `"https://..."` strings and let a
/// kernel fake `#![forbid(unsafe_code)]` inside a comment.
fn strip_comments(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'"' => {
                out.push('"');
                index += 1;
                while index < bytes.len() {
                    let byte = bytes[index];
                    out.push(byte as char);
                    index += 1;
                    if byte == b'\\' && index < bytes.len() {
                        out.push(bytes[index] as char);
                        index += 1;
                    } else if byte == b'"' {
                        break;
                    }
                }
            }
            b'\'' => {
                out.push('\'');
                index += 1;
                let mut copied = 0usize;
                while index < bytes.len() && copied < 4 {
                    let byte = bytes[index];
                    out.push(byte as char);
                    index += 1;
                    copied += 1;
                    if byte == b'\\' && index < bytes.len() {
                        out.push(bytes[index] as char);
                        index += 1;
                        copied += 1;
                    } else if byte == b'\'' {
                        break;
                    }
                }
            }
            b'r' if bytes.get(index + 1) == Some(&b'"') || bytes.get(index + 1) == Some(&b'#') => {
                out.push('r');
                index += 1;
                let mut hashes = 0usize;
                while bytes.get(index) == Some(&b'#') {
                    hashes += 1;
                    out.push('#');
                    index += 1;
                }
                if bytes.get(index) == Some(&b'"') {
                    out.push('"');
                    index += 1;
                    while index < bytes.len() {
                        if bytes[index] == b'"' {
                            let mut matched = 0usize;
                            let mut probe = index + 1;
                            while matched < hashes && bytes.get(probe) == Some(&b'#') {
                                matched += 1;
                                probe += 1;
                            }
                            if matched == hashes {
                                for _ in 0..=hashes {
                                    out.push('#');
                                }
                                index = index + 1 + hashes;
                                break;
                            }
                        }
                        out.push(bytes[index] as char);
                        index += 1;
                    }
                }
            }
            byte => {
                out.push(byte as char);
                index += 1;
            }
        }
    }
    out
}

/// Source text that must obey the kernel boundary: comments and trailing
/// `#[cfg(test)]` unit-test modules are stripped (tests may legitimately
/// mention embedder concerns). String literals are kept, because diagnostic
/// text must not smuggle business vocabulary either.
fn kernel_code(source: &str) -> String {
    let without_comments = strip_comments(source);
    // The scanner strips the *trailing* unit-test module. To keep that sound,
    // each source file may contain at most one `#[cfg(test)]` module and it must
    // be the final item; otherwise test code would be scanned as production code
    // (false positives) or production code could hide after a test module.
    let cfg_tests = without_comments.matches("#[cfg(test)]").count();
    assert!(
        cfg_tests <= 1,
        "kernel source may contain at most one trailing `#[cfg(test)]` module"
    );
    match without_comments.rfind("\n#[cfg(test)]") {
        Some(index) => without_comments[..index].to_string(),
        None => without_comments,
    }
}

/// Expand a grouped `use` statement into one path per imported item, so
/// `use std::{fs, net};` becomes `std::fs` / `std::net`.
fn expand_use_statement(statement: &str) -> Vec<String> {
    fn split_top_level(body: &str) -> Vec<&str> {
        let mut items = Vec::new();
        let mut depth = 0usize;
        let mut start = 0usize;
        for (index, ch) in body.char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    items.push(&body[start..index]);
                    start = index + 1;
                }
                _ => {}
            }
        }
        items.push(&body[start..]);
        items
    }

    fn walk(prefix: &str, body: &str, out: &mut Vec<String>) {
        for raw in split_top_level(body) {
            let item = raw.trim();
            if item.is_empty() || item == "self" {
                if !prefix.is_empty() {
                    out.push(prefix.to_string());
                }
                continue;
            }
            if let Some(open) = item.find('{') {
                let mut depth = 0usize;
                let mut close = None;
                for (index, ch) in item.char_indices().skip(open) {
                    match ch {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                close = Some(index);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(close) = close {
                    let inner_prefix = item[..open].trim().trim_end_matches("::");
                    let next_prefix = match (prefix.is_empty(), inner_prefix.is_empty()) {
                        (true, true) => String::new(),
                        (true, false) => inner_prefix.to_string(),
                        (false, true) => prefix.to_string(),
                        (false, false) => format!("{prefix}::{inner_prefix}"),
                    };
                    walk(&next_prefix, &item[open + 1..close], out);
                    continue;
                }
            }
            let path = if prefix.is_empty() {
                item.to_string()
            } else {
                format!("{prefix}::{item}")
            };
            let path = path.split(" as ").next().unwrap_or(&path).to_string();
            out.push(path);
        }
    }

    let mut out = Vec::new();
    let body = statement
        .trim_start_matches("pub use ")
        .trim_start_matches("use ")
        .trim_end_matches(';')
        .trim();
    walk("", body, &mut out);
    out
}

/// Collect every `use` statement (including `pub use`) and expand it.
fn expanded_use_paths(code: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut rest = code;
    while let Some(start) = rest.find("use ") {
        let after = &rest[start..];
        let Some(end) = after.find(';') else {
            break;
        };
        let statement = after[..end + 1].trim().trim_start_matches("pub ");
        if statement.starts_with("use ") {
            paths.extend(expand_use_statement(statement));
        }
        rest = &after[end + 1..];
    }
    paths
}

fn normalize(code: &str) -> String {
    code.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Split text into lower-cased identifier words, handling `snake_case`,
/// `CamelCase`/`PascalCase` and punctuation/string boundaries. Business APIs
/// most often appear as compound identifiers (`payment_gateway`,
/// `PaymentService`), which a plain substring check would miss.
fn identifier_words(text: &str) -> Vec<String> {
    fn split_compound(run: &str, out: &mut Vec<String>) {
        for snake_part in run.split('_') {
            if snake_part.is_empty() {
                continue;
            }
            let chars: Vec<char> = snake_part.chars().collect();
            let mut start = 0usize;
            for index in 1..chars.len() {
                let previous = chars[index - 1];
                let current = chars[index];
                let boundary = (previous.is_lowercase() && current.is_uppercase())
                    || (previous.is_uppercase()
                        && current.is_uppercase()
                        && chars.get(index + 1).is_some_and(|next| next.is_lowercase()));
                if boundary {
                    out.push(
                        chars[start..index]
                            .iter()
                            .collect::<String>()
                            .to_lowercase(),
                    );
                    start = index;
                }
            }
            out.push(chars[start..].iter().collect::<String>().to_lowercase());
        }
    }

    let mut words = Vec::new();
    let mut run = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            run.push(ch);
        } else if !run.is_empty() {
            split_compound(&run, &mut words);
            run.clear();
        }
    }
    if !run.is_empty() {
        split_compound(&run, &mut words);
    }
    words
}

/// Collect public documentation text (`///`, `//!`, `/** ... */`, `/*! ... */`
/// and `#[doc = "..."]`) so the boundary guard also covers rustdoc.
fn doc_comment_text(source: &str) -> String {
    let mut docs = String::new();
    let mut in_block_doc = false;
    for line in source.lines() {
        let trimmed = line.trim_start();
        if in_block_doc {
            docs.push(' ');
            docs.push_str(trimmed);
            if trimmed.contains("*/") {
                in_block_doc = false;
            }
            continue;
        }
        if trimmed.starts_with("/**") || trimmed.starts_with("/*!") {
            docs.push(' ');
            docs.push_str(trimmed);
            in_block_doc = !trimmed.contains("*/");
            continue;
        }
        if trimmed.starts_with("///") || trimmed.starts_with("//!") {
            docs.push(' ');
            docs.push_str(trimmed.trim_start_matches('/').trim_start_matches('!'));
            continue;
        }
        if trimmed.starts_with("#[doc") {
            if let Some(start) = trimmed.find('"') {
                if let Some(end) = trimmed[start + 1..].find('"') {
                    docs.push(' ');
                    docs.push_str(&trimmed[start + 1..start + 1 + end]);
                }
            }
        }
    }
    docs
}

fn src_files(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let entries = fs::read_dir(dir).unwrap_or_else(|_| panic!("read dir {}", dir.display()));
        for entry in entries {
            let path = entry.expect("src entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&root.join("src"), &mut files);
    files.sort();
    assert!(!files.is_empty(), "src/ must contain Rust sources");
    files
}

// ---------------------------------------------------------------------------
// Cargo.toml parsing
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ManifestFacts {
    /// Real crate names of normal + target-specific + build dependencies.
    runtime_deps: BTreeSet<String>,
    /// `[dev-dependencies]` real crate names (tests only, not shipped).
    dev_deps: BTreeSet<String>,
    features: BTreeMap<String, Vec<String>>,
    lib_path_override: Option<String>,
    has_package_build_field: bool,
}

fn manifest_facts(manifest: &str) -> ManifestFacts {
    #[derive(PartialEq, Clone, Copy)]
    enum Kind {
        Normal,
        Build,
        Dev,
        Features,
        Other,
    }
    let mut facts = ManifestFacts::default();
    let mut kind = Kind::Other;
    let mut table_dep: Option<String> = None;
    let mut current_section = String::new();
    let mut table_package: BTreeMap<String, String> = BTreeMap::new();

    for raw_line in manifest.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') {
            let section = line.trim_matches(|c| c == '[' || c == ']').trim();
            let lower = section.to_lowercase();
            current_section = section.to_string();

            let header_dep = if let Some(rest) = lower.strip_prefix("dependencies.") {
                Some((rest, Kind::Normal))
            } else if let Some(rest) = lower.strip_prefix("build-dependencies.") {
                Some((rest, Kind::Build))
            } else if let Some(rest) = lower.strip_prefix("dev-dependencies.") {
                Some((rest, Kind::Dev))
            } else if let Some((_, rest)) = lower.rsplit_once(".dependencies.") {
                Some((rest, Kind::Normal))
            } else if let Some((_, rest)) = lower.rsplit_once(".build-dependencies.") {
                Some((rest, Kind::Build))
            } else if let Some((_, rest)) = lower.rsplit_once(".dev-dependencies.") {
                Some((rest, Kind::Dev))
            } else {
                None
            };

            if let Some((name, header_kind)) = header_dep {
                kind = header_kind;
                let name = name.trim().trim_matches('\'').trim_matches('"');
                table_dep = Some(name.to_string());
                continue;
            }
            table_dep = None;
            kind = if lower == "dependencies" || lower.ends_with(".dependencies") {
                Kind::Normal
            } else if lower == "build-dependencies" || lower.ends_with(".build-dependencies") {
                Kind::Build
            } else if lower == "dev-dependencies" || lower.ends_with(".dev-dependencies") {
                Kind::Dev
            } else if lower == "features" {
                Kind::Features
            } else {
                Kind::Other
            };
            if lower == "lib" {
                facts.lib_path_override = Some(String::new());
            }
            continue;
        }

        match kind {
            Kind::Features => {
                let Some((name, value)) = line.split_once('=') else {
                    continue;
                };
                let name = name.trim().to_string();
                if name.is_empty() {
                    continue;
                }
                let values = facts.features.entry(name).or_default();
                values.push(value.trim().to_string());
            }
            Kind::Normal | Kind::Build | Kind::Dev => {
                let Some((raw_key, raw_value)) = line.split_once('=') else {
                    continue;
                };
                let key = raw_key.trim().trim_matches('"').to_string();
                if key.is_empty() {
                    continue;
                }
                let mut real = table_dep.clone().unwrap_or_else(|| key.clone());
                if table_dep.is_some() && key == "package" {
                    let package = raw_value
                        .trim()
                        .trim_matches(|c| c == '"' || c == '\'')
                        .to_string();
                    if !package.is_empty() {
                        table_package.insert(current_section.clone(), package.clone());
                        real = package;
                    }
                }
                if let Some(package_pos) = raw_value.find("package") {
                    let tail = &raw_value[package_pos..];
                    if let Some(quote) = tail.find(['"', '\'']) {
                        let quote_char = tail.as_bytes()[quote] as char;
                        if let Some(end_rel) = tail[quote + 1..].find(quote_char) {
                            real = tail[quote + 1..quote + 1 + end_rel].to_string();
                        }
                    }
                }
                match kind {
                    Kind::Normal => {
                        facts.runtime_deps.insert(real);
                    }
                    Kind::Build => {
                        facts.runtime_deps.insert(real);
                    }
                    Kind::Dev => {
                        facts.dev_deps.insert(real);
                    }
                    _ => {}
                }
            }
            Kind::Other => {
                if let Some((key, value)) = line.split_once('=') {
                    let key = key.trim();
                    let value = value.trim().trim_matches('"');
                    if key == "path" && facts.lib_path_override.as_deref() == Some("") {
                        facts.lib_path_override = Some(value.to_string());
                    }
                    if key == "build" {
                        facts.has_package_build_field = true;
                    }
                }
            }
        }
    }

    // `[dependencies.foo]` has no `name =` line: record the header's key even if
    // the table body only contains `package = "..."`.
    for raw_line in manifest.lines() {
        let line = raw_line.trim();
        if !line.starts_with('[') {
            continue;
        }
        let section = line.trim_matches(|c| c == '[' || c == ']').trim();
        let lower = section.to_lowercase();
        let header = if let Some(rest) = lower.strip_prefix("dependencies.") {
            Some((rest, Kind::Normal))
        } else if let Some(rest) = lower.strip_prefix("build-dependencies.") {
            Some((rest, Kind::Build))
        } else if let Some(rest) = lower.strip_prefix("dev-dependencies.") {
            Some((rest, Kind::Dev))
        } else if let Some((_, rest)) = lower.rsplit_once(".dependencies.") {
            Some((rest, Kind::Normal))
        } else if let Some((_, rest)) = lower.rsplit_once(".build-dependencies.") {
            Some((rest, Kind::Build))
        } else if let Some((_, rest)) = lower.rsplit_once(".dev-dependencies.") {
            Some((rest, Kind::Dev))
        } else {
            None
        };
        if let Some((name, header_kind)) = header {
            let name = table_package
                .get(section)
                .cloned()
                .unwrap_or_else(|| name.trim().trim_matches('\'').trim_matches('"').to_string());
            if name.is_empty() {
                continue;
            }
            match header_kind {
                Kind::Normal | Kind::Build => {
                    facts.runtime_deps.insert(name);
                }
                Kind::Dev => {
                    facts.dev_deps.insert(name);
                }
                _ => {}
            }
        }
    }
    facts
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Self-tests: the guard must actually catch the bypasses it claims to catch.
// ---------------------------------------------------------------------------

#[test]
fn guard_parses_table_dependencies_and_package_renames() {
    let manifest = r#"
[package]
name = "kernel"
version = "0.1.0"

[dependencies]
serde_json = "1"

[target.'cfg(unix)'.dependencies.tokio]
version = "1"

[dev-dependencies]
tokio = { version = "1" }
"#;
    let facts = manifest_facts(manifest);
    assert!(
        facts.runtime_deps.contains("tokio"),
        "table syntax + target-specific dependencies must be seen: {:?}",
        facts.runtime_deps
    );

    let renamed = r#"
[dependencies.serde_json]
package = "tokio"
version = "1"
"#;
    let facts = manifest_facts(renamed);
    assert!(
        facts.runtime_deps.contains("tokio"),
        "package = \"tokio\" renames must be resolved to the real crate name: {:?}",
        facts.runtime_deps
    );
}

#[test]
fn guard_sees_flat_target_sections_and_single_quoted_renames() {
    let manifest = r#"
[target.'cfg(unix)'.dependencies]
libc = "0.2"

[target.'cfg(unix)'.dev-dependencies]
tokio = { version = "1" }
"#;
    let facts = manifest_facts(manifest);
    assert!(
        facts.runtime_deps.contains("libc"),
        "flat [target.*.dependencies] sections must be parsed: {:?}",
        facts.runtime_deps
    );

    let renamed = r#"
[dependencies]
serde_json = { package = 'tokio', version = "1" }
"#;
    let facts = manifest_facts(renamed);
    assert!(
        facts.runtime_deps.contains("tokio"),
        "single-quoted package renames must be resolved: {:?}",
        facts.runtime_deps
    );
}

#[test]
fn guard_expands_grouped_use_and_spaced_paths() {
    let grouped = expand_use_statement("use std::{fs, net, process::Command};");
    assert!(grouped.contains(&"std::fs".to_string()), "{grouped:?}");
    assert!(grouped.contains(&"std::net".to_string()), "{grouped:?}");
    assert!(
        grouped.contains(&"std::process::Command".to_string()),
        "{grouped:?}"
    );

    let normalized = normalize("use std :: process :: Command;");
    assert!(
        normalized.contains("std::process::Command"),
        "whitespace-normalized paths must be checked: {normalized}"
    );
}

#[test]
fn guard_tokenizes_snake_case_and_camel_case_identifiers() {
    let words = identifier_words("fn payment_gateway() { PaymentService::new(); }");
    assert!(words.contains(&"payment".to_string()), "{words:?}");
    assert!(words.contains(&"service".to_string()), "{words:?}");
    let words = identifier_words("pub struct PaymentGateway;");
    assert!(words.contains(&"payment".to_string()), "{words:?}");
}

#[test]
fn guard_collects_doc_comments_and_doc_attributes() {
    let source = "/// The payment service.\n//! database helpers\n#[doc = \"tenant\"]\nfn f() {}\n";
    let docs = doc_comment_text(source);
    assert!(docs.contains("payment"), "{docs:?}");
    assert!(docs.contains("database"), "{docs:?}");
    assert!(docs.contains("tenant"), "{docs:?}");
}

#[test]
fn guard_limits_sources_to_one_trailing_test_module() {
    let one = "pub fn f() {}\n#[cfg(test)]\nmod tests { #[test] fn t() {} }\n";
    assert!(kernel_code(one).contains("pub fn f()"));
    assert!(!kernel_code(one).contains("#[cfg(test)]"));

    let two = "pub fn f() {}\n#[cfg(test)]\nmod a {}\n#[cfg(test)]\nmod b {}\n";
    let result = std::panic::catch_unwind(|| kernel_code(two));
    assert!(
        result.is_err(),
        "multiple cfg(test) modules must be rejected"
    );
}

#[test]
fn guard_comment_lexer_keeps_strings_and_drops_forged_comments() {
    let forged = "// #![forbid(unsafe_code)]\nfn f() {}\n";
    assert!(
        !strip_comments(forged).contains("#![forbid(unsafe_code)]"),
        "a comment must not be able to fake the forbid(unsafe_code) attribute"
    );

    let string_with_comment_marker = r#""https://example.com/payment" + 1"#;
    let stripped = strip_comments(string_with_comment_marker);
    assert!(
        stripped.contains("payment"),
        "comment stripping must not truncate string literals: {stripped}"
    );
}

#[test]
fn runtime_dependencies_stay_generic_and_minimal() {
    let manifest = fs::read_to_string(repo_root().join("Cargo.toml")).expect("Cargo.toml");
    let facts = manifest_facts(&manifest);

    let unexpected: Vec<&String> = facts
        .runtime_deps
        .iter()
        .filter(|name| !ALLOWED_RUNTIME_DEPS.contains(&name.as_str()))
        .collect();
    assert!(
        unexpected.is_empty(),
        "kernel runtime dependencies must stay generic: unexpected {unexpected:?}; \
         allowed: {ALLOWED_RUNTIME_DEPS:?}. Business/IO crates belong to plugins."
    );
    assert!(
        !facts.runtime_deps.is_empty(),
        "expected the kernel to declare its generic runtime dependencies"
    );
    // Dev-dependencies (tokio for tests, ...) do not ship with the kernel and
    // are intentionally outside this allow-list; only runtime dependencies are.
    for forbidden in FORBIDDEN_DEPS {
        assert!(
            !facts.runtime_deps.contains(*forbidden),
            "`{forbidden}` must not be a kernel **runtime** dependency (I/O, \
             runtime or business concern); use a plugin/embedder crate instead"
        );
    }
    assert!(
        !facts.has_package_build_field,
        "a kernel build script could inject host/business assumptions; remove \
         `build = ...` from Cargo.toml (see docs/KERNEL-BOUNDARY.md)"
    );
    assert!(
        !repo_root().join("build.rs").exists(),
        "a kernel build script could inject host/business assumptions; keep the \
         kernel free of build scripts"
    );
    assert!(
        !repo_root().join("src/bin").exists(),
        "the kernel is a library crate; binaries belong to the embedder or examples"
    );
    if let Some(path) = &facts.lib_path_override {
        if !path.is_empty() {
            assert_eq!(
                path, "src/lib.rs",
                "the kernel library entry point must stay `src/lib.rs`"
            );
        }
    }

    for (feature, lines) in &facts.features {
        for line in lines {
            assert!(
                !line.contains("dep:"),
                "kernel feature `{feature}` must not enable dependencies (`{line}`)"
            );
        }
    }
    assert!(
        facts
            .features
            .keys()
            .all(|name| matches!(name.as_str(), "default" | "supervision")),
        "unexpected kernel feature: {:?} (only `default`/`supervision` are supported)",
        facts.features.keys().collect::<Vec<_>>()
    );
    for forbidden in FORBIDDEN_DEPS {
        for (feature, lines) in &facts.features {
            for line in lines {
                assert!(
                    !line.contains(forbidden),
                    "kernel feature `{feature}` references forbidden crate `{forbidden}`"
                );
            }
        }
    }
}

#[test]
fn sources_do_not_reference_io_net_process_env() {
    let root = repo_root();
    for file in src_files(&root) {
        let name = file.file_name().unwrap().to_string_lossy().to_string();
        let source = kernel_code(&fs::read_to_string(&file).expect("read src file"));
        let uses = expanded_use_paths(&source);
        let normalized = normalize(&format!("{source}\n{}\n", uses.join("\n")));
        for token in FORBIDDEN_SRC_TOKENS {
            assert!(
                !normalized.contains(token),
                "kernel source `{name}` references forbidden API `{token}`: I/O, \
                 networking, process control, environment access, file inclusion \
                 and extra runtimes belong to plugins/embedders \
                 (see docs/KERNEL-BOUNDARY.md)"
            );
        }
    }
}

#[test]
fn sources_do_not_contain_business_vocabulary() {
    let root = repo_root();
    for file in src_files(&root) {
        let name = file.file_name().unwrap().to_string_lossy().to_string();
        let stem = file
            .file_stem()
            .map(|stem| stem.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        for word in FORBIDDEN_FILE_STEMS {
            assert!(
                !stem.contains(word),
                "kernel source file `{name}` advertises a business concern `{word}`"
            );
        }
        let raw = fs::read_to_string(&file).expect("read src file");

        // Code and string literals: compound identifiers count (`payment_gateway`,
        // `PaymentService`), not just standalone words.
        let words = identifier_words(&kernel_code(&raw));
        for forbidden in FORBIDDEN_DOMAIN_WORDS {
            assert!(
                !words.iter().any(|word| word == forbidden),
                "kernel source `{name}` contains business vocabulary `{forbidden}`: \
                 domain logic belongs to plugins, not the kernel"
            );
        }

        // Public documentation is part of the kernel contract too.
        let doc_words = identifier_words(&doc_comment_text(&raw));
        for forbidden in FORBIDDEN_DOC_WORDS {
            assert!(
                !doc_words.iter().any(|word| word == forbidden),
                "kernel documentation in `{name}` mentions domain vocabulary \
                 `{forbidden}`: rustdoc is a public contract, keep domain naming \
                 in plugins"
            );
        }
    }
}

#[test]
fn kernel_keeps_forbid_unsafe_and_a_bounded_public_surface() {
    let root = repo_root();
    let lib = kernel_code(&fs::read_to_string(root.join("src/lib.rs")).expect("src/lib.rs"));
    assert!(
        lib.contains("#![forbid(unsafe_code)]"),
        "the kernel must keep `#![forbid(unsafe_code)]`: soundness is a kernel promise"
    );

    let mut public_items = 0usize;
    for file in src_files(&root) {
        let source = kernel_code(&fs::read_to_string(&file).expect("read src file"));
        for line in source.lines() {
            let trimmed = line.trim_start();
            let is_public = [
                "pub fn ",
                "pub async fn ",
                "pub const fn ",
                "pub struct ",
                "pub enum ",
                "pub trait ",
                "pub type ",
                "pub const ",
                "pub static ",
                "pub use ",
                "pub mod ",
            ]
            .iter()
            .any(|prefix| trimmed.starts_with(prefix));
            if is_public {
                public_items += 1;
            }
        }
    }
    assert!(
        public_items <= PUBLIC_ITEM_CAP,
        "kernel public surface grew to {public_items} items (cap {PUBLIC_ITEM_CAP}); \
         keep the kernel minimal and put convenience/business APIs in plugins"
    );
}
