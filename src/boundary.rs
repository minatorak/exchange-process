//! Dependency-direction gate, enforced by reading the layer sources. Layers
//! import downward only:
//!
//! ```text
//! domain < application < { api, infrastructure } < runtime
//!                          (these two are peers)
//! ```
//!
//! The scan runs over `src/` at test time from the manifest directory, so a
//! forbidden edge fails `cargo test` where the author can see it.

use std::fs;
use std::path::{Path, PathBuf};

type Rule = (&'static str, &'static [&'static str]);

const RULES: &[Rule] = &[
    (
        "domain",
        &[
            "crate::application",
            "crate::infrastructure",
            "crate::api",
            "crate::runtime",
        ],
    ),
    (
        "application",
        &["crate::infrastructure", "crate::api", "crate::runtime"],
    ),
    ("api", &["crate::infrastructure", "crate::runtime"]),
    ("infrastructure", &["crate::api", "crate::runtime"]),
];

fn layer_sources(layer: &str) -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(layer);
    let mut sources = Vec::new();
    let mut stack = vec![root];
    while let Some(directory) = stack.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }
    sources
}

fn forbidden_edges(source: &str, layer: &str, path: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    for (line_number, line) in source.lines().enumerate() {
        for edge in RULES
            .iter()
            .find(|(owner, _)| *owner == layer)
            .map(|(_, edges)| *edges)
            .unwrap_or(&[])
        {
            if line.contains(edge) {
                violations.push(format!(
                    "{}:{} imports {edge} (upward/peer edge)",
                    path.display(),
                    line_number + 1
                ));
            }
        }
    }
    violations
}

#[test]
fn layers_import_downward_only() {
    let mut violations = Vec::new();
    for (layer, _) in RULES {
        for path in layer_sources(layer) {
            let source = fs::read_to_string(&path).expect("layer source is readable");
            violations.extend(forbidden_edges(&source, layer, &path));
        }
    }
    assert!(
        violations.is_empty(),
        "dependency-direction violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn every_layer_has_at_least_one_declared_source() {
    // The single-package scaffold keeps all five layers; an accidentally
    // deleted layer file would silently drop the scan for it.
    for (layer, _) in RULES {
        assert!(
            !layer_sources(layer).is_empty(),
            "layer {layer} has no .rs source to scan"
        );
    }
}
