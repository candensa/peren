#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompatFlagSupport {
    Supported,
    Unsupported,
    Partial(&'static str),
}

pub(crate) struct CompatFlagTable;

impl Default for CompatFlagTable {
    fn default() -> Self {
        Self
    }
}

impl CompatFlagTable {
    #[must_use]
    pub(crate) fn lookup(flag: &str) -> CompatFlagSupport {
        match flag {
            "durable_object_fetch_requires_full_url"
            | "nodejs_als"
            | "no_nodejs_compat"
            | "no_nodejs_compat_v2" => CompatFlagSupport::Supported,
            "nodejs_compat" | "nodejs_compat_v2" => CompatFlagSupport::Partial(
                "Peren implements a deliberate subset of Node APIs; verify every node: import",
            ),
            "streams_enable_constructors" => CompatFlagSupport::Partial(
                "stream constructors are available without full backpressure semantics",
            ),
            _ => CompatFlagSupport::Unsupported,
        }
    }

    #[must_use]
    pub(crate) fn check(flags: &[String]) -> (Vec<String>, Vec<(String, &'static str)>) {
        let mut unsupported = Vec::new();
        let mut partial = Vec::new();
        for flag in flags {
            match Self::lookup(flag) {
                CompatFlagSupport::Unsupported => unsupported.push(flag.clone()),
                CompatFlagSupport::Partial(note) => partial.push((flag.clone(), note)),
                CompatFlagSupport::Supported => {}
            }
        }
        (unsupported, partial)
    }
}
