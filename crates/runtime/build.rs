use std::{
    env,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};

const PARTS: &[&str] = &[
    "prelude", "crypto", "channel", "global", "source", "storage", "object", "media", "ai",
    "vector", "binding", "workflow", "event", "cache", "hydrate",
];

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let source = manifest.join("src").join("bootstrap");
    let bundle = manifest.join("src").join("bootstrap.js");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("output directory"));

    println!("cargo:rerun-if-changed={}", bundle.display());
    for part in PARTS {
        println!(
            "cargo:rerun-if-changed={}",
            source.join(format!("{part}.js")).display()
        );
    }

    let expected = expected_bundle(&source);
    let actual = fs::read_to_string(&bundle).expect("read src/bootstrap.js");
    assert!(
        actual == expected,
        "src/bootstrap.js is out of sync with src/bootstrap/*.js; run `make bootstrap`"
    );

    generate_embedded_deno_sources(&out_dir);
}

fn expected_bundle(source: &Path) -> String {
    let mut bundle = String::from(
        "// Generated from crates/runtime/src/bootstrap/*.js. Edit those files and rebuild the checked-in bundle.\n",
    );
    for part in PARTS {
        let path = source.join(format!("{part}.js"));
        bundle.push_str(&fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("read {}: {error}", path.display());
        }));
    }
    bundle
}

fn generate_embedded_deno_sources(out_dir: &Path) {
    let files = [
        DenoSource::lazy_js("deno_webidl", "deno_webidl", "0.250.0", "00_webidl.js"),
        DenoSource::lazy_esm("deno_web", "deno_web", "0.281.0", "geometry.js"),
        DenoSource::lazy_esm("deno_web", "deno_web", "0.281.0", "webtransport.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "00_infra.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "00_url.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "01_broadcast_channel.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "01_console.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "01_dom_exception.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "01_mimesniff.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "01_urlpattern.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "02_event.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "02_structured_clone.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "02_timers.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "03_abort_signal.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "04_global_interfaces.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "05_base64.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "06_streams.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "08_text_encoding.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "09_file.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "10_filereader.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "12_location.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "13_message_port.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "14_compression.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "15_performance.js"),
        DenoSource::lazy_js("deno_web", "deno_web", "0.281.0", "16_image_data.js"),
        DenoSource::lazy_esm("deno_net", "deno_net", "0.242.0", "03_quic.js"),
        DenoSource::lazy_js("deno_net", "deno_net", "0.242.0", "01_net.js"),
        DenoSource::lazy_js("deno_net", "deno_net", "0.242.0", "02_tls.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "20_headers.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "21_formdata.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "22_body.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "22_http_client.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "23_request.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "23_response.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "26_fetch.js"),
        DenoSource::lazy_js("deno_fetch", "deno_fetch", "0.274.0", "27_eventsource.js"),
    ];

    let embedded_dir = out_dir.join("embedded-deno");
    fs::create_dir_all(&embedded_dir).expect("create embedded Deno source directory");

    let mut generated =
        String::from("use deno_core::{Extension, ExtensionFileSource};\nuse std::borrow::Cow;\n\n");
    generated.push_str("pub(super) fn embed(extension: &mut Extension) {\n");
    for (extension, target) in [
        ("deno_webidl", "lazy_loaded_js_files"),
        ("deno_web", "lazy_loaded_esm_files"),
        ("deno_web", "lazy_loaded_js_files"),
        ("deno_net", "lazy_loaded_esm_files"),
        ("deno_net", "lazy_loaded_js_files"),
        ("deno_fetch", "lazy_loaded_js_files"),
    ] {
        writeln!(
            generated,
            "    if extension.name == {extension:?} {{ extension.{target} = Cow::Owned(Vec::new()); }}"
        )
        .expect("write generated Deno extension reset");
    }

    for (index, file) in files.iter().enumerate() {
        let source = locate_dependency_file(file.package, file.version, file.file);
        println!("cargo:rerun-if-changed={}", source.display());
        let copy = embedded_dir.join(format!("{index:02}_{}", file.file));
        fs::copy(&source, &copy).unwrap_or_else(|error| {
            panic!("copy {} to {}: {error}", source.display(), copy.display());
        });
        let target = match file.kind {
            SourceKind::LazyEsm => "lazy_loaded_esm_files",
            SourceKind::LazyJs => "lazy_loaded_js_files",
        };
        let specifier = format!("ext:{}/{}", file.extension, file.file);
        let copy = copy.display().to_string();
        writeln!(
            generated,
            "    if extension.name == {:?} {{ extension.{}.to_mut().push(ExtensionFileSource::new({specifier:?}, deno_core::ascii_str_include!({copy:?}))); }}",
            file.extension, target
        )
        .expect("write generated Deno extension source");
    }

    generated.push_str("}\n");
    fs::write(out_dir.join("embedded_deno_sources.rs"), generated)
        .expect("write embedded Deno source table");
}

fn locate_dependency_file(package: &str, version: &str, file: &str) -> PathBuf {
    let cargo_home = env::var_os("CARGO_HOME").map_or_else(
        || PathBuf::from(env::var_os("HOME").expect("home directory")).join(".cargo"),
        PathBuf::from,
    );
    let registry = cargo_home.join("registry").join("src");
    let package_dir = format!("{package}-{version}");
    let mut candidates = fs::read_dir(&registry)
        .unwrap_or_else(|error| panic!("read {}: {error}", registry.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path().join(&package_dir).join(file))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    candidates.sort();
    candidates
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("locate {package_dir}/{file} under {}", registry.display()))
}

struct DenoSource {
    extension: &'static str,
    package: &'static str,
    version: &'static str,
    file: &'static str,
    kind: SourceKind,
}

impl DenoSource {
    const fn lazy_esm(
        extension: &'static str,
        package: &'static str,
        version: &'static str,
        file: &'static str,
    ) -> Self {
        Self {
            extension,
            package,
            version,
            file,
            kind: SourceKind::LazyEsm,
        }
    }

    const fn lazy_js(
        extension: &'static str,
        package: &'static str,
        version: &'static str,
        file: &'static str,
    ) -> Self {
        Self {
            extension,
            package,
            version,
            file,
            kind: SourceKind::LazyJs,
        }
    }
}

enum SourceKind {
    LazyEsm,
    LazyJs,
}
