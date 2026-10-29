fn main() {
    println!("cargo:rerun-if-changed=src/");

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let src_dir = std::path::PathBuf::from(manifest_dir).join("src/");

    trolley::generate_call_ctx(std::path::Path::new("trolley_call_ctx.rs"));

    trolley::interface::generator(
        &[&src_dir],
        std::path::Path::new("trolley_calculator.rs"),
        &["services"],
    );
}
