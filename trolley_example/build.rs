use std::{env, path};

use trolley::{generate_call_ctxs, interface};

fn main() {
    println!("cargo:rerun-if-changed=src/");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let src_dir = path::PathBuf::from(manifest_dir).join("src/");

    generate_call_ctxs(path::Path::new("trolley_call_ctx.rs"));

    interface::generator(
        &[&src_dir],
        path::Path::new("trolley_calculator.rs"),
        &["services"],
    );
}
