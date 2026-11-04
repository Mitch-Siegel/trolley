use std::{env, path};

use trolley_build::interface;

fn main() {
    println!("cargo:rerun-if-changed=src/");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let src_dir = path::PathBuf::from(manifest_dir).join("src/");

    interface::generator(&[&src_dir], path::Path::new("trolley_calculator.rs"));
}
