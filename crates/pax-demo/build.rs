//! Embed application build provenance.
#[path = "../../build-support/build_info.rs"]
mod build_info;
fn main() {
    build_info::embed();
}
