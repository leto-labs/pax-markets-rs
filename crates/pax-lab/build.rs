//! Build metadata for reproducible offline benchmarks.
#[path = "../../build-support/build_info.rs"]
mod build_info;
fn main() {
    build_info::embed();
}
