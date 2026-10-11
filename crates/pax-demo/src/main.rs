//! Read-only market-data application entry point.
mod cli;
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    match cli::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pax-demo: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
