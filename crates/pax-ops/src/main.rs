//! Scoped deployment operator entry point.
mod cli;

fn main() {
    if let Err(error) = cli::run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
