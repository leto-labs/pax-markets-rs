//! Startup-only affinity context for background threads created by ingestion.
use std::{cell::Cell, io};
#[derive(Clone, Copy)]
pub(crate) struct Target(core_affinity::CoreId);
impl Target {
    pub fn checked(id: usize) -> io::Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "worker CPU affinity requires Linux",
            ));
        }
        core_affinity::get_core_ids()
            .and_then(|cpus| cpus.into_iter().find(|c| c.id == id))
            .map(Self)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "worker CPU outside caller allowed mask",
                )
            })
    }
    pub fn pin(self) -> io::Result<()> {
        // Constructed from the caller's allowed mask before its owner is pinned.
        // The safe dependency sets a checked CPU ID; the kernel can still reject
        // a later cpuset change. Never infer that affinity reserves the CPU.
        if core_affinity::set_for_current(self.0) {
            Ok(())
        } else {
            Err(io::Error::other("cannot pin background worker"))
        }
    }
}
thread_local! { static BACKGROUND:Cell<Option<Target>> = const {Cell::new(None)}; }
pub(crate) fn configure(target: Option<Target>) {
    BACKGROUND.set(target);
}
pub(crate) fn target() -> Option<Target> {
    BACKGROUND.get()
}
pub(crate) fn pin(target: Option<Target>) -> io::Result<()> {
    target.map_or(Ok(()), Target::pin)
}
