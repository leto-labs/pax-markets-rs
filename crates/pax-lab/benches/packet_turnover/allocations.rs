//! Optional allocation diagnostics. Never enabled in the portable timing baseline.
use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
static ACTIVE: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "mimalloc")]
static INNER: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(not(feature = "mimalloc"))]
static INNER: std::alloc::System = std::alloc::System;
struct CountedSystem;
#[global_allocator]
static ALLOCATOR: CountedSystem = CountedSystem;

// SAFETY: Every operation forwards its unchanged pointer/layout contract to
// the selected inner allocator. The wrapper never dereferences pointers, allocates, panics, or unwinds
// while recording. Relaxed atomics count diagnostic events, not object lifetimes.
unsafe impl GlobalAlloc for CountedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller guarantees a valid allocation layout.
        let pointer = unsafe { INNER.alloc(layout) };
        record(pointer, layout.size());
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Forward the valid layout; The inner allocator preserves zeroing semantics.
        let pointer = unsafe { INNER.alloc_zeroed(layout) };
        record(pointer, layout.size());
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller supplies this allocator's live pointer and original layout.
        unsafe {
            INNER.dealloc(pointer, layout);
        }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: Forward the caller's live allocation and valid new size unchanged.
        let result = unsafe { INNER.realloc(pointer, layout, size) };
        record(result, size);
        result
    }
}
fn record(pointer: *mut u8, size: usize) {
    if !pointer.is_null() && ACTIVE.load(Ordering::Relaxed) {
        CALLS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}
/// Count successful allocation/reallocation calls and requested bytes. Runs only
/// in this single-threaded benchmark; returned values are dropped after counting.
pub fn measure<T>(function: impl FnOnce() -> T) -> (T, u64, u64) {
    CALLS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Relaxed);
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.store(false, Ordering::Relaxed);
        }
    }
    let reset = Reset;
    let result = function();
    drop(reset);
    (
        result,
        CALLS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}
/// Verify the hook observes real allocations before trusting a zero result.
pub fn self_check() {
    let (buffer, calls, bytes) = measure(|| std::hint::black_box(vec![1u8; 4096]));
    assert!(calls >= 1 && bytes >= 4096);
    drop(buffer);
}
