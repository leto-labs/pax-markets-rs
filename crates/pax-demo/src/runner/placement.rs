//! Optional single-thread ingestion ownership; HTTP stays on the caller runtime.
use super::Error;
use std::{future::Future, time::Duration};
use tokio::sync::oneshot;

/// Runtime placement, selected once before observation rather than per input.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
#[serde(tag = "runtime", rename_all = "snake_case")]
pub enum Placement {
    /// Existing caller runtime; portable default and comparison baseline.
    #[default]
    Shared,
    /// Managed current-thread runtime with explicit ingestion and worker CPUs.
    DedicatedWithWorkerCpu {
        /// Allowed logical CPU for ingestion.
        cpu: usize,
        /// Allowed logical CPU for report, capture and sidecar worker threads.
        worker_cpu: usize,
    },
    /// Dedicated runtime with legacy inherited background-thread affinity.
    Dedicated {
        /// Logical CPU ID, not an index into the available-CPU list.
        cpu: Option<usize>,
    },
}
impl Placement {
    /// Validate requested affinity before metadata access or spawning a thread.
    pub fn validate(self) -> Result<(), Error> {
        let (cpu, worker_cpu) = match self {
            Self::Shared => (None, None),
            Self::Dedicated { cpu } => (cpu, None),
            Self::DedicatedWithWorkerCpu { cpu, worker_cpu } => (Some(cpu), Some(worker_cpu)),
        };
        if let Some(worker_cpu) = worker_cpu {
            crate::worker::Target::checked(worker_cpu)?;
        }
        if let Some(id) = cpu {
            // Only Linux's allowed-mask implementation is supported by this
            // application. Other platforms retain unpinned dedicated execution.
            if !cfg!(target_os = "linux") {
                return Err("CPU affinity is supported only on Linux; omit --ingest-cpu".into());
            }
            let allowed = core_affinity::get_core_ids().ok_or("cannot read allowed CPU set")?;
            if !allowed.iter().any(|cpu| cpu.id == id) {
                return Err(
                    format!("ingestion CPU {id} is outside the caller's allowed CPU set").into(),
                );
            }
        }
        Ok(())
    }
}

/// Execute a source on the selected runtime and join its dedicated owner.
/// Normal stop signals remain the source's responsibility, including capture
/// drain. Dropping this future cancels the dedicated source and its network tasks;
/// an abruptly canceled capture is left explicitly incomplete.
pub async fn run<F, T>(placement: Placement, source: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>> + Send + 'static,
    T: Send + 'static,
{
    placement.validate()?;
    let (cpu, worker) = match placement {
        Placement::Shared => return source.await,
        Placement::Dedicated { cpu } => (cpu, None),
        Placement::DedicatedWithWorkerCpu { cpu, worker_cpu } => {
            (Some(cpu), Some(crate::worker::Target::checked(worker_cpu)?))
        }
    };
    let (cancel, canceled) = oneshot::channel();
    let _owner = CancelOnDrop(Some(cancel));
    let (completed, completion) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name("pax-ingestion".into())
        .spawn(move || {
            let result = (|| {
                crate::worker::configure(worker);
                if let Some(id) = cpu {
                    // Safe API; id was checked against sched_getaffinity's allowed
                    // mask before the dependency can call CPU_SET.
                    // https://docs.rs/core_affinity/0.8.3/core_affinity/
                    if !core_affinity::set_for_current(core_affinity::CoreId { id }) {
                        return Err(format!("cannot pin ingestion to CPU {id}").into());
                    }
                }
                // No worker threads: transport and reconstruction share this owner.
                // Tokio 1.53.2 runtime/builder.rs::new_current_thread / enable_all.
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let result = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = canceled => Err("dedicated ingestion owner canceled".into()),
                        result = source => result,
                    }
                });
                runtime.shutdown_timeout(Duration::from_secs(1));
                result
            })();
            let _ = completed.send(result);
        })?;
    let result = completion.await;
    tokio::task::spawn_blocking(move || thread.join())
        .await?
        .map_err(|_| "dedicated ingestion thread panicked")?;
    result.map_err(|_| "dedicated ingestion closed without a result")?
}
struct CancelOnDrop(Option<oneshot::Sender<()>>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            let _ = cancel.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn background_worker_can_escape_inherited_ingestion_affinity() {
        let before = core_affinity::get_core_ids().unwrap();
        if before.len() < 2 {
            return;
        }
        let (cpu, worker_cpu) = (before[0], before[1]);
        run(
            Placement::DedicatedWithWorkerCpu {
                cpu: cpu.id,
                worker_cpu: worker_cpu.id,
            },
            async move {
                assert_eq!(core_affinity::get_core_ids().unwrap(), vec![cpu]);
                let target = crate::worker::target();
                let worker = std::thread::spawn(move || {
                    assert_eq!(core_affinity::get_core_ids().unwrap(), vec![cpu]);
                    crate::worker::pin(target).unwrap();
                    assert_eq!(core_affinity::get_core_ids().unwrap(), vec![worker_cpu]);
                });
                worker.join().unwrap();
                assert_eq!(core_affinity::get_core_ids().unwrap(), vec![cpu]);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(core_affinity::get_core_ids().unwrap(), before);
        assert!(
            Placement::DedicatedWithWorkerCpu {
                cpu: cpu.id,
                worker_cpu: usize::MAX
            }
            .validate()
            .is_err()
        );
    }
    #[tokio::test]
    async fn dedicated_tasks_share_a_thread_and_return_errors() {
        let error = run(Placement::Dedicated { cpu: None }, async {
            let owner = std::thread::current().id();
            assert_eq!(
                tokio::spawn(async { std::thread::current().id() })
                    .await
                    .unwrap(),
                owner
            );
            Err::<(), Error>("source failure".into())
        })
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "source failure");
    }
    #[tokio::test]
    async fn aborting_caller_drops_dedicated_work() {
        struct Dropped(Option<oneshot::Sender<()>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let (ready, started) = oneshot::channel();
        let (done, dropped) = oneshot::channel();
        let owner = tokio::spawn(run(Placement::Dedicated { cpu: None }, async move {
            let _guard = Dropped(Some(done));
            let _ = ready.send(());
            std::future::pending::<Result<(), Error>>().await
        }));
        started.await.unwrap();
        owner.abort();
        let _ = owner.await;
        tokio::time::timeout(Duration::from_secs(2), dropped)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn panic_is_reported_and_owner_joined() {
        let error = run(Placement::Dedicated { cpu: None }, async {
            panic!("test panic");
            #[allow(unreachable_code)]
            Ok::<(), Error>(())
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("panicked"));
    }
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn affinity_applies_only_to_the_dedicated_owner() {
        let before = core_affinity::get_core_ids().unwrap();
        let cpu = before[0];
        run(Placement::Dedicated { cpu: Some(cpu.id) }, async move {
            assert_eq!(core_affinity::get_core_ids().unwrap(), vec![cpu]);
            tokio::task::yield_now().await;
            assert_eq!(core_affinity::get_core_ids().unwrap(), vec![cpu]);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(core_affinity::get_core_ids().unwrap(), before);
        assert!(
            Placement::Dedicated {
                cpu: Some(usize::MAX)
            }
            .validate()
            .is_err()
        );
    }
}
