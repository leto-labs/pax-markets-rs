//! Join both demo tasks without allowing an unread response to prevent CLI exit.
use pax_demo::runner::Error;
use std::time::Duration;
use tokio::{sync::watch, task::JoinHandle};

pub(super) async fn supervise(
    mut http: JoinHandle<Result<(), Error>>,
    mut source: JoinHandle<Result<(), Error>>,
    stop: watch::Sender<bool>,
    grace: Duration,
) -> Result<(), Error> {
    let (first, remaining) = tokio::select! {
        result = &mut http => (result, &mut source),
        result = &mut source => (result, &mut http),
    };
    let _ = stop.send(true);
    let second = match tokio::time::timeout(grace, &mut *remaining).await {
        Ok(result) => result.map_err(Error::from).and_then(|result| result),
        Err(_) => {
            // serve has no recording writer to finalize. The CLI runtime drops
            // outstanding HTTP connection tasks after this supervisor returns.
            remaining.abort();
            let _ = remaining.await;
            Err("demo shutdown exceeded its grace period; remaining task aborted".into())
        }
    };
    // Preserve the initiating failure, or report a secondary shutdown failure.
    first??;
    second
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn secondary_failure_is_reported_after_clean_source_exit() {
        let (stop, mut receiver) = watch::channel(false);
        let http = tokio::spawn(async move {
            receiver.changed().await.unwrap();
            Err("HTTP shutdown failed".into())
        });
        let source = tokio::spawn(async { Ok(()) });
        let error = supervise(http, source, stop, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "HTTP shutdown failed");
    }

    #[tokio::test]
    async fn blocked_http_task_is_aborted_after_the_grace_period() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let observer = dropped.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let http = tokio::spawn(async move {
            let _guard = Dropped(observer);
            let _ = ready.send(());
            std::future::pending::<Result<(), Error>>().await
        });
        started.await.unwrap();
        let source = tokio::spawn(async { Ok(()) });
        let (stop, _receiver) = watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            supervise(http, source, stop, Duration::from_millis(10)),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().to_string().contains("grace period"));
        assert!(dropped.load(Ordering::SeqCst));
    }
}
