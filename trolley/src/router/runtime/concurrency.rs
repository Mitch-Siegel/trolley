use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard, OwnedSemaphorePermit, Semaphore, TryAcquireError, broadcast};

pub(super) struct Concurrency {
    n_permits: u32,
    permits: Arc<Mutex<Arc<Semaphore>>>,
    permit_tx: broadcast::Sender<()>,
    _permit_rx: broadcast::Receiver<()>,
}

pub(super) struct Permit {
    _inner: OwnedSemaphorePermit,
    avail_tx: broadcast::Sender<()>,
}

pub(super) struct ExclusivePermit<'a> {
    _inner: OwnedSemaphorePermit,
    avail_tx: broadcast::Sender<()>,
    _guard: MutexGuard<'a, Arc<Semaphore>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.avail_tx
            .send(())
            .expect("all permit availablility receivers dropped");
    }
}

impl Drop for ExclusivePermit<'_> {
    fn drop(&mut self) {
        self.avail_tx
            .send(())
            .expect("all permit availablility receivers dropped");
    }
}

impl Concurrency {
    pub(super) fn new(n_permits: u32) -> Self {
        let permits = Arc::new(Mutex::new(Arc::new(Semaphore::new(n_permits as usize))));
        let (permit_tx, permit_rx) = broadcast::channel(1);
        Self {
            n_permits,
            permits,
            permit_tx,
            _permit_rx: permit_rx,
        }
    }

    pub(super) async fn acquire_owned(&self) -> Permit {
        loop {
            // first, acquire the guard and try to get a permit
            let guard = self.permits.lock().await;
            match guard.clone().try_acquire_owned() {
                Ok(permit) => {
                    return Permit {
                        _inner: permit,
                        avail_tx: self.permit_tx.clone(),
                    };
                }
                Err(TryAcquireError::Closed) => panic!("permits semaphore closed"),
                Err(TryAcquireError::NoPermits) => {
                    let mut rx = self.permit_tx.subscribe();
                    // release the guard and wait for a notification that a permit is available
                    drop(guard);
                    match rx.recv().await {
                        Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => (),
                        other => {
                            panic!("unable to receive permit availability broadcast: {other:?}")
                        }
                    }
                }
            }
        }
    }

    pub(super) async fn acquire_all(&self) -> ExclusivePermit<'_> {
        let guard: MutexGuard<Arc<Semaphore>> = self.permits.lock().await;
        tracing::trace!(
            "holding circuit breaker lock & waiting for all outstanding requests to complete"
        );
        let sem = guard.clone();
        let permit = sem
            .acquire_many_owned(self.n_permits)
            .await
            .expect("unable to acquire all permits");

        ExclusivePermit {
            _inner: permit,
            avail_tx: self.permit_tx.clone(),
            _guard: guard,
        }
    }
}
