use std::sync::Arc;

use tokio::sync::{
    Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, TryAcquireError, broadcast,
};

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

pub(super) struct ExclusivePermit {
    _inner: OwnedSemaphorePermit,
    avail_tx: broadcast::Sender<()>,
    _guard: OwnedMutexGuard<Arc<Semaphore>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.avail_tx
            .send(())
            .expect("all permit availablility receivers dropped");
    }
}

impl Drop for ExclusivePermit {
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

    pub(super) async fn acquire_all(&self) -> ExclusivePermit {
        let guard: OwnedMutexGuard<Arc<Semaphore>> = self.permits.clone().lock_owned().await;
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

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::task;

    impl Concurrency {
        #[cfg(test)]
        pub(crate) async fn try_acquire_owned(&self) -> Option<Permit> {
            let guard = self.permits.lock().await;
            match guard.clone().try_acquire_owned() {
                Ok(permit) => Some(Permit {
                    _inner: permit,
                    avail_tx: self.permit_tx.clone(),
                }),
                Err(TryAcquireError::Closed) => panic!("permits semaphore closed"),
                Err(TryAcquireError::NoPermits) => None,
            }
        }

        #[cfg(test)]
        pub(crate) async fn try_acquire_all(&self) -> Option<ExclusivePermit> {
            let guard: OwnedMutexGuard<Arc<Semaphore>> = self.permits.clone().lock_owned().await;
            match guard.clone().try_acquire_many_owned(self.n_permits) {
                Ok(permit) => Some(ExclusivePermit {
                    _inner: permit,
                    avail_tx: self.permit_tx.clone(),
                    _guard: guard,
                }),
                Err(TryAcquireError::Closed) => panic!("permits semaphore closed"),
                Err(TryAcquireError::NoPermits) => None,
            }
        }
    }

    #[tokio::test]
    async fn acquire_owned() {
        let permits = Arc::new(Concurrency::new(2));
        let p1 = permits.acquire_owned().await;
        let p2 = permits.acquire_owned().await;

        let move_permits = permits.clone();
        let p3 = tokio::spawn(async move { move_permits.acquire_owned().await });
        assert!(!p3.is_finished()); // should be blocked, all are owned

        // yield to allow p3 to receive `TryAcquireError::NoPermits`
        task::yield_now().await;

        // free up a permit
        drop(p1);
        task::yield_now().await;

        // now the third permit should be available
        assert!(p3.is_finished());
        drop(p3.await.unwrap());
        drop(p2);
    }

    #[tokio::test]
    async fn try_acquire_owned() {
        let permits = Arc::new(Concurrency::new(2));
        let p1 = permits.acquire_owned().await;
        let p2 = permits.acquire_owned().await;

        // try to acquire a third permit, should fail
        assert!(permits.try_acquire_owned().await.is_none());

        // free up a permit
        drop(p1);
        task::yield_now().await;

        // now the third permit should be available
        assert!(permits.try_acquire_owned().await.is_some());
        drop(p2);
    }

    #[tokio::test]
    async fn acquire_all() {
        let permits = Arc::new(Concurrency::new(2));

        let all = permits.acquire_all().await;
        drop(all);

        // hold a single permit while trying to acquire all
        let single = permits.acquire_owned().await;
        let move_permits = permits.clone();
        let join_all = tokio::spawn(async move { move_permits.acquire_all().await });

        // we better not have gotten all, since we hold `single`
        task::yield_now().await;
        assert!(!join_all.is_finished());

        // and we better not be able to get another single permit, since acquire_all is holding the guard
        let move_permits = permits.clone();
        let join_single = tokio::spawn(async move { move_permits.try_acquire_owned().await });
        task::yield_now().await;
        assert!(!join_single.is_finished());

        drop(single);
        task::yield_now().await;

        // we expect the single permit to still not be able to acquire, since acquire_all is still holding the guard
        assert!(!join_single.is_finished());

        // we should have succeeded in acquiring all permits here though
        assert!(join_all.is_finished());
        join_single.abort();
        drop(join_all.await.unwrap());
    }

    #[tokio::test]
    async fn try_acquire_all() {
        let permits = Arc::new(Concurrency::new(2));

        // should succeed when no permits are held
        let all = permits.try_acquire_all().await;
        assert!(all.is_some());
        drop(all);

        // should fail when a single permit is held
        let single = permits.acquire_owned().await;
        assert!(permits.try_acquire_all().await.is_none());

        // should succeed again after the permit is released
        drop(single);
        task::yield_now().await;
        let all = permits.try_acquire_all().await;
        assert!(all.is_some());

        // while holding all, try_acquire_owned should block (guard is held)
        let move_permits = permits.clone();
        let join_single = tokio::spawn(async move { move_permits.try_acquire_owned().await });
        // FUTURE: if we try to acquire a single permit at the same time as trying to acquire all, while all are held, we should go from all to all_2, never all to single
        task::yield_now().await;
        assert!(!join_single.is_finished());
        drop(all);
        task::yield_now().await;
        assert!(join_single.await.unwrap().is_some());
    }
}
