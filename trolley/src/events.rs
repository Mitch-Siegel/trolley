use std::{
    any, fmt,
    sync::{
        Arc,
        atomic::{self, AtomicBool},
    },
};

use tokio::{sync::mpsc, task::JoinHandle};

pub struct Sender<E> {
    event_tx: mpsc::Sender<E>,
}
impl<E> Sender<E>
where
    E: fmt::Debug,
{
    #[must_use]
    pub const fn new(event_tx: mpsc::Sender<E>) -> Self {
        Self { event_tx }
    }

    pub async fn send(&self, event: E) {
        tracing::trace!("EventSender::send({:?})", event);
        match self.event_tx.send(event).await {
            Ok(()) => (),
            Err(e) => {
                tracing::error!(
                    "unable to send event: {}\nSomething has gone badly wrong - panicking (event_tx {:?})",
                    e,
                    self.event_tx,
                );
                panic!(
                    "unable to send event: {}\nSomething has gone badly wrong - panicking (event_tx {:?})",
                    e, self.event_tx,
                );
            }
        }
    }
}

impl<E> From<mpsc::Sender<E>> for Sender<E> {
    fn from(value: mpsc::Sender<E>) -> Self {
        Self { event_tx: value }
    }
}

impl<E> fmt::Debug for Sender<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event sender")
    }
}

impl<E> Clone for Sender<E> {
    fn clone(&self) -> Self {
        Self {
            event_tx: self.event_tx.clone(),
        }
    }
}

pub struct Receiver<E> {
    event_rx: mpsc::Receiver<E>,
    router_alive: Arc<AtomicBool>,
}

impl<E> Receiver<E>
where
    E: Send + 'static,
{
    pub const fn new(event_rx: mpsc::Receiver<E>, router_alive: Arc<AtomicBool>) -> Self {
        Self {
            event_rx,
            router_alive,
        }
    }

    // FUTURE: konw when router is shut down and exit (gracefully) automatically
    pub fn spawn_handler<HdlrCtx, F>(
        mut self,
        hdlr: impl Fn(E, HdlrCtx) -> F + Send + Sync + 'static,
        hdlr_meta: HdlrCtx,
    ) -> JoinHandle<()>
    where
        HdlrCtx: Clone + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            while let Some(event) = self.event_rx.recv().await {
                hdlr(event, hdlr_meta.clone()).await;
            }
            if self.router_alive.load(atomic::Ordering::Relaxed) {
                tracing::warn!("fell out of event receiver for {:?}", any::type_name::<E>());
            }
        })
    }

    pub async fn recv(&mut self) -> Option<E> {
        self.event_rx.recv().await
    }

    /// Try to receive an event without blocking
    ///
    /// # Errors
    /// Returns an error if no events are available, or if the channel is closed
    pub fn try_recv(&mut self) -> Result<E, mpsc::error::TryRecvError> {
        self.event_rx.try_recv()
    }
}

impl<E> fmt::Debug for Receiver<E>
where
    E: Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event receiver")
    }
}
