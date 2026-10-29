use std::{
    fmt,
    sync::{Arc, atomic::AtomicBool},
};

use tokio::{sync::mpsc, task::JoinHandle};

pub struct EventSender<E> {
    event_tx: mpsc::Sender<E>,
}
impl<E> EventSender<E>
where
    E: fmt::Debug,
{
    pub fn new(event_tx: mpsc::Sender<E>) -> Self {
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

impl<E> From<mpsc::Sender<E>> for EventSender<E> {
    fn from(value: mpsc::Sender<E>) -> Self {
        Self { event_tx: value }
    }
}

impl<E> fmt::Debug for EventSender<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event sender")
    }
}

impl<E> Clone for EventSender<E> {
    fn clone(&self) -> Self {
        Self {
            event_tx: self.event_tx.clone(),
        }
    }
}

pub struct EventReceiver<E> {
    event_rx: mpsc::Receiver<E>,
    router_alive: Arc<AtomicBool>,
}

impl<E> EventReceiver<E>
where
    E: Send + 'static,
{
    pub fn new(event_rx: mpsc::Receiver<E>, router_alive: Arc<AtomicBool>) -> Self {
        Self {
            event_rx,
            router_alive,
        }
    }

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
            if self.router_alive.load(std::sync::atomic::Ordering::Relaxed) {
                tracing::warn!(
                    "fell out of event receiver for {:?}",
                    std::any::type_name::<E>()
                );
            }
        })
    }

    pub async fn recv(&mut self) -> Option<E> {
        self.event_rx.recv().await
    }

    pub async fn try_recv(&mut self) -> Result<E, mpsc::error::TryRecvError> {
        self.event_rx.try_recv()
    }
}

impl<E> fmt::Debug for EventReceiver<E>
where
    E: Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event receiver")
    }
}
