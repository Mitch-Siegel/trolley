use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{sync::Notify, task, time::Duration};

use crate::{RouterBase, RouterMeta, RouterPriv, Service, router};

use super::*;

struct MockCall<T> {
    expected_value: usize,
    wait_on: Arc<Notify>,
    t: T,
}

struct NotSent;
struct Sent {
    result: oneshot::Receiver<Result<<MockState as Service<SomethingArgs>>::Output, routed::Error>>,
    first_wait: oneshot::Receiver<()>,
    second_wait: oneshot::Receiver<()>,
}
struct Dispatched {
    result: oneshot::Receiver<Result<<MockState as Service<SomethingArgs>>::Output, routed::Error>>,
    first_wait: oneshot::Receiver<()>,
    second_wait: oneshot::Receiver<()>,
}
struct FirstBlock {
    result: oneshot::Receiver<Result<<MockState as Service<SomethingArgs>>::Output, routed::Error>>,
    second_wait: oneshot::Receiver<()>,
}
struct SecondBlock {
    result: oneshot::Receiver<Result<<MockState as Service<SomethingArgs>>::Output, routed::Error>>,
    second_wait: oneshot::Receiver<()>,
}
struct SecondUnblock(
    oneshot::Receiver<Result<<MockState as Service<SomethingArgs>>::Output, routed::Error>>,
);

impl MockCall<NotSent> {
    fn new(expected_value: usize) -> Self {
        Self {
            expected_value,
            wait_on: Arc::new(Notify::new()),
            t: NotSent,
        }
    }

    async fn send(
        self,
        cmd_tx: &mpsc::Sender<Box<dyn routed::AnyCall<MockState>>>,
    ) -> MockCall<Sent> {
        let (first_wait_tx, first_wait_rx) = oneshot::channel();
        let (second_wait_tx, second_wait_rx) = oneshot::channel();

        let (call, result_rx) = routed::Const::new(
            SomethingArgs {
                wait_on: self.wait_on.clone(),
                first_wait: first_wait_tx,
                second_wait: second_wait_tx,
            },
            routed::Timeout::Single(Duration::from_millis(100)),
        );
        cmd_tx.send(Box::new(call)).await.unwrap();
        MockCall::<Sent> {
            expected_value: self.expected_value,
            wait_on: self.wait_on,
            t: Sent {
                result: result_rx,
                first_wait: first_wait_rx,
                second_wait: second_wait_rx,
            },
        }
    }
}

impl MockCall<Sent> {
    fn check_dispatched(self) -> Result<MockCall<Dispatched>, &'static str> {
        // check that we haven't finished
        if !self.t.result.is_empty() {
            return Err("unexpected result received");
        }

        // check that we are not yet at either the first or second wait point
        if !self.t.first_wait.is_empty() {
            return Err("shouldn't have arrived at first wait point");
        }
        if !self.t.second_wait.is_empty() {
            return Err("shouldn't have arrived at second wait point");
        }

        Ok(MockCall::<Dispatched> {
            expected_value: self.expected_value,
            wait_on: self.wait_on,
            t: Dispatched {
                result: self.t.result,
                first_wait: self.t.first_wait,
                second_wait: self.t.second_wait,
            },
        })
    }
}

impl MockCall<Dispatched> {
    fn check_first_block(mut self) -> Result<MockCall<FirstBlock>, &'static str> {
        // check that we haven't finished
        if !self.t.result.is_empty() {
            return Err("unexpected result received");
        }

        // we have arrived at the first wait point
        match self.t.first_wait.try_recv() {
            Ok(()) => {}
            Err(_) => return Err("should have arrived at first wait point"),
        }

        // we have not arrived at the second wait point
        if !self.t.second_wait.is_empty() {
            return Err("shouldn't have arrived at second wait point");
        }

        // advance past the first wait point
        self.wait_on.notify_one();

        Ok(MockCall::<FirstBlock> {
            expected_value: self.expected_value,
            wait_on: self.wait_on,
            t: FirstBlock {
                result: self.t.result,
                second_wait: self.t.second_wait,
            },
        })
    }
}

impl MockCall<FirstBlock> {
    fn check_first_unblock(self) -> Result<MockCall<SecondBlock>, &'static str> {
        // check that we haven't finished
        if !self.t.result.is_empty() {
            return Err("unexpected result received");
        }

        // unblock from the first wait point
        self.wait_on.notify_one();

        Ok(MockCall::<SecondBlock> {
            expected_value: self.expected_value,
            wait_on: self.wait_on,
            t: SecondBlock {
                second_wait: self.t.second_wait,
                result: self.t.result,
            },
        })
    }
}

impl MockCall<SecondBlock> {
    fn check_second_block(mut self) -> Result<MockCall<SecondUnblock>, &'static str> {
        // check that we have finished
        if !self.t.result.is_empty() {
            return Err("unexpected result received");
        }

        // check that we have arrived at the second wait point
        match self.t.second_wait.try_recv() {
            Ok(()) => {}
            Err(_) => return Err("should have arrived at second wait point"),
        }

        Ok(MockCall::<SecondUnblock> {
            expected_value: self.expected_value,
            wait_on: self.wait_on,
            t: SecondUnblock(self.t.result),
        })
    }
}

impl MockCall<SecondUnblock> {
    fn check_second_block(mut self) -> Result<(), String> {
        // check that we have finished
        let out = match self.t.0.try_recv() {
            Ok(res) => res.expect("recv error getting result"),
            Err(_) => return Err("should have received result".to_string()),
        };

        if out != self.expected_value {
            return Err(format!(
                "unexpected result returned from call: got {}, expected {}",
                out, self.expected_value
            ));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct MockState {
    count: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl RouterBase for MockState {
    type Notif = ();
    type Event = ();

    type ShutdownResult = ();

    async fn shutdown(self) -> Self::ShutdownResult {}

    fn handle_notification(&mut self, _notif: Self::Notif) {
        self.count.store(5, Ordering::Relaxed);
    }
}

impl RouterMeta for () {
    type RouterState = MockState;
}

#[async_trait::async_trait]
impl RouterPriv for MockState {
    type InternalEvent = usize;
    type Meta = ();

    async fn new(_meta: Self::Meta, _ctx: router::Ctx<Self>) -> Self {
        unreachable!();
    }

    fn handle_internal_event(&self, internal: Self::InternalEvent) {
        self.count.store(internal, Ordering::Relaxed);
    }
}

impl Router for MockState {}

#[derive(Debug)]
struct SomethingArgs {
    wait_on: Arc<Notify>,
    first_wait: oneshot::Sender<()>,
    second_wait: oneshot::Sender<()>,
}

#[async_trait::async_trait]
impl Service<SomethingArgs> for MockState {
    type Output = usize;

    async fn call(&self, args: SomethingArgs) -> Self::Output {
        args.first_wait.send(()).unwrap();
        args.wait_on.notified().await;
        self.count.fetch_add(1, Ordering::Relaxed);
        let result = self.count.load(Ordering::Relaxed);
        args.second_wait.send(()).unwrap();
        args.wait_on.notified().await;
        result
    }
}

fn test_setup(
    n_permits: u32,
) -> (
    Arc<Concurrency>,
    Arc<RwLock<MockState>>,
    Arc<Notify>,
    Arc<AtomicUsize>,
) {
    let concurrency = Arc::new(Concurrency::new(n_permits));
    let wait_on = Arc::new(Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(RwLock::new(MockState {
        count: count.clone(),
    }));

    (concurrency, state, wait_on, count)
}

#[tokio::test]
async fn cmd_rx() {
    let (cmd_tx, cmd_rx) = mpsc::channel(2);
    let (shutdown_cmd_tx, shutdown_cmd_rx) = broadcast::channel(1);
    let (concurrency, state, _wait_on, count) = test_setup(1);

    let join_cmd_rx = tokio::spawn(cmd_rx_loop(
        cmd_rx,
        concurrency.clone(),
        state.clone(),
        shutdown_cmd_rx,
    ));

    // sanity check initial state
    assert!(concurrency.try_acquire_owned().await.is_some());
    assert!(state.try_read().is_ok());
    assert_eq!(count.load(Ordering::Relaxed), 0);

    let unsent = MockCall::new(1);

    // send a command, sanity check result rx
    let sent = unsent.send(&cmd_tx).await;

    let dispatched = sent.check_dispatched().unwrap();

    // first, yield for cmd_rx_loop to dequeue and spawn
    task::yield_now().await;

    // at this point, the task should have run, but should be blocked on wait_on
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok());
    assert_eq!(count.load(Ordering::Relaxed), 0);

    // unblock to do the addition
    let run = dispatched.check_first_block().unwrap();
    task::yield_now().await;

    // addition should be complete, but went back to waiting on wait_on
    // thus, the permit should still be held
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert!(concurrency.try_acquire_owned().await.is_none());

    // unblock and yield, completing the task and releasing the permit
    let unblocked = run.check_first_unblock().unwrap();
    let second_block = unblocked.check_second_block().unwrap();
    task::yield_now().await;
    second_block.check_second_block().unwrap();

    // permit should now be available
    assert!(concurrency.try_acquire_owned().await.is_some());

    // clean up
    shutdown_cmd_tx.send(()).unwrap();
    join_cmd_rx.await.unwrap();
}

#[tokio::test]
async fn notif_rx_alone() {
    let (notif_tx, notif_rx) = mpsc::channel(1);
    let (shutdown_cmd_tx, shutdown_cmd_rx) = broadcast::channel(1);
    let (concurrency, state, _wait_on, count) = test_setup(1);

    let join_notif_rx = tokio::spawn(notif_rx_loop(
        notif_rx,
        state.clone(),
        concurrency.clone(),
        shutdown_cmd_rx,
    ));

    // sanity check initial state
    assert!(concurrency.try_acquire_all().await.is_some());
    assert!(state.try_read().is_ok());
    assert_eq!(count.load(Ordering::Relaxed), 0);

    notif_tx.send(()).await.unwrap();

    // yield so notif_rx_loop dequeues the notification and runs the entire handler synchronously
    task::yield_now().await;

    // should have seen the count be set to 5, with permit available
    assert_eq!(count.load(Ordering::Relaxed), 5);
    assert!(concurrency.try_acquire_all().await.is_some());

    // clean up
    shutdown_cmd_tx.send(()).unwrap();
    join_notif_rx.await.unwrap();
}

#[tokio::test]
// this monolith should exist as-is to show the sequence in which things happen
#[allow(clippy::too_many_lines)]
async fn notif_rx_with_cmd() {
    let (cmd_tx, cmd_rx) = mpsc::channel(5);
    let (notif_tx, notif_rx) = mpsc::channel(2);
    let (shutdown_cmd_tx, shutdown_cmd_rx) = broadcast::channel(2);
    let (concurrency, state, _wait_on, count) = test_setup(2);

    let join_cmd_rx = tokio::spawn(cmd_rx_loop(
        cmd_rx,
        concurrency.clone(),
        state.clone(),
        shutdown_cmd_rx,
    ));

    let join_notif_rx = tokio::spawn(notif_rx_loop(
        notif_rx,
        state.clone(),
        concurrency.clone(),
        shutdown_cmd_tx.subscribe(),
    ));

    // sanity check initial state
    assert!(concurrency.try_acquire_all().await.is_some());
    assert!(state.try_read().is_ok());
    assert_eq!(count.load(Ordering::Relaxed), 0);

    // create five calls
    let mut calls: Vec<MockCall<NotSent>> = vec![
        MockCall::new(8),
        MockCall::new(7),
        MockCall::new(6),
        MockCall::new(2),
        MockCall::new(1),
    ];
    let mut sent: Vec<MockCall<Dispatched>> = Vec::new();

    // fill the call channel with the first two calls
    for _ in 0..2 {
        sent.push(
            calls
                .pop()
                .unwrap()
                .send(&cmd_tx)
                .await
                .check_dispatched()
                .unwrap(),
        );
    }
    let call_1 = sent.pop().unwrap();
    let call_0 = sent.pop().unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 0);

    // yield: cmd_rx_loop dequeues both calls, acquires both permits, and spawns both tasks
    // 0 takes the read lock and blocks on its first wait;
    // 1 blocks on the read lock
    task::yield_now().await;

    // sanity check: both (all) permits held, count unmodified, read lock on state held
    assert_eq!(count.load(Ordering::Relaxed), 0);
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok());

    // step 0 past its first wait to increment count and block on its second wait
    let call_0 = call_0.check_first_block().unwrap();
    task::yield_now().await;

    // 0 has incremented, but not exited (blocking with read lock held)
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok());

    // complete 0: releases read lock and permit;
    // 1 takes the read lock and blocks on its first wait
    let call_0 = call_0.check_first_unblock().unwrap();
    let call_0 = call_0.check_second_block().unwrap();
    task::yield_now().await; // finish 0
    call_0.check_second_block().unwrap();
    task::yield_now().await; // yield to 1

    // 1 takes read lock and blocks on first wait
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert!(concurrency.try_acquire_owned().await.is_some()); // 0's permit freed
    assert!(state.try_read().is_ok()); // read lock held by 1

    // step 1 past its first wait to increment count and block on its second wait
    let call_1 = call_1.check_first_block().unwrap();
    task::yield_now().await;
    let call_1 = call_1.check_first_unblock().unwrap();
    let call_1 = call_1.check_second_block().unwrap();

    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert!(concurrency.try_acquire_all().await.is_none()); // 1 still holds permit
    assert!(concurrency.try_acquire_owned().await.is_some()); // 0's permit freed
    assert!(state.try_read().is_ok()); // read lock held by 1

    notif_tx.send(()).await.unwrap();
    // queue a notification before we let 1 finish, and also the remaining 3 calls.
    // the notification should be processed, setting count to 5 before any of the
    // last 3 calls are able to get permits
    let mut late: Vec<MockCall<Dispatched>> = Vec::new();
    for _ in 0..3 {
        late.push(
            calls
                .pop()
                .unwrap()
                .send(&cmd_tx)
                .await
                .check_dispatched()
                .unwrap(),
        );
    }
    let call_4 = late.pop().unwrap();
    let call_3 = late.pop().unwrap();
    let call_2 = late.pop().unwrap();

    task::yield_now().await;
    call_1.check_second_block().unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 5);
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_write().is_err());

    // yield: notif_rx_loop handles the notification synchronously (count := 5)
    // while holding all permits; then cmd_rx_loop dequeues 2 of the 3 calls
    // (2 permits) and blocks on the third
    task::yield_now().await;

    assert_eq!(count.load(Ordering::Relaxed), 5);
    // calls 2 and 3 dequeued
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok()); // no lock held yet

    // yield: call_2 takes the read lock and blocks on its first wait;
    // call_3 blocks on the read lock
    task::yield_now().await;

    assert_eq!(count.load(Ordering::Relaxed), 5);
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok()); // read lock held by call_2
    task::yield_now().await;

    // step call_2 past its first wait: count += 1, then blocks on second wait
    let call_2 = call_2.check_first_block().unwrap();
    task::yield_now().await;

    assert_eq!(count.load(Ordering::Relaxed), 6);
    assert!(concurrency.try_acquire_all().await.is_none());
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok());

    // complete call_2 (releases read lock and permit): cmd_rx_loop dequeues the
    // third call; call_3 takes the read lock and blocks on its first wait
    let call_2 = call_2.check_first_unblock().unwrap();
    let call_2 = call_2.check_second_block().unwrap();
    task::yield_now().await; // finish call_2
    call_2.check_second_block().unwrap();
    task::yield_now().await; // call_3 takes read lock; call_4 dequeued

    assert_eq!(count.load(Ordering::Relaxed), 6);
    assert!(concurrency.try_acquire_all().await.is_none()); // calls 3 & 4 hold permits
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok()); // read lock held by call_3

    // step call_3 past its first wait: count += 1, then blocks on second wait
    let call_3 = call_3.check_first_block().unwrap();
    task::yield_now().await;

    assert_eq!(count.load(Ordering::Relaxed), 7);
    assert!(concurrency.try_acquire_all().await.is_none());
    assert!(concurrency.try_acquire_owned().await.is_none());
    assert!(state.try_read().is_ok());

    // complete call_3 (releases read lock and permit);
    // call_4 takes the read lock and blocks on its first wait
    let call_3 = call_3.check_first_unblock().unwrap();
    let call_3 = call_3.check_second_block().unwrap();
    task::yield_now().await; // finish call_3
    call_3.check_second_block().unwrap();
    task::yield_now().await; // call_4 takes read lock, first wait

    assert_eq!(count.load(Ordering::Relaxed), 7);
    assert!(concurrency.try_acquire_all().await.is_none()); // call_4 holds permit
    assert!(concurrency.try_acquire_owned().await.is_some()); // call_3's permit freed
    assert!(state.try_read().is_ok()); // read lock held by call_4

    // step call_4 past its first wait: count += 1, then blocks on second wait
    let call_4 = call_4.check_first_block().unwrap();
    task::yield_now().await;

    assert_eq!(count.load(Ordering::Relaxed), 8);
    assert!(concurrency.try_acquire_all().await.is_none()); // call_4 holds permit
    assert!(concurrency.try_acquire_owned().await.is_some()); // call_3's permit freed
    assert!(state.try_read().is_ok());

    // complete call_4: read lock and all permits released
    let call_4 = call_4.check_first_unblock().unwrap();
    let call_4 = call_4.check_second_block().unwrap();
    task::yield_now().await;
    call_4.check_second_block().unwrap();

    assert_eq!(count.load(Ordering::Relaxed), 8);
    assert!(concurrency.try_acquire_all().await.is_some());
    assert!(concurrency.try_acquire_owned().await.is_some());
    assert!(state.try_read().is_ok());

    // clean up
    shutdown_cmd_tx.send(()).unwrap();
    join_cmd_rx.await.unwrap();
    join_notif_rx.await.unwrap();
}

#[tokio::test]
async fn internal_event_rx() {
    let (internal_event_tx, internal_event_rx) = mpsc::channel(2);
    let (shutdown_cmd_tx, shutdown_cmd_rx) = broadcast::channel(2);
    let (concurrency, state, _wait_on, count) = test_setup(2);

    let join_internal_events = task::spawn(internal_event_rx_loop(
        internal_event_rx,
        state,
        concurrency.clone(),
        shutdown_cmd_rx,
    ));

    assert!(concurrency.try_acquire_all().await.is_some());

    internal_event_tx.send(9).await.unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 0);
    task::yield_now().await;
    assert_eq!(count.load(Ordering::Relaxed), 9);
    assert!(concurrency.try_acquire_all().await.is_some());

    shutdown_cmd_tx.send(()).unwrap();
    join_internal_events.await.unwrap();
}

#[tokio::test]
async fn shutdown_rx() {
    let (shutdown_cmd_tx, shutdown_cmd_rx) = oneshot::channel();
    let (internal_shutdown_tx, mut internal_shutdown_rx) = broadcast::channel(2);
    let (_concurrency, state, _wait_on, _count) = test_setup(2);
    let alive = Arc::new(());

    let join_shutdown_hdlr = task::spawn(shutdown_handler(
        state,
        shutdown_cmd_rx,
        internal_shutdown_tx,
        alive,
        JoinSet::new(),
    ));

    let (shutdown_cmd_done_tx, shutdown_cmd_done_rx) =
        oneshot::channel::<<MockState as RouterBase>::ShutdownResult>();

    shutdown_cmd_tx.send(shutdown_cmd_done_tx).unwrap();
    internal_shutdown_rx.recv().await.unwrap();

    shutdown_cmd_done_rx.await.unwrap();

    join_shutdown_hdlr.await.unwrap();
}

#[tokio::test]
async fn test_start() {
    let (notif_tx, notif_rx) = mpsc::channel(2);
    let (_event_tx, event_rx) = mpsc::channel(2);
    let (_internal_event_tx, internal_event_rx) = mpsc::channel(2);

    let state = MockState {
        count: Arc::new(AtomicUsize::new(0)),
    };

    let (handle, _events) = start(state, 2, notif_tx, notif_rx, event_rx, internal_event_rx);

    handle.shutdown().await;
}
