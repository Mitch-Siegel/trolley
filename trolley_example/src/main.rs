use trolley::{Router, RouterBase, router::RouterPriv};

use std::sync::Arc;
use tokio::{sync::RwLock, task::JoinSet, time::Duration};

pub mod services {
    include!(concat!(env!("OUT_DIR"), "/trolley_call_ctx.rs"));
}
include!(concat!(env!("OUT_DIR"), "/trolley_calculator.rs"));

#[derive(Debug)]
pub enum AdditionError {
    BatteryDepleted,
    BatteryMissing,
}

#[trolley::interface]
impl Calculator {
    pub async fn add(&self, a: u32, b: u32) -> Result<u32, AdditionError> {
        {
            let mut maybe_battery = self.battery.write().await;
            if let Some(battery) = &mut *maybe_battery {
                if battery.charge == 0 {
                    return Err(AdditionError::BatteryDepleted);
                } else {
                    battery.charge -= 1;
                }
            } else {
                return Err(AdditionError::BatteryMissing);
            }
        }

        println!("starting addition");
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let result = a + b;
        println!("{}+{}={}", a, b, result);
        Ok(result)
    }

    pub async fn change_batteries(&mut self) {}
}

#[derive(Debug)]
pub struct Battery {
    charge: u8,
}

#[derive(Debug)]
#[trolley::router_meta(Calculator)]
pub struct CalculatorMeta {}

#[derive(Debug, Router)]
pub struct Calculator {
    battery: Arc<RwLock<Option<Battery>>>,
}

impl Calculator {
    pub fn new() -> Self {
        Self {
            battery: Arc::new(RwLock::new(Some(Battery { charge: 100 }))),
        }
    }
}

impl Default for Calculator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum CalculatorNotif {
    BatteryRemoved,
}

#[async_trait::async_trait]
impl RouterPriv for Calculator {
    type InternalEvent = ();
    type Meta = CalculatorMeta;

    async fn new(_meta: Self::Meta, _ctx: trolley::RouterCtx<Self>) -> Self {
        Self::default()
    }

    fn handle_internal_event(&self, internal: Self::InternalEvent) {
        unimplemented!("{:?}", internal);
    }
}

#[async_trait::async_trait]
impl RouterBase for Calculator {
    type Event = ();
    type Notif = CalculatorNotif;
    type ShutdownResult = Option<Battery>;

    fn handle_notification(&mut self, notif: Self::Notif) {
        match notif {
            Self::Notif::BatteryRemoved => *self.battery.blocking_write() = None,
        }
    }

    async fn shutdown(self) -> Self::ShutdownResult {
        Arc::try_unwrap(self.battery).unwrap().into_inner()
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("Hello, world!");

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let (handle, _events) = Calculator::spawn(CalculatorMeta {}, 3).await;

    let mut really_hard_tasks = JoinSet::new();

    for to_add in 0..10 {
        tracing::debug!("spawn task {}", to_add);
        let my_handle = handle.clone();
        really_hard_tasks.spawn(async move {
            my_handle
                .with_timeout(Duration::from_secs(5))
                .add(123, to_add)
                .await
        });
    }
    tracing::info!("done spawning tasks");

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    handle.0.notify(CalculatorNotif::BatteryRemoved).await;

    while let Some(res) = really_hard_tasks.join_next().await {
        tracing::info!("{:?}", res);
    }
}
