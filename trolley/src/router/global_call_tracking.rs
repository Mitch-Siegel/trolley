use std::{
    collections::{HashMap, VecDeque},
    sync::OnceLock,
    time,
};

use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub(crate) struct TrackedCall {
    pub name: String,
    pub start: time::Instant,
}

pub(crate) static GLOBAL_CALLS: OnceLock<RwLock<HashMap<String, VecDeque<TrackedCall>>>> =
    OnceLock::new();

macro_rules! _push_global_call {
    ($ty:ty, $val:expr) => {{
        let handle = crate::router::global_call_tracking::GLOBAL_CALLS
            .get_or_init(|| ::tokio::sync::RwLock::new(std::collections::HashMap::new()));

        let mut map = handle.write().await;
        let key = std::any::type_name::<$ty>().to_string();
        let entry = map.entry(key.clone()).or_default();
        entry.push_back(crate::router::global_call_tracking::TrackedCall {
            name: $val.into(),
            start: std::time::Instant::now(),
        });

        // let mut call_str = String::from("pushed call");
        // for call in entry {
        //     let TrackedCall { name, start: _ } = call;
        //     call_str += &format!("\n{}", name);
        // }
        // tracing::error!(call_str);
        tracing::trace!("{} calls pending for {}", entry.len(), key);

        drop(map);
    }};
}

macro_rules! _pop_global_call {
    ($ty:ty, $val:expr) => {{
        let end_time = std::time::Instant::now();
        let handle = crate::router::global_call_tracking::GLOBAL_CALLS
            .get_or_init(|| ::tokio::sync::RwLock::new(std::collections::HashMap::new()));

        let mut map = handle.write().await;
        let key = std::any::type_name::<$ty>().to_string();
        let entry = map.entry(key).or_default();
        let crate::router::global_call_tracking::TrackedCall { name, start } =
            entry.pop_front().expect("error tracking calls globally");
        tracing::trace!(
            "(call took {}ms) {} = {} - {} calls pending behind this",
            (end_time - start).as_millis(),
            name,
            $val,
            entry.len(),
        );

        drop(map);
    }};
}

macro_rules! _global_calls_for_state {
    ($ty:ty) => {{
        let handle = crate::router::global_call_tracking::GLOBAL_CALLS
            .get_or_init(|| crate::Handle::new(std::collections::HashMap::new()));

        let mut map = handle.write().await;
        let key = std::any::type_name::<$ty>().to_string();
        map.entry(key).or_default().clone()

        drop(map);
    }};
}
