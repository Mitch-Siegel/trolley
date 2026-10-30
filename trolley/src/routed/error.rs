use std::{error, fmt};

use tokio::{sync::oneshot, time::Duration};

#[derive(Debug)]
pub enum Error {
    RwLockTimeout(Duration),
    ExecuteTimeout(Duration, Duration),
    RouterShutdown,
    RecvError(oneshot::error::RecvError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RwLockTimeout(duration) => {
                write!(f, "timed out waiting for rwlock on state ({duration:?})")
            }
            Self::ExecuteTimeout(lock_duration, execution_duration) => write!(
                f,
                "timed out executing call ({lock_duration:?} spent acquiring lock, {execution_duration:?} spent in call)",
            ),
            Self::RouterShutdown => write!(f, "router was shut down"),
            Self::RecvError(e) => write!(f, "unable to receive response: {e}"),
        }
    }
}

impl error::Error for Error {}
