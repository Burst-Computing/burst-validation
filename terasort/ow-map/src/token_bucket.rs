use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{interval, Duration};

pub struct TokenBucket {
    sem: Arc<Semaphore>,
    jh: tokio::task::JoinHandle<()>,
}

impl TokenBucket {
    pub fn new(duration: Duration, capacity: usize) -> Self {
        let sem = Arc::new(Semaphore::new(capacity));

        // refills the tokens at the end of each interval
        let jh = tokio::spawn({
            let sem = sem.clone();
            let mut interval = interval(duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            async move {
                loop {
                    interval.tick().await;

                    if sem.available_permits() < capacity {
                        sem.add_permits(capacity - sem.available_permits());
                    }
                }
            }
        });

        Self { jh, sem }
    }

    pub async fn acquire(&self) {
        // This can return an error if the semaphore is closed, but we
        // never close it, so this error can never happen.
        let permit = self.sem.acquire().await.unwrap();
        // To avoid releasing the permit back to the semaphore, we use
        // the `SemaphorePermit::forget` method.
        permit.forget();
    }
}

impl Drop for TokenBucket {
    fn drop(&mut self) {
        // Kill the background task so it stops taking up resources when we
        // don't need it anymore.
        self.jh.abort();
    }
}
