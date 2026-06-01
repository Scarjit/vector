pub mod config;
pub mod data;
pub mod limiter;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub use config::BatchConfig;
use futures::{
    Future, StreamExt,
    stream::{Fuse, Stream},
};
use pin_project::pin_project;
use tokio::time::Sleep;
use tracing::{debug, trace};

#[pin_project]
pub struct Batcher<S, C> {
    state: C,

    #[pin]
    /// The stream this `Batcher` wraps
    stream: Fuse<S>,

    #[pin]
    timer: Maybe<Sleep>,
}

/// An `Option`, but with pin projection
#[pin_project(project = MaybeProj)]
pub enum Maybe<T> {
    Some(#[pin] T),
    None,
}

impl<S, C> Batcher<S, C>
where
    S: Stream,
    C: BatchConfig<S::Item>,
{
    pub fn new(stream: S, config: C) -> Self {
        Self {
            state: config,
            stream: stream.fuse(),
            timer: Maybe::None,
        }
    }
}

impl<S, C> Stream for Batcher<S, C>
where
    S: Stream,
    C: BatchConfig<S::Item>,
{
    type Item = C::Batch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();
            trace!(message = "Batcher: polling inner stream.", batch_len = this.state.len());
            match this.stream.poll_next(cx) {
                Poll::Ready(None) => {
                    trace!(message = "Batcher: inner stream closed.");
                    return {
                        if this.state.len() == 0 {
                            debug!(message = "Batcher: stream closed, no pending items. Finishing.");
                            Poll::Ready(None)
                        } else {
                            debug!(
                                message = "Batcher: stream closed, flushing remaining batch.",
                                batch_len = this.state.len(),
                            );
                            Poll::Ready(Some(this.state.take_batch()))
                        }
                    };
                }
                Poll::Ready(Some(item)) => {
                    let (item_fits, item_metadata) = this.state.item_fits_in_batch(&item);
                    trace!(
                        message = "Batcher: received item from stream.",
                        item_fits,
                        current_batch_len = this.state.len(),
                    );
                    if item_fits {
                        this.state.push(item, item_metadata);
                        trace!(message = "Batcher: item pushed.", new_batch_len = this.state.len());
                        if this.state.is_batch_full() {
                            debug!(
                                message = "Batcher: batch full after push, emitting batch.",
                                batch_len = this.state.len(),
                            );
                            this.timer.set(Maybe::None);
                            return Poll::Ready(Some(this.state.take_batch()));
                        } else if this.state.len() == 1 {
                            debug!(
                                message = "Batcher: first item in batch, starting timeout.",
                                timeout = ?this.state.timeout(),
                            );
                            this.timer
                                .set(Maybe::Some(tokio::time::sleep(this.state.timeout())));
                        }
                    } else {
                        debug!(
                            message = "Batcher: item does not fit, emitting current batch and starting new one.",
                            current_batch_len = this.state.len(),
                        );
                        let output = Poll::Ready(Some(this.state.take_batch()));
                        this.state.push(item, item_metadata);
                        trace!(
                            message = "Batcher: item pushed into new batch, resetting timer.",
                            timeout = ?this.state.timeout(),
                        );
                        this.timer
                            .set(Maybe::Some(tokio::time::sleep(this.state.timeout())));
                        // Poll the new timer immediately so its waker is registered with
                        // Tokio's timer wheel before we return.  Without this, if the
                        // caller takes longer than timeout to call poll_next again (e.g.
                        // due to downstream backpressure), the timer fires with no waker
                        // registered and the lone batch stalls until the next event.
                        if let MaybeProj::Some(timer) = this.timer.as_mut().project() {
                            let _ = timer.poll(cx);
                        }
                        return output;
                    }
                }
                Poll::Pending => {
                    trace!(
                        message = "Batcher: inner stream pending.",
                        batch_len = this.state.len(),
                        has_timer = matches!(*this.timer.as_ref(), Maybe::Some(_)),
                    );
                    return {
                        if let MaybeProj::Some(timer) = this.timer.as_mut().project() {
                            match timer.poll(cx) {
                                Poll::Ready(()) => {
                                    debug!(
                                        message = "Batcher: timeout fired, emitting batch.",
                                        batch_len = this.state.len(),
                                    );
                                    this.timer.set(Maybe::None);
                                    debug_assert!(
                                        this.state.len() != 0,
                                        "timer should have been cancelled"
                                    );
                                    Poll::Ready(Some(this.state.take_batch()))
                                }
                                Poll::Pending => {
                                    trace!(message = "Batcher: timer still pending, waiting.");
                                    Poll::Pending
                                }
                            }
                        } else {
                            trace!(message = "Batcher: no timer and stream pending, parking.");
                            Poll::Pending
                        }
                    };
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[cfg(test)]
#[allow(clippy::similar_names)]
mod test {
    use std::{num::NonZeroUsize, time::Duration};

    use futures::stream;

    use super::*;
    use crate::BatcherSettings;

    #[tokio::test]
    async fn item_limit() {
        let stream = stream::iter([1, 2, 3]);
        let settings = BatcherSettings::new(
            Duration::from_millis(100),
            NonZeroUsize::new(10000).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        );
        let batcher = Batcher::new(stream, settings.as_item_size_config(|x: &u32| *x as usize));
        let batches: Vec<_> = batcher.collect().await;
        assert_eq!(batches, vec![vec![1, 2], vec![3],]);
    }

    #[tokio::test]
    async fn size_limit() {
        let batcher = Batcher::new(
            stream::iter([1, 2, 3, 4, 5, 6, 2, 3, 1]),
            BatcherSettings::new(
                Duration::from_millis(100),
                NonZeroUsize::new(5).unwrap(),
                NonZeroUsize::new(100).unwrap(),
            )
            .as_item_size_config(|x: &u32| *x as usize),
        );
        let batches: Vec<_> = batcher.collect().await;
        assert_eq!(
            batches,
            vec![
                vec![1, 2],
                vec![3],
                vec![4],
                vec![5],
                vec![6],
                vec![2, 3],
                vec![1],
            ]
        );
    }

    #[tokio::test]
    async fn timeout_limit() {
        tokio::time::pause();

        let timeout = Duration::from_millis(100);
        let stream = stream::iter([1, 2]).chain(stream::pending());
        let batcher = Batcher::new(
            stream,
            BatcherSettings::new(
                timeout,
                NonZeroUsize::new(5).unwrap(),
                NonZeroUsize::new(100).unwrap(),
            )
            .as_item_size_config(|x: &u32| *x as usize),
        );

        tokio::pin!(batcher);
        let mut next = batcher.next();
        assert_eq!(futures::poll!(&mut next), Poll::Pending);
        tokio::time::advance(timeout).await;
        let batch = next.await;
        assert_eq!(batch, Some(vec![1, 2]));
    }
}
