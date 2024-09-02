use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Sink;
use pin_project_lite::pin_project;
use stable_eyre::eyre::{eyre, Report};
use tokio::sync::broadcast::Sender;

pin_project! {
    #[derive(Clone)]
    pub struct BroadcastSink<T> {
        #[pin]
        sender: Sender<T>,
    }
}

impl<T> BroadcastSink<T> {
    pub fn new(sender: Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T> Sink<T> for BroadcastSink<T> {
    type Error = Report;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // A broadcast channel is always ready to send
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.project();
        this.sender
            .send(item)
            .map(|_it| ())
            .map_err(|_| eyre!("Could not send to channel"))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // A broadcast channel doesn't need to be flushed
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // We don't close the broadcast channel when the Sink is dropped
        Poll::Ready(Ok(()))
    }
}
