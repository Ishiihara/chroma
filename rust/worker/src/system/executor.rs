use super::{
    scheduler::Scheduler,
    sender::{Sender, Wrapper},
    system::System,
    Component, Handler, HandlerWithResult,
};
use crate::system::dedicated_executor::DedicatedExecutor;
use crate::system::ComponentContext;
use std::sync::Arc;
use tokio::select;

struct Inner<C>
where
    C: Component,
{
    pub(super) sender: Sender<C>,
    pub(super) cancellation_token: tokio_util::sync::CancellationToken,
    pub(super) system: System,
    pub(super) scheduler: Scheduler,
}

#[derive(Clone)]
/// # Description
/// The executor holds the context for a components execution and is responsible for
/// running the components handler methods
pub(super) struct ComponentExecutor<C>
where
    C: Component,
{
    inner: Arc<Inner<C>>,
    handler: C,
}

impl<C> ComponentExecutor<C>
where
    C: Component + Send + 'static,
{
    pub(super) fn new(
        sender: Sender<C>,
        cancellation_token: tokio_util::sync::CancellationToken,
        handler: C,
        system: System,
        scheduler: Scheduler,
    ) -> Self {
        ComponentExecutor {
            inner: Arc::new(Inner {
                sender,
                cancellation_token,
                system,
                scheduler,
            }),
            handler,
        }
    }

    pub(super) async fn run(&mut self, mut channel: tokio::sync::mpsc::Receiver<Wrapper<C>>) {
        loop {
            select! {
                _ = self.inner.cancellation_token.cancelled() => {
                    break;
                }
                message = channel.recv() => {
                    match message {
                        Some(mut message) => {
                                message.handle(&mut self.handler,
                                    &ComponentContext{
                                        system: self.inner.system.clone(),
                                        sender: self.inner.sender.clone(),
                                        cancellation_token: self.inner.cancellation_token.clone(),
                                        scheduler: self.inner.scheduler.clone(),
                                    }
                                ).await;
                        }
                        None => {
                            // TODO: Log error
                        }
                    }
                }
            }
        }
    }

    pub(super) async fn invoke<M>(&mut self, message: M)
    where
        M: Send + 'static,
        C: Handler<M>,
    {
        let ctx = ComponentContext {
            system: self.inner.system.clone(),
            sender: self.inner.sender.clone(),
            cancellation_token: self.inner.cancellation_token.clone(),
            scheduler: self.inner.scheduler.clone(),
        };
        select! {
            _ = self.inner.cancellation_token.cancelled() => {
                return;
            }
            _ = self.handler.handle(message, &ctx) => {}
        }
    }

    pub(super) async fn invoke_with_send<M, T>(
        &mut self,
        message: M,
        sender: tokio::sync::mpsc::Sender<T>,
    ) where
        M: Send + 'static,
        C: HandlerWithResult<M, T>,
        T: Send + 'static,
    {
        let ctx = ComponentContext {
            system: self.inner.system.clone(),
            sender: self.inner.sender.clone(),
            cancellation_token: self.inner.cancellation_token.clone(),
            scheduler: self.inner.scheduler.clone(),
        };
        select! {
            _ = self.inner.cancellation_token.cancelled() => {
                return;
            }
            result = self.handler.handle(message, &ctx) => {
                let _ = sender.send(result).await;
            }
        }
    }
}
