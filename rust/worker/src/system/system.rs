use std::fmt::Debug;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use tokio::runtime::Builder;
use tokio::{pin, select};

use super::dedicated_executor::Job;
use super::dedicated_executor::JobError;
use super::scheduler::Scheduler;
use super::sender::{self, Sender, Wrapper};
use super::ComponentRuntime;
use super::JobHandle;
use super::{executor, ComponentContext};
use super::{executor::ComponentExecutor, Component, ComponentHandle, Handler, StreamHandler};

#[derive(Clone)]
pub(crate) struct System {
    inner: Arc<Inner>,
}

struct Inner {
    scheduler: Scheduler,
}

impl System {
    pub(crate) fn new() -> System {
        System {
            inner: Arc::new(Inner {
                scheduler: Scheduler::new(),
            }),
        }
    }

    pub(crate) fn start_component<C>(&mut self, mut component: C) -> ComponentHandle<C>
    where
        C: Component + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(component.queue_size());
        let sender = Sender::new(tx);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let _ = component.on_start(&mut ComponentContext {
            system: self.clone(),
            sender: sender.clone(),
            cancellation_token: cancel_token.clone(),
            scheduler: self.inner.scheduler.clone(),
        });
        let mut executor = ComponentExecutor::new(
            sender.clone(),
            cancel_token.clone(),
            component,
            self.clone(),
            self.inner.scheduler.clone(),
        );

        match C::runtime() {
            ComponentRuntime::Global => {
                let join_handle: tokio::task::JoinHandle<()> =
                    tokio::spawn(async move { executor.run(rx).await });
                ComponentHandle::new(cancel_token, Some(join_handle), sender)
            }
            ComponentRuntime::Inherit => {
                let join_handle: tokio::task::JoinHandle<()> =
                    tokio::spawn(async move { executor.run(rx).await });
                ComponentHandle::new(cancel_token, Some(join_handle), sender)
            }
            ComponentRuntime::Dedicated => {
                println!("Spawning on dedicated thread");
                // Spawn on a dedicated thread
                let mut rt = Builder::new_current_thread().enable_all().build().unwrap();
                let join_handle = std::thread::spawn(move || {
                    rt.block_on(async move { executor.run(rx).await });
                });
                // TODO: Implement Join for dedicated threads
                ComponentHandle::new(cancel_token, None, sender)
            }
            ComponentRuntime::MultiThread => {
                println!("Spawning on dedicated thread");
                // Spawn on a dedicated thread
                let mut rt = Builder::new_multi_thread().enable_all().build().unwrap();
                let join_handle = std::thread::spawn(move || {
                    rt.block_on(async move { executor.run(rx).await });
                });
                // TODO: Implement Join for dedicated threads
                ComponentHandle::new(cancel_token, None, sender)
            }
        }
    }

    pub(crate) fn invoke<C, T>(&self, mut component: C) -> JobHandle<T>
    where
        C: Component<Output = Result<T, JobError>> + Send + 'static,
        T: Send + Debug + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(component.queue_size());
        let sender = Sender::new(tx);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        let mut ctx = ComponentContext {
            system: self.clone(),
            sender: sender.clone(),
            cancellation_token: cancel_token_clone,
            scheduler: self.inner.scheduler.clone(),
        };
        let _ = component.on_start(&mut ctx);
        match C::runtime() {
            ComponentRuntime::Global => {
                let cancel_clone = cancel_token.clone();
                let (tx, rx) = tokio::sync::oneshot::channel();
                let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => {}
                        result = component.run_task(&ctx) => {
                            match tx.send(result) {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("Failed to send result: {:?}", e);
                                }
                            }
                        }
                    }
                });
                let job = Job::new(rx, cancel_token.clone());

                JobHandle::new(cancel_token, Some(join_handle), job)
            }
            ComponentRuntime::Inherit => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let cancel_clone = cancel_token.clone();
                let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => {}
                        _ = component.run_task(&ctx) => {}
                    }
                });
                let job = Job::new(rx, cancel_token.clone());
                JobHandle::new(cancel_token, Some(join_handle), job)
            }
            ComponentRuntime::Dedicated => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let cancel_clone = cancel_token.clone();
                println!("Spawning on dedicated thread");
                // Spawn on a dedicated thread
                let mut rt = Builder::new_current_thread().enable_all().build().unwrap();
                let join_handle = std::thread::spawn(move || {
                    rt.block_on(async move {
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = cancel_clone.cancelled() => {}
                                _ = component.run_task(&ctx) => {}
                            }
                        });
                    });
                });
                // TODO: Implement Join for dedicated threads
                let job = Job::new(rx, cancel_token.clone());
                JobHandle::new(cancel_token, None, job)
            }
            ComponentRuntime::MultiThread => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let cancel_clone = cancel_token.clone();
                println!("Spawning on dedicated thread");
                // Spawn on a dedicated thread
                let mut rt = Builder::new_current_thread().enable_all().build().unwrap();
                let join_handle = std::thread::spawn(move || {
                    rt.block_on(async move {
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = cancel_clone.cancelled() => {}
                                _ = component.run_task(&ctx) => {}
                            }
                        });
                    });
                });
                // TODO: Implement Join for dedicated threads
                let job = Job::new(rx, cancel_token.clone());
                JobHandle::new(cancel_token, None, job)
            }
        }
    }

    pub(crate) fn invoke_component<C, M>(&self, mut component: C, message: M) -> ComponentHandle<C>
    where
        C: Handler<M> + Component + Send + 'static,
        M: Send + Debug + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(component.queue_size());
        let sender = Sender::new(tx);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let _ = component.on_start(&mut ComponentContext {
            system: self.clone(),
            sender: sender.clone(),
            cancellation_token: cancel_token.clone(),
            scheduler: self.inner.scheduler.clone(),
        });
        let mut executor = ComponentExecutor::new(
            sender.clone(),
            cancel_token.clone(),
            component,
            self.clone(),
            self.inner.scheduler.clone(),
        );
        match C::runtime() {
            ComponentRuntime::Global => {
                let join_handle: tokio::task::JoinHandle<()> =
                    tokio::spawn(async move { executor.invoke(message).await });
                ComponentHandle::new(cancel_token, Some(join_handle), sender)
            }
            ComponentRuntime::Inherit => {
                let join_handle: tokio::task::JoinHandle<()> =
                    tokio::spawn(async move { executor.invoke(message).await });
                ComponentHandle::new(cancel_token, Some(join_handle), sender)
            }
            ComponentRuntime::Dedicated => {
                println!("Spawning on dedicated thread");
                // Spawn on a dedicated thread
                let mut rt = Builder::new_current_thread().enable_all().build().unwrap();
                let join_handle = std::thread::spawn(move || {
                    rt.block_on(async move { executor.invoke(message).await });
                });
                // TODO: Implement Join for dedicated threads
                ComponentHandle::new(cancel_token, None, sender)
            }
            ComponentRuntime::MultiThread => {
                println!("Spawning on dedicated thread");
                // Spawn on a dedicated thread
                let mut rt = Builder::new_multi_thread().enable_all().build().unwrap();
                let join_handle = std::thread::spawn(move || {
                    rt.block_on(async move { executor.invoke(message).await });
                });
                // TODO: Implement Join for dedicated threads
                ComponentHandle::new(cancel_token, None, sender)
            }
        }
    }

    pub(super) fn register_stream<C, S, M>(&self, stream: S, ctx: &ComponentContext<C>)
    where
        C: StreamHandler<M> + Handler<M>,
        M: Send + Debug + 'static,
        S: Stream + Send + Stream<Item = M> + 'static,
    {
        let ctx = ComponentContext {
            system: self.clone(),
            sender: ctx.sender.clone(),
            cancellation_token: ctx.cancellation_token.clone(),
            scheduler: ctx.scheduler.clone(),
        };
        tokio::spawn(async move { stream_loop(stream, &ctx).await });
    }

    pub(crate) async fn stop(&self) {
        self.inner.scheduler.stop();
    }

    pub(crate) async fn join(&self) {
        self.inner.scheduler.join().await;
    }
}

async fn stream_loop<C, S, M>(stream: S, ctx: &ComponentContext<C>)
where
    C: StreamHandler<M> + Handler<M>,
    M: Send + Debug + 'static,
    S: Stream + Send + Stream<Item = M> + 'static,
{
    pin!(stream);
    loop {
        select! {
            _ = ctx.cancellation_token.cancelled() => {
                break;
            }
            message = stream.next() => {
                match message {
                    Some(message) => {
                        let res = ctx.sender.send(message).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Failed to send message: {:?}", e);
                                // TODO: switch to logging
                                // Terminate the stream
                                break;
                            }
                        }
                    },
                    None => {
                        break;
                    }
                }
            }
        }
    }
}
