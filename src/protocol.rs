//! [`ProtocolHandler`] implementation for the docs [`Engine`].

use std::sync::Arc;

use anyhow::Result;
use futures_lite::future::Boxed as BoxedFuture;
use iroh::{endpoint::Connecting, protocol::ProtocolHandler};

use crate::engine::Engine;

impl<D: iroh_blobs::store::Store> ProtocolHandler for Engine<D> {
    fn accept(self: Arc<Self>, conn: Connecting) -> BoxedFuture<Result<()>> {
        Box::pin(async move { self.handle_connection(conn).await })
    }

    fn shutdown(self: Arc<Self>) -> BoxedFuture<()> {
        Box::pin(async move {
            if let Err(err) = (*self).shutdown().await {
                tracing::warn!("shutdown error: {:?}", err);
            }
        })
    }
}
