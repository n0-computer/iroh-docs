//! [`ProtocolHandler`] implementation for the docs [`Engine`].

use anyhow::Result;
use futures_lite::future::Boxed as BoxedFuture;
use iroh::{endpoint::Connecting, protocol::ProtocolHandler};

use crate::engine::Engine;

impl<D: iroh_blobs::store::Store> ProtocolHandler for Engine<D> {
    fn accept(&self, conn: Connecting) -> BoxedFuture<Result<()>> {
        let this = self.clone();
        Box::pin(async move { this.handle_connection(conn).await })
    }

    fn shutdown(&self) -> BoxedFuture<()> {
        let this = self.clone();
        Box::pin(async move {
            if let Err(err) = this.shutdown().await {
                tracing::warn!("shutdown error: {:?}", err);
            }
        })
    }
}
