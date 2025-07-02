//! irpc-based RPC implementation for docs.

#![allow(missing_docs)]

pub(crate) mod actor;
mod api;
pub(crate) mod docs_handle_request;
pub mod protocol;

pub use self::api::*;

pub type RpcError = serde_error::Error;
pub type RpcResult<T> = std::result::Result<T, RpcError>;
