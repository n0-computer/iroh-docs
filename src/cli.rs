//! Define commands for interacting with documents in Iroh.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    rc::Rc,
    str::FromStr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use colored::Colorize;
use dialoguer::Confirm;
use futures_buffered::BufferedStreamExt;
use futures_lite::{Stream, StreamExt};
use indicatif::{HumanBytes, HumanDuration, MultiProgress, ProgressBar, ProgressStyle};
use iroh::{hash::Hash, AddrInfoOptions};
use iroh_base::base32::fmt_short;
use iroh_blobs::{
    provider::AddProgress,
    rpc::client::blobs::{self, WrapOption},
    util::{
        fs::{path_content_info, path_to_key, PathContent},
        SetTagOption,
    },
    Tag,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tracing::warn;

use crate::{
    engine::Origin,
    rpc::client::docs::{self, Doc, Entry, LiveEvent, ShareMode},
    store::{DownloadPolicy, FilterKind, Query, SortDirection},
    AuthorId, ContentStatus, DocTicket, NamespaceId,
};

pub mod authors;

type AuthorsClient = crate::rpc::client::authors::Client;

const ENV_AUTHOR: &str = "IROH_AUTHOR";
const ENV_DOC: &str = "IROH_DOC";

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum::AsRefStr, strum::EnumString, strum::Display)]
pub(crate) enum ConsolePaths {
    #[strum(serialize = "current-author")]
    CurrentAuthor,
    #[strum(serialize = "history")]
    History,
}

impl ConsolePaths {
    fn root(iroh_data_dir: impl AsRef<Path>) -> PathBuf {
        PathBuf::from(iroh_data_dir.as_ref()).join("console")
    }
    pub fn with_iroh_data_dir(self, iroh_data_dir: impl AsRef<Path>) -> PathBuf {
        Self::root(iroh_data_dir).join(self.as_ref())
    }
}

/// Environment for CLI and REPL
///
/// This is cheaply cloneable and has interior mutability. If not running in the console
/// environment, `Self::set_doc` and `Self::set_author` will lead to an error, as changing the
/// environment is only supported within the console.
#[derive(Clone, Debug)]
pub struct ConsoleEnv(Arc<RwLock<ConsoleEnvInner>>);

#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, Clone)]
struct ConsoleEnvInner {
    /// Active author. Read from IROH_AUTHOR env variable.
    /// For console also read from/persisted to a file.
    /// Defaults to the node's default author if both are empty.
    author: AuthorId,
    /// Active doc. Read from IROH_DOC env variable. Not persisted.
    doc: Option<NamespaceId>,
    is_console: bool,
    iroh_data_dir: PathBuf,
}

impl ConsoleEnv {
    /// Read from environment variables and the console config file.
    pub async fn for_console(
        iroh_data_dir: PathBuf,
        authors: &crate::rpc::client::authors::Client,
    ) -> Result<Self> {
        let console_data_dir = ConsolePaths::root(&iroh_data_dir);
        tokio::fs::create_dir_all(&console_data_dir)
            .await
            .with_context(|| {
                format!(
                    "failed to create console data directory at `{}`",
                    console_data_dir.to_string_lossy()
                )
            })?;

        Self::migrate_console_files_016_017(&iroh_data_dir).await?;

        let configured_author = Self::get_console_default_author(&iroh_data_dir)?;
        let author = env_author(configured_author, authors).await?;
        let env = ConsoleEnvInner {
            author,
            doc: env_doc()?,
            is_console: true,
            iroh_data_dir,
        };
        Ok(Self(Arc::new(RwLock::new(env))))
    }

    /// Read only from environment variables.
    pub async fn for_cli(iroh_data_dir: PathBuf, authors: &AuthorsClient) -> Result<Self> {
        let author = env_author(None, authors).await?;
        let env = ConsoleEnvInner {
            author,
            doc: env_doc()?,
            is_console: false,
            iroh_data_dir,
        };
        Ok(Self(Arc::new(RwLock::new(env))))
    }

    fn get_console_default_author(iroh_data_root: &Path) -> anyhow::Result<Option<AuthorId>> {
        let author_path = ConsolePaths::CurrentAuthor.with_iroh_data_dir(iroh_data_root);
        if let Ok(s) = std::fs::read_to_string(&author_path) {
            let author = AuthorId::from_str(&s).with_context(|| {
                format!(
                    "Failed to parse author file at {}",
                    author_path.to_string_lossy()
                )
            })?;
            Ok(Some(author))
        } else {
            Ok(None)
        }
    }

    /// True if running in a Iroh console session, false for a CLI command
    pub(crate) fn is_console(&self) -> bool {
        self.0.read().unwrap().is_console
    }

    /// Return the iroh data directory
    pub fn iroh_data_dir(&self) -> PathBuf {
        self.0.read().unwrap().iroh_data_dir.clone()
    }

    /// Set the active author.
    ///
    /// Will error if not running in the Iroh console.
    /// Will persist to a file in the Iroh data dir otherwise.
    pub(crate) fn set_author(&self, author: AuthorId) -> anyhow::Result<()> {
        let author_path = ConsolePaths::CurrentAuthor.with_iroh_data_dir(self.iroh_data_dir());
        let mut inner = self.0.write().unwrap();
        if !inner.is_console {
            bail!("Switching the author is only supported within the Iroh console, not on the command line");
        }
        inner.author = author;
        std::fs::write(author_path, author.to_string().as_bytes())?;
        Ok(())
    }

    /// Set the active document.
    ///
    /// Will error if not running in the Iroh console.
    /// Will not persist, only valid for the current console session.
    pub(crate) fn set_doc(&self, doc: NamespaceId) -> anyhow::Result<()> {
        let mut inner = self.0.write().unwrap();
        if !inner.is_console {
            bail!("Switching the document is only supported within the Iroh console, not on the command line");
        }
        inner.doc = Some(doc);
        Ok(())
    }

    /// Get the active document.
    pub fn doc(&self, arg: Option<NamespaceId>) -> anyhow::Result<NamespaceId> {
        let inner = self.0.read().unwrap();
        let doc_id = arg.or(inner.doc).ok_or_else(|| {
            anyhow!(
                "Missing document id. Set the active document with the `IROH_DOC` environment variable or the `-d` option.\n\
                In the console, you can also set the active document with `doc switch`."
            )
        })?;
        Ok(doc_id)
    }

    /// Get the active author.
    ///
    /// This is either the node's default author, or in the console optionally the author manually
    /// switched to.
    pub fn author(&self) -> AuthorId {
        let inner = self.0.read().unwrap();
        inner.author
    }

    pub(crate) async fn migrate_console_files_016_017(iroh_data_dir: &Path) -> Result<()> {
        // In iroh up to 0.16, we stored console settings directly in the data directory. Starting
        // from 0.17, they live in a subdirectory and have new paths.
        let old_current_author = iroh_data_dir.join("default_author.pubkey");
        if old_current_author.is_file() {
            if let Err(err) = tokio::fs::rename(
                &old_current_author,
                ConsolePaths::CurrentAuthor.with_iroh_data_dir(iroh_data_dir),
            )
            .await
            {
                warn!(path=%old_current_author.to_string_lossy(), "failed to migrate the console's current author file: {err}");
            }
        }
        let old_history = iroh_data_dir.join("history");
        if old_history.is_file() {
            if let Err(err) = tokio::fs::rename(
                &old_history,
                ConsolePaths::History.with_iroh_data_dir(iroh_data_dir),
            )
            .await
            {
                warn!(path=%old_history.to_string_lossy(), "failed to migrate the console's history file: {err}");
            }
        }
        Ok(())
    }
}

async fn env_author(from_config: Option<AuthorId>, authors: &AuthorsClient) -> Result<AuthorId> {
    if let Some(author) = env::var(ENV_AUTHOR)
        .ok()
        .map(|s| {
            s.parse()
                .context("Failed to parse IROH_AUTHOR environment variable")
        })
        .transpose()?
        .or(from_config)
    {
        Ok(author)
    } else {
        authors.default().await
    }
}

fn env_doc() -> Result<Option<NamespaceId>> {
    env::var(ENV_DOC)
        .ok()
        .map(|s| {
            s.parse()
                .context("Failed to parse IROH_DOC environment variable")
        })
        .transpose()
}

/// The maximum length of content to display before truncating.
const MAX_DISPLAY_CONTENT_LEN: u64 = 80;

/// Different modes to display content.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum DisplayContentMode {
    /// Displays the content if small enough, otherwise it displays the content hash.
    Auto,
    /// Display the content unconditionally.
    Content,
    /// Display the hash of the content.
    Hash,
    /// Display the shortened hash of the content.
    ShortHash,
}

/// General download policy for a document.
#[derive(Debug, Clone, Copy, clap::ValueEnum, derive_more::Display)]
pub enum FetchKind {
    /// Download everything in this document.
    Everything,
    /// Download nothing in this document.
    Nothing,
}

#[allow(missing_docs)]
/// Subcommands for the download policy command.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum DlPolicyCmd {
    Set {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Set the general download policy for this document.
        kind: FetchKind,
        /// Add an exception to the download policy.
        /// An exception must be formatted as `<matching_kind>:<encoding>:<pattern>`.
        ///
        /// - <matching_kind> can be either `prefix` or `exact`.
        ///
        /// - `<encoding>` can be either `utf8` or `hex`.
        #[clap(short, long, value_name = "matching_kind>:<encoding>:<pattern")]
        except: Vec<FilterKind>,
    },
    Get {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
    },
}

/// Possible `Document` commands.
#[allow(missing_docs)]
#[derive(Debug, Clone, Parser)]
pub enum DocCommands {
    /// Set the active document (only works within the Iroh console).
    Switch { id: NamespaceId },
    /// Create a new document.
    Create {
        /// Switch to the created document (only in the Iroh console).
        #[clap(long)]
        switch: bool,
    },
    /// Join a document from a ticket.
    Join {
        ticket: DocTicket,
        /// Switch to the joined document (only in the Iroh console).
        #[clap(long)]
        switch: bool,
    },
    /// List documents.
    List,
    /// Share a document with peers.
    Share {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// The sharing mode.
        mode: ShareMode,
        /// Options to configure the address information in the generated ticket.
        ///
        /// Use `relay-and-addresses` in networks with no internet connectivity.
        #[clap(long, default_value_t = AddrInfoOptions::Id)]
        addr_options: AddrInfoOptions,
    },
    /// Set an entry in a document.
    Set {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Author of the entry.
        ///
        /// Required unless the author is set through the IROH_AUTHOR environment variable.
        /// Within the Iroh console, the active author can also set with `author switch`.
        #[clap(long)]
        author: Option<AuthorId>,
        /// Key to the entry (parsed as UTF-8 string).
        key: String,
        /// Content to store for this entry (parsed as UTF-8 string)
        value: String,
    },
    /// Set the download policies for a document.
    #[clap(subcommand)]
    DlPolicy(DlPolicyCmd),
    /// Get entries in a document.
    ///
    /// Shows the author, content hash and content length for all entries for this key.
    Get {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Key to the entry (parsed as UTF-8 string).
        key: String,
        /// If true, get all entries that start with KEY.
        #[clap(short, long)]
        prefix: bool,
        /// Filter by author.
        #[clap(long)]
        author: Option<AuthorId>,
        /// How to show the contents of the key.
        #[clap(short, long, value_enum, default_value_t=DisplayContentMode::Auto)]
        mode: DisplayContentMode,
    },
    /// Delete all entries below a key prefix.
    Del {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Author of the entry.
        ///
        /// Required unless the author is set through the IROH_AUTHOR environment variable.
        /// Within the Iroh console, the active author can also set with `author switch`.
        #[clap(long)]
        author: Option<AuthorId>,
        /// Prefix to delete. All entries whose key starts with or is equal to the prefix will be
        /// deleted.
        prefix: String,
    },
    /// List all keys in a document.
    #[clap(alias = "ls")]
    Keys {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Filter by author.
        #[clap(long)]
        author: Option<AuthorId>,
        /// Optional key prefix (parsed as UTF-8 string)
        prefix: Option<String>,
        /// How to sort the entries
        #[clap(long, default_value_t=Sorting::Author)]
        sort: Sorting,
        /// Sort in descending order
        #[clap(long)]
        desc: bool,
        /// How to show the contents of the keys.
        #[clap(short, long, value_enum, default_value_t=DisplayContentMode::ShortHash)]
        mode: DisplayContentMode,
    },
    /// Import data into a document
    Import {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also be set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Author of the entry.
        ///
        /// Required unless the author is set through the IROH_AUTHOR environment variable.
        /// Within the Iroh console, the active author can also be set with `author switch`.
        #[clap(long)]
        author: Option<AuthorId>,
        /// Prefix to add to imported entries (parsed as UTF-8 string). Defaults to no prefix
        #[clap(long)]
        prefix: Option<String>,
        /// Path to a local file or directory to import
        ///
        /// Pathnames will be used as the document key
        path: String,
        /// If true, don't copy the file into iroh, reference the existing file instead
        ///
        /// Moving a file imported with `in-place` will result in data corruption
        #[clap(short, long)]
        in_place: bool,
        /// When true, you will not get a prompt to confirm you want to import the files
        #[clap(long, default_value_t = false)]
        no_prompt: bool,
    },
    /// Export the most recent data for a key from a document
    Export {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also be set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Key to the entry (parsed as UTF-8 string)
        ///
        /// When just the key is present, will export the latest entry for that key.
        key: String,
        /// Path to export to
        #[clap(short, long)]
        out: String,
    },
    /// Watch for changes and events on a document
    Watch {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
    },
    /// Stop syncing a document.
    Leave {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        doc: Option<NamespaceId>,
    },
    /// Delete a document from the local node.
    ///
    /// This is a destructive operation. Both the document secret key and all entries in the
    /// document will be permanently deleted from the node's storage. Content blobs will be deleted
    /// through garbage collection unless they are referenced from another document or tag.
    Drop {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        doc: Option<NamespaceId>,
    },
}

/// How to sort.
#[derive(clap::ValueEnum, Clone, Debug, Default, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Sorting {
    /// Sort by author, then key
    #[default]
    Author,
    /// Sort by key, then author
    Key,
}

impl From<Sorting> for crate::store::SortBy {
    fn from(value: Sorting) -> Self {
        match value {
            Sorting::Author => Self::AuthorKey,
            Sorting::Key => Self::KeyAuthor,
        }
    }
}

impl DocCommands {
    /// Runs the document command given the iroh client and the console environment.
    pub async fn run(
        self,
        docs: &docs::Client,
        blobs: &blobs::Client,
        env: &ConsoleEnv,
    ) -> Result<()> {
        match self {
            Self::Switch { id: doc } => {
                env.set_doc(doc)?;
                println!("Active doc is now {}", fmt_short(doc.as_bytes()));
            }
            Self::Create { switch } => {
                if switch && !env.is_console() {
                    bail!("The --switch flag is only supported within the Iroh console.");
                }

                let doc = docs.create().await?;
                println!("{}", doc.id());

                if switch {
                    env.set_doc(doc.id())?;
                    println!("Active doc is now {}", fmt_short(doc.id().as_bytes()));
                }
            }
            Self::Join { ticket, switch } => {
                if switch && !env.is_console() {
                    bail!("The --switch flag is only supported within the Iroh console.");
                }

                let doc = docs.import(ticket).await?;
                println!("{}", doc.id());

                if switch {
                    env.set_doc(doc.id())?;
                    println!("Active doc is now {}", fmt_short(doc.id().as_bytes()));
                }
            }
            Self::List => {
                let mut stream = docs.list().await?;
                while let Some((id, kind)) = stream.try_next().await? {
                    println!("{id} {kind}")
                }
            }
            Self::Share {
                doc,
                mode,
                addr_options,
            } => {
                let doc = get_doc(docs, env, doc).await?;
                let ticket = doc.share(mode, addr_options).await?;
                println!("{}", ticket);
            }
            Self::Set {
                doc,
                author,
                key,
                value,
            } => {
                let doc = get_doc(docs, env, doc).await?;
                let author = author.unwrap_or(env.author());
                let key = key.as_bytes().to_vec();
                let value = value.as_bytes().to_vec();
                let hash = doc.set_bytes(author, key, value).await?;
                println!("{}", hash);
            }
            Self::Del {
                doc,
                author,
                prefix,
            } => {
                let doc = get_doc(docs, env, doc).await?;
                let author = author.unwrap_or(env.author());
                let prompt =
                    format!("Deleting all entries whose key starts with {prefix}. Continue?");
                if Confirm::new()
                    .with_prompt(prompt)
                    .interact()
                    .unwrap_or(false)
                {
                    let key = prefix.as_bytes().to_vec();
                    let removed = doc.del(author, key).await?;
                    println!("Deleted {removed} entries.");
                    println!(
                        "Inserted an empty entry for author {} with key {prefix}.",
                        fmt_short(author)
                    );
                } else {
                    println!("Aborted.")
                }
            }
            Self::Get {
                doc,
                key,
                prefix,
                author,
                mode,
            } => {
                let doc = get_doc(docs, env, doc).await?;
                let key = key.as_bytes().to_vec();
                let query = Query::all();
                let query = match (author, prefix) {
                    (None, false) => query.key_exact(key),
                    (None, true) => query.key_prefix(key),
                    (Some(author), true) => query.author(author).key_prefix(key),
                    (Some(author), false) => query.author(author).key_exact(key),
                };

                let mut stream = doc.get_many(query).await?;
                while let Some(entry) = stream.try_next().await? {
                    println!("{}", fmt_entry(blobs, &entry, mode).await);
                }
            }
            Self::Keys {
                doc,
                prefix,
                author,
                mode,
                sort,
                desc,
            } => {
                let doc = get_doc(docs, env, doc).await?;
                let mut query = Query::all();
                if let Some(author) = author {
                    query = query.author(author);
                }
                if let Some(prefix) = prefix {
                    query = query.key_prefix(prefix);
                }
                let direction = match desc {
                    true => SortDirection::Desc,
                    false => SortDirection::Asc,
                };
                query = query.sort_by(sort.into(), direction);
                let mut stream = doc.get_many(query).await?;
                while let Some(entry) = stream.try_next().await? {
                    println!("{}", fmt_entry(blobs, &entry, mode).await);
                }
            }
            Self::Leave { doc } => {
                let doc = get_doc(docs, env, doc).await?;
                doc.leave().await?;
                println!("Doc {} is now inactive", fmt_short(doc.id()));
            }
            Self::Import {
                doc,
                author,
                prefix,
                path,
                in_place,
                no_prompt,
            } => {
                let doc = get_doc(docs, env, doc).await?;
                let author = author.unwrap_or(env.author());
                let mut prefix = prefix.unwrap_or_else(|| String::from(""));

                if prefix.ends_with('/') {
                    prefix.pop();
                }
                let root = canonicalize_path(&path)?.canonicalize()?;
                let tag = tag_from_file_name(&root)?;

                let root0 = root.clone();
                println!("Preparing import...");
                // get information about the directory or file we are trying to import
                // and confirm with the user that they still want to import the file
                let PathContent { size, files } =
                    tokio::task::spawn_blocking(|| path_content_info(root0)).await??;
                if !no_prompt {
                    let prompt = format!("Import {files} files totaling {}?", HumanBytes(size));
                    if !Confirm::new()
                        .with_prompt(prompt)
                        .interact()
                        .unwrap_or(false)
                    {
                        println!("Aborted.");
                        return Ok(());
                    } else {
                        print!("\r");
                    }
                }

                let stream = blobs
                    .add_from_path(
                        root.clone(),
                        in_place,
                        SetTagOption::Named(tag.clone()),
                        WrapOption::NoWrap,
                    )
                    .await?;
                let root_prefix = match root.parent() {
                    Some(p) => p.to_path_buf(),
                    None => PathBuf::new(),
                };
                let start = Instant::now();
                import_coordinator(doc, author, root_prefix, prefix, stream, size, files).await?;
                println!("Success! ({})", HumanDuration(start.elapsed()));
            }
            Self::Export { doc, key, out } => {
                let doc = get_doc(docs, env, doc).await?;
                let key_str = key.clone();
                let key = key.as_bytes().to_vec();
                let path: PathBuf = canonicalize_path(&out)?;
                let mut stream = doc.get_many(Query::key_exact(key)).await?;
                let entry = match stream.try_next().await? {
                    None => {
                        println!("<unable to find entry for key {key_str}>");
                        return Ok(());
                    }
                    Some(e) => e,
                };
                match blobs.read(entry.content_hash()).await {
                    Ok(mut content) => {
                        if let Some(dir) = path.parent() {
                            if let Err(err) = std::fs::create_dir_all(dir) {
                                println!(
                                    "<unable to create directory for {}: {err}>",
                                    path.display()
                                );
                            }
                        };
                        let pb = ProgressBar::new(content.size());
                        pb.set_style(ProgressStyle::default_bar()
                                .template("{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})").unwrap()
                                .progress_chars("=>-"));
                        let file = tokio::fs::File::create(path.clone()).await?;
                        if let Err(err) =
                            tokio::io::copy(&mut content, &mut pb.wrap_async_write(file)).await
                        {
                            pb.finish_and_clear();
                            println!("<unable to write to file {}: {err}>", path.display())
                        } else {
                            pb.finish_and_clear();
                            println!("wrote '{key_str}' to {}", path.display());
                        }
                    }
                    Err(err) => println!("<failed to get content: {err}>"),
                }
            }
            Self::Watch { doc } => {
                let doc = get_doc(docs, env, doc).await?;
                let mut stream = doc.subscribe().await?;
                while let Some(event) = stream.next().await {
                    let event = event?;
                    match event {
                        LiveEvent::InsertLocal { entry } => {
                            println!(
                                "local change:  {}",
                                fmt_entry(blobs, &entry, DisplayContentMode::Auto).await
                            )
                        }
                        LiveEvent::InsertRemote {
                            entry,
                            from,
                            content_status,
                        } => {
                            let content = match content_status {
                                ContentStatus::Complete => {
                                    fmt_entry(blobs, &entry, DisplayContentMode::Auto).await
                                }
                                ContentStatus::Incomplete => {
                                    let (Ok(content) | Err(content)) =
                                        fmt_content(blobs, &entry, DisplayContentMode::ShortHash)
                                            .await;
                                    format!("<incomplete: {} ({})>", content, human_len(&entry))
                                }
                                ContentStatus::Missing => {
                                    let (Ok(content) | Err(content)) =
                                        fmt_content(blobs, &entry, DisplayContentMode::ShortHash)
                                            .await;
                                    format!("<missing: {} ({})>", content, human_len(&entry))
                                }
                            };
                            println!(
                                "remote change via @{}: {}",
                                fmt_short(from.as_bytes()),
                                content
                            )
                        }
                        LiveEvent::ContentReady { hash } => {
                            println!("content ready: {}", fmt_short(hash.as_bytes()))
                        }
                        LiveEvent::SyncFinished(event) => {
                            let origin = match event.origin {
                                Origin::Accept => "they initiated",
                                Origin::Connect(_) => "we initiated",
                            };
                            match event.result {
                                Ok(details) => {
                                    println!(
                                        "synced peer {} ({origin}, received {}, sent {}",
                                        fmt_short(event.peer),
                                        details.entries_received,
                                        details.entries_sent
                                    )
                                }
                                Err(err) => println!(
                                    "failed to sync with peer {} ({origin}): {err}",
                                    fmt_short(event.peer)
                                ),
                            }
                        }
                        LiveEvent::NeighborUp(peer) => {
                            println!("neighbor peer up: {peer:?}");
                        }
                        LiveEvent::NeighborDown(peer) => {
                            println!("neighbor peer down: {peer:?}");
                        }
                        LiveEvent::PendingContentReady => {
                            println!("all pending content is now ready")
                        }
                    }
                }
            }
            Self::Drop { doc } => {
                let doc = get_doc(docs, env, doc).await?;
                println!(
                    "Deleting a document will permanently remove the document secret key, all document entries, \n\
                    and all content blobs which are not referenced from other docs or tags."
                );
                let prompt = format!("Delete document {}?", fmt_short(doc.id()));
                if Confirm::new()
                    .with_prompt(prompt)
                    .interact()
                    .unwrap_or(false)
                {
                    docs.drop_doc(doc.id()).await?;
                    println!("Doc {} has been deleted.", fmt_short(doc.id()));
                } else {
                    println!("Aborted.")
                }
            }
            Self::DlPolicy(DlPolicyCmd::Set { doc, kind, except }) => {
                let doc = get_doc(docs, env, doc).await?;
                let download_policy = match kind {
                    FetchKind::Everything => DownloadPolicy::EverythingExcept(except),
                    FetchKind::Nothing => DownloadPolicy::NothingExcept(except),
                };
                if let Err(e) = doc.set_download_policy(download_policy).await {
                    println!("Could not set the document's download policy. {e}")
                }
            }
            Self::DlPolicy(DlPolicyCmd::Get { doc }) => {
                let doc = get_doc(docs, env, doc).await?;
                match doc.get_download_policy().await {
                    Ok(dl_policy) => {
                        let (kind, exceptions) = match dl_policy {
                            DownloadPolicy::NothingExcept(exceptions) => {
                                (FetchKind::Nothing, exceptions)
                            }
                            DownloadPolicy::EverythingExcept(exceptions) => {
                                (FetchKind::Everything, exceptions)
                            }
                        };
                        println!("Download {kind} in this document.");
                        if !exceptions.is_empty() {
                            println!("Exceptions:");
                            for exception in exceptions {
                                println!("{exception}")
                            }
                        }
                    }
                    Err(x) => {
                        println!("Could not get the document's download policy: {x}")
                    }
                }
            }
        }
        Ok(())
    }
}

/// Gets the document given the client, the environment (and maybe the [`crate::keys::NamespaceId`]).
async fn get_doc(
    docs: &docs::Client,
    env: &ConsoleEnv,
    id: Option<NamespaceId>,
) -> anyhow::Result<Doc> {
    let doc_id = env.doc(id)?;
    docs.open(doc_id).await?.context("Document not found")
}

/// Formats the content. If an error occurs it's returned in a formatted, friendly way.
async fn fmt_content(
    blobs: &blobs::Client,
    entry: &Entry,
    mode: DisplayContentMode,
) -> Result<String, String> {
    let read_failed = |err: anyhow::Error| format!("<failed to get content: {err}>");
    let encode_hex = |err: std::string::FromUtf8Error| format!("0x{}", hex::encode(err.as_bytes()));
    let as_utf8 = |buf: Vec<u8>| String::from_utf8(buf).map(|repr| format!("\"{repr}\""));

    match mode {
        DisplayContentMode::Auto => {
            if entry.content_len() < MAX_DISPLAY_CONTENT_LEN {
                // small content: read fully as UTF-8
                let bytes = blobs
                    .read_to_bytes(entry.content_hash())
                    .await
                    .map_err(read_failed)?;
                Ok(as_utf8(bytes.into()).unwrap_or_else(encode_hex))
            } else {
                // large content: read just the first part as UTF-8
                let mut blob_reader = blobs
                    .read(entry.content_hash())
                    .await
                    .map_err(read_failed)?;
                let mut buf = Vec::with_capacity(MAX_DISPLAY_CONTENT_LEN as usize + 5);

                blob_reader
                    .read_buf(&mut buf)
                    .await
                    .map_err(|io_err| read_failed(io_err.into()))?;
                let mut repr = as_utf8(buf).unwrap_or_else(encode_hex);
                // let users know this is not shown in full
                repr.push_str("...");
                Ok(repr)
            }
        }
        DisplayContentMode::Content => {
            // read fully as UTF-8
            let bytes = blobs
                .read_to_bytes(entry.content_hash())
                .await
                .map_err(read_failed)?;
            Ok(as_utf8(bytes.into()).unwrap_or_else(encode_hex))
        }
        DisplayContentMode::ShortHash => {
            let hash = entry.content_hash();
            Ok(fmt_short(hash.as_bytes()))
        }
        DisplayContentMode::Hash => {
            let hash = entry.content_hash();
            Ok(hash.to_string())
        }
    }
}

/// Converts the [`Entry`] to human-readable bytes.
fn human_len(entry: &Entry) -> HumanBytes {
    HumanBytes(entry.content_len())
}

/// Formats an entry for display as a `String`.
#[must_use = "this won't be printed, you need to print it yourself"]
async fn fmt_entry(blobs: &blobs::Client, entry: &Entry, mode: DisplayContentMode) -> String {
    let key = std::str::from_utf8(entry.key())
        .unwrap_or("<bad key>")
        .bold();
    let author = fmt_short(entry.author());
    let (Ok(content) | Err(content)) = fmt_content(blobs, entry, mode).await;
    let len = human_len(entry);
    format!("@{author}: {key} = {content} ({len})")
}

/// Converts a path to a canonical path.
fn canonicalize_path(path: &str) -> anyhow::Result<PathBuf> {
    let path = PathBuf::from(shellexpand::tilde(&path).to_string());
    Ok(path)
}

/// Creates a [`Tag`] from a file name (given as a [`Path`]).
fn tag_from_file_name(path: &Path) -> anyhow::Result<Tag> {
    match path.file_name() {
        Some(name) => name
            .to_os_string()
            .into_string()
            .map(|t| t.into())
            .map_err(|e| anyhow!("{e:?} contains invalid Unicode")),
        None => bail!("the given `path` does not have a proper directory or file name"),
    }
}

/// Takes the `BlobsClient::add_from_path` and coordinates adding blobs to a
/// document via the hash of the blob. It also creates and powers the
/// `ImportProgressBar`.
#[tracing::instrument(skip_all)]
async fn import_coordinator(
    doc: Doc,
    author_id: AuthorId,
    root: PathBuf,
    prefix: String,
    blob_add_progress: impl Stream<Item = Result<AddProgress>> + Send + Unpin + 'static,
    expected_size: u64,
    expected_entries: u64,
) -> Result<()> {
    let imp = ImportProgressBar::new(
        &root.display().to_string(),
        doc.id(),
        expected_size,
        expected_entries,
    );
    let task_imp = imp.clone();

    let collections = Rc::new(RefCell::new(BTreeMap::<
        u64,
        (String, u64, Option<Hash>, u64),
    >::new()));

    let doc2 = doc.clone();
    let imp2 = task_imp.clone();

    let _stats: Vec<_> = blob_add_progress
        .filter_map(|item| {
            let item = match item.context("Error adding files") {
                Err(e) => return Some(Err(e)),
                Ok(item) => item,
            };
            match item {
                AddProgress::Found { name, id, size } => {
                    tracing::info!("Found({id},{name},{size})");
                    imp.add_found(name.clone(), size);
                    collections.borrow_mut().insert(id, (name, size, None, 0));
                    None
                }
                AddProgress::Progress { id, offset } => {
                    tracing::info!("Progress({id}, {offset})");
                    if let Some((_, size, _, last_val)) = collections.borrow_mut().get_mut(&id) {
                        assert!(*last_val <= offset, "wtf");
                        assert!(offset <= *size, "wtf2");
                        imp.add_progress(offset - *last_val);
                        *last_val = offset;
                    }
                    None
                }
                AddProgress::Done { hash, id } => {
                    tracing::info!("Done({id},{hash:?})");
                    match collections.borrow_mut().get_mut(&id) {
                        Some((path_str, size, ref mut h, last_val)) => {
                            imp.add_progress(*size - *last_val);
                            imp.import_found(path_str.clone());
                            let path = PathBuf::from(path_str.clone());
                            *h = Some(hash);
                            let key =
                                match path_to_key(path, Some(prefix.clone()), Some(root.clone())) {
                                    Ok(k) => k.to_vec(),
                                    Err(e) => {
                                        tracing::info!(
                                            "error getting key from {}, id {id}",
                                            path_str
                                        );
                                        return Some(Err(anyhow::anyhow!(
                                            "Issue creating a key for entry {hash:?}: {e}"
                                        )));
                                    }
                                };
                            // send update to doc
                            tracing::info!(
                                "setting entry {} (id: {id}) to doc",
                                String::from_utf8(key.clone()).unwrap()
                            );
                            Some(Ok((key, hash, *size)))
                        }
                        None => {
                            tracing::info!(
                                "error: got `AddProgress::Done` for unknown collection id {id}"
                            );
                            Some(Err(anyhow::anyhow!(
                                "Received progress information on an unknown file."
                            )))
                        }
                    }
                }
                AddProgress::AllDone { hash, .. } => {
                    imp.add_done();
                    tracing::info!("AddProgress::AllDone({hash:?})");
                    None
                }
                AddProgress::Abort(e) => {
                    tracing::info!("Error while adding data: {e}");
                    Some(Err(anyhow::anyhow!("Error while adding files: {e}")))
                }
            }
        })
        .map(move |res| {
            let doc = doc2.clone();
            let imp = imp2.clone();
            async move {
                match res {
                    Ok((key, hash, size)) => {
                        let doc = doc.clone();
                        doc.set_hash(author_id, key, hash, size).await?;
                        imp.import_progress();
                        Ok(size)
                    }
                    Err(err) => Err(err),
                }
            }
        })
        .buffered_unordered(128)
        .try_collect()
        .await?;

    task_imp.all_done();
    Ok(())
}

/// Progress bar for importing files.
#[derive(Debug, Clone)]
struct ImportProgressBar {
    mp: MultiProgress,
    import: ProgressBar,
    add: ProgressBar,
}

impl ImportProgressBar {
    /// Creates a new import progress bar.
    fn new(source: &str, doc_id: NamespaceId, expected_size: u64, expected_entries: u64) -> Self {
        let mp = MultiProgress::new();
        let add = mp.add(ProgressBar::new(0));
        add.set_style(ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})").unwrap()
            .progress_chars("=>-"));
        add.set_message(format!("Importing from {source}..."));
        add.set_length(expected_size);
        add.set_position(0);
        add.enable_steady_tick(Duration::from_millis(500));

        let doc_id = fmt_short(doc_id.to_bytes());
        let import = mp.add(ProgressBar::new(0));
        import.set_style(ProgressStyle::default_bar()
            .template("{msg}\n{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}, eta {eta})").unwrap()
            .progress_chars("=>-"));
        import.set_message(format!("Adding to doc {doc_id}..."));
        import.set_length(expected_entries);
        import.set_position(0);
        import.enable_steady_tick(Duration::from_millis(500));

        Self { mp, import, add }
    }

    fn add_found(&self, _name: String, _size: u64) {}

    fn import_found(&self, _name: String) {}

    /// Marks having made some progress to the progress bar.
    fn add_progress(&self, size: u64) {
        self.add.inc(size);
    }

    /// Marks having made one unit of progress on the import progress bar.
    fn import_progress(&self) {
        self.import.inc(1);
    }

    /// Sets the `add` progress bar as completed.
    fn add_done(&self) {
        self.add.set_position(self.add.length().unwrap_or_default());
    }

    /// Sets the all progress bars as done.
    fn all_done(self) {
        self.mp.clear().ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    #[allow(unused_variables, unreachable_code, clippy::diverging_sub_expression)]
    async fn test_doc_import() -> Result<()> {
        let temp_dir = tempfile::tempdir().context("tempdir")?;

        tokio::fs::create_dir_all(temp_dir.path())
            .await
            .context("create dir all")?;

        let foobar = temp_dir.path().join("foobar");
        tokio::fs::write(foobar, "foobar")
            .await
            .context("write foobar")?;
        let foo = temp_dir.path().join("foo");
        tokio::fs::write(foo, "foo").await.context("write foo")?;

        let data_dir = tempfile::tempdir()?;

        //  let node = crate::commands::start::start_node(data_dir.path(), None, None).await?;
        // let node = todo!();
        // let client = node.client();
        let docs: docs::Client = todo!();
        let authors = docs.authors();
        let doc = docs.create().await.context("doc create")?;
        let author = authors.create().await.context("author create")?;

        // set up command, getting iroh node
        let cli = ConsoleEnv::for_console(data_dir.path().to_owned(), &authors)
            .await
            .context("ConsoleEnv")?;
        // let iroh = iroh::client::Iroh::connect_path(data_dir.path())
        //     .await
        //     .context("rpc connect")?;
        // let iroh = todo!();
        let docs = todo!();
        let blobs = todo!();

        let command = DocCommands::Import {
            doc: Some(doc.id()),
            author: Some(author),
            prefix: None,
            path: temp_dir.path().to_string_lossy().into(),
            in_place: false,
            no_prompt: true,
        };

        command
            .run(&docs, &blobs, &cli)
            .await
            .context("DocCommands run")?;

        let keys: Vec<_> = doc
            .get_many(Query::all())
            .await
            .context("doc get many")?
            .try_collect()
            .await?;
        assert_eq!(2, keys.len());

        // todo
        // iroh.shutdown(false).await?;
        Ok(())
    }
}
