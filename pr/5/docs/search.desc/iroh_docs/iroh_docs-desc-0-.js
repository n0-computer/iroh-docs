searchState.loadedDescShard("iroh_docs", 0, "Multi-dimensional key-value documents with an efficient …\nAuthor key to insert entries in a <code>crate::Replica</code>\nTimestamps of the latest entry for each author.\n<code>AuthorPublicKey</code> in bytes\nIdentifier for an <code>Author</code>\nContains both a key (either secret or public) to a …\n<code>NamespacePublicKey</code> in bytes\nThe corresponding <code>VerifyingKey</code> for a <code>NamespaceSecret</code>. It …\nNamespace key of a <code>crate::Replica</code>.\nThis contains an actor spawned on a separate thread to …\nGet the byte representation of this <code>AuthorId</code>.\nGet the byte representation of this <code>NamespaceId</code>.\nConvert to byte slice.\nConvert to byte slice.\neither a public or private key\nHandlers and actors to for live syncing replicas.\nConvert to a base32 string limited to the first 10 bytes …\nConvert to a base32 string limited to the first 10 bytes …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCreate an <code>Author</code> from a byte array.\nCreate from a slice of bytes.\nCreate a <code>NamespaceSecret</code> from a byte array.\nCreate from a slice of bytes.\nGet the <code>AuthorId</code> for this author.\nGet the <code>NamespaceId</code> for this namespace.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nConvert into <code>NamespacePublicKey</code>.\nConvert into <code>AuthorPublicKey</code>.\nMetrics for iroh-docs\nNetwork implementation of the iroh-docs protocol\nCreate a new doc ticket\nCreate a new <code>Author</code> with a random key.\nCreate a new <code>NamespaceSecret</code> with a random key.\nA list of nodes to contact.\n<code>ProtocolHandler</code> implementation for the docs <code>Engine</code>.\nGet the <code>AuthorPublicKey</code> for this author.\nGet the <code>NamespacePublicKey</code> for this namespace.\nConvert into <code>NamespacePublicKey</code> by fetching from a …\nConvert into <code>AuthorPublicKey</code> by fetching from a …\nQuic RPC implementation for docs.\nSign a message with this <code>Author</code> key.\nSign a message with this <code>NamespaceSecret</code> key.\nStorage trait and implementation for iroh-docs documents\nAPI for iroh-docs replicas\nReturns the <code>Author</code> byte representation.\nReturns the <code>NamespaceSecret</code> byte representation.\nConvert to byte array.\nConvert to byte array.\nStrictly verify a signature on a message with this <code>Author</code>…\nVerify that a signature matches the <code>msg</code> bytes and was …\nStrictly verify a signature on a message with this …\nVerify that a signature matches the <code>msg</code> bytes and was …\nOptions when opening a replica.\nThe state for an open replica.\nThe <code>SyncHandle</code> controls an actor thread which executes …\nMakes sure that all pending database operations are …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nBy how many handles the replica is currently held open\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nSpawn a sync actor and return a handle.\nSubscribe to replica events.\nOptionally subscribe to replica events.\nHow many event subscriptions are open\nSet sync state to true.\nWhether to accept sync requests for this replica.\nSet to true to set sync state to true.\nA node connected to us and we accepted the exchange\nWe initiated the exchange\nThe content of an entry was downloaded and is now …\nPersistent default author for a docs engine.\nWhere to persist the default author.\nDirect join request via API\nThe sync engine coordinates actors that manage open …\nA local insertion.\nReceived a remote insert.\nEvents informing about actions of the live sync progress.\nMemory storage.\nWe lost a neighbor in the swarm.\nWe have a new neighbor in the swarm.\nPeer showed up as new neighbor in the gossip swarm\nWhy we performed a sync exchange\nAll pending content is now ready.\nFile based persistent storage.\nWe received a sync report while a sync was running, so run …\nEvent emitted when a sync operation completes\nA set-reconciliation sync finished.\nWhy we started a sync request\nWe synced after receiving a sync report that indicated …\nGet the blob store.\nThe persistent default author for this engine.\n<code>Endpoint</code> used by the engine.\nConverts an <code>EntryStatus</code> into a [‘ContentStatus’].\nTimestamp when the sync started\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nGet the current default author.\nHandle an incoming iroh-docs connection.\nHandle a docs request from the RPC server.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nStop the live sync for a document and leave the gossip …\nLoad the default author from the storage.\nLoad the default author from storage.\nOrigin of the sync exchange\nPeer we synced with\nSave a new default author.\nResult of the sync operation\nSet the default author.\nShutdown the engine.\nStart the sync engine.\nStart to sync a document.\nTimestamp when the sync finished\nSubscribe to replica and sync progress events.\nHandle to the actor thread.\nIf the content is available at the local node\nThe inserted entry.\nThe inserted entry.\nThe peer that sent us the entry.\nThe content hash of the newly available entry content\nMetrics for iroh-docs\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nWe aborted the sync request.\nReason why we aborted an incoming sync request.\nErrors that may occur on handling incoming sync …\nWhether we want to accept or reject an incoming sync …\nAccept the sync request.\nWe are already syncing this namespace.\nFailed to close\nFailed to close\nFailed to establish connection\nFailed to establish connection\nErrors that may occur on outgoing sync requests.\nThe ALPN identifier for the iroh-docs protocol\nWe experienced an error while trying to provide the …\nNamespace is not available.\nFailed to open replica\nDecline the sync request\nThe remote peer aborted the sync request.\nFailed to run sync\nFailed to run sync\nDetails of a finished sync operation.\nTime a sync operation took\nTime to establish connection\nConnect to a peer and sync a replica\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nHandle an iroh-docs connection and sync all shared …\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nGet the namespace (if available)\nThe namespace that was synced.\nThe outcome of the sync operation\nGet the peer’s node ID (if available)\nThe peer we syned with.\nTime to run sync exchange\nThe time this operation took\nRPC Client for docs and authors\nProtocol definitions for RPC.\nAPI for document management.\nAPI for document management.\nA node connected to us and we accepted the exchange\nIroh docs client.\nWe initiated the exchange\nDirect join request via API\nPeer showed up as new neighbor in the gossip swarm\nWhy we performed a sync exchange\nWe received a sync report while a sync was running, so run …\nEvent emitted when a sync operation completes\nWhy we started a sync request\nWe synced after receiving a sync report that indicated …\nCreates a new document author.\nReturns the default document author of this node.\nDeletes the given author by id.\nExports the given author.\nTimestamp when the sync started\nReturns the argument unchanged.\nImports the given author.\nCalls <code>U::from(self)</code>.\nLists document authors for which we have a secret key.\nCreates a new docs client.\nOrigin of the sync exchange\nPeer we synced with\nResult of the sync operation\nSets the node-wide default author.\nTimestamp when the sync finished\nWe got an error and need to abort.\nA node connected to us and we accepted the exchange\nWe are done setting the entry to the doc.\nIroh docs client.\nWe initiated the exchange\nThe content of an entry was downloaded and is now …\nDirect join request via API\nDocument handle\nA single entry in a <code>Doc</code>.\nOutcome of a <code>Doc::export_file</code> operation\nProgress stream for <code>Doc::export_file</code>.\nAn item was found with name <code>name</code>, from now on referred to …\nOutcome of a <code>Doc::import_file</code> operation\nProgress stream for <code>Doc::import_file</code>.\nProgress messages for an doc import operation\nWe are done adding <code>id</code> to the data store and the hash is …\nA local insertion.\nReceived a remote insert.\nEvents informing about actions of the live sync progress.\nWe lost a neighbor in the swarm.\nWe have a new neighbor in the swarm.\nPeer showed up as new neighbor in the gossip swarm\nWhy we performed a sync exchange\nAll pending content is now ready.\nWe got progress ingesting item <code>id</code>.\nRead-only access\nWe received a sync report while a sync was running, so run …\nIntended capability for document share tickets\nEvent emitted when a sync operation completes\nA set-reconciliation sync finished.\nWhy we started a sync request\nWe synced after receiving a sync report that indicated …\nWrite access\nReturns the <code>AuthorId</code> of this entry.\nCloses the document.\nReturns the <code>Hash</code> of the content data of this record.\nReturns the length of the data addressed by this record’…\nCreates a client.\nDeletes entries that match the given <code>author</code> and key <code>prefix</code>.\nDeletes a document from the local node.\nExports an entry as a file to a given absolute path.\nFinishes writing the stream, ignoring all intermediate …\nIterates through the export progress stream, returning …\nTimestamp when the sync started\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the download policy for this document\nReturns an entry for a key and author.\nReturns all entries matching the query.\nReturns a single entry.\nReturns sync peers for this document\nThe hash of the entry’s content\nReturns the document id of this doc.\nReturns the <code>RecordIdentifier</code> for this entry.\nImports a document from a ticket and joins all peers in …\nImports a document from a ticket, creates a subscription …\nAdds an entry from an absolute file path\nImports a document from a namespace capability.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns the key of this entry.\nThe key of the entry\nStops the live sync for this document.\nLists all documents.\nCreates a new docs client.\nReturns a <code>Doc</code> client for a single document.\nOrigin of the sync exchange\nThe path to which the entry was saved\nPeer we synced with\nResult of the sync operation\nSets the content of a key to a byte array.\nSets the download policy for this document\nSets an entries on the doc via its key, hash, and size.\nShares this document with peers over a ticket.\nThe size of the entry\nThe size of the entry\nStarts to sync this document with a list of peers.\nTimestamp when the sync finished\nReturns status info for this document\nSubscribes to events for this document.\nReturns the timestamp of this entry.\nThe hash of the entry.\nA new unique id for this entry.\nThe unique id of the entry.\nThe unique id of the entry.\nThe key of the entry\nThe name of the entry.\nThe offset of the progress, in bytes.\nThe size of the entry in bytes.\nIf the content is available at the local node\nThe inserted entry.\nThe inserted entry.\nThe peer that sent us the entry.\nThe content hash of the newly available entry content\nCreate a new document author.\nResponse for <code>AuthorCreateRequest</code>\nDelete an author\nResponse for <code>AuthorDeleteRequest</code>\nExports an author\nResponse for <code>AuthorExportRequest</code>\nGet the default author.\nResponse for <code>AuthorGetDefaultRequest</code>\nImport author from secret key\nResponse to <code>ImportRequest</code>\nList document authors for which we have a secret key.\nResponse for <code>AuthorListRequest</code>\nSet the default author.\nResponse for <code>AuthorSetDefaultRequest</code>\nOpen a document\nResponse to <code>CloseRequest</code>\nCreate a new document\nResponse to <code>CreateRequest</code>\nDelete entries in a document\nResponse to <code>DelRequest</code>\nList all documents\nSubscribe to events for a document.\nResponse to <code>DocSubscribeRequest</code>\nStop the live sync for a doc, and optionally delete the …\nResponse to <code>DropRequest</code>\nA request to the node to save the data of the entry to the …\nProgress messages for an doc export operation\nGet a download policy\nResponse to <code>GetDownloadPolicyRequest</code>\nGet entries from a document\nResponse to <code>GetExactRequest</code>\nGet entries from a document\nResponse to <code>GetManyRequest</code>\nGet peers for document\nResponse to <code>GetSyncPeersRequest</code>\nA request to the node to add the data at the given …\nWrapper around <code>ImportProgress</code>.\nImport a document from a capability.\nResponse to <code>ImportRequest</code>\nStop the live sync for a doc, and optionally delete the …\nResponse to <code>LeaveRequest</code>\nResponse to <code>DocListRequest</code>\nOpen a document\nResponse to <code>OpenRequest</code>\nThe RPC service type for the docs protocol.\nSet a download policy\nResponse to <code>SetDownloadPolicyRequest</code>\nSet an entry in a document via its hash\nResponse to <code>SetHashRequest</code>\nSet an entry in a document\nResponse to <code>SetRequest</code>\nShare a document with peers over a ticket.\nThe response to <code>ShareRequest</code>\nStart to sync a doc with peers.\nResponse to <code>StartSyncRequest</code>\nGet info on a document\nResponse to <code>StatusRequest</code>\nConfiguration of the addresses in the ticket.\nAuthor matcher\nThe id of the author to delete\nThe id of the author to delete\nThe author\nThe author to import\nAuthor of this entry.\nAuthor of this entry.\nAuthor of this entry.\nAuthor of this entry.\nThe author id\nThe id of the created author\nThe id of the author\nThe id of the author\nThe author id of the imported author\nThe capability over the document.\nThe namespace capability.\nThe document id\nthe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id.\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe document id\nThe newly-created entry.\nThe entry you want to export\nThe document entry\nThe document entry\nThe event that occurred on the document\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nHash of this entry.\nThe document id\nThe document id\nTrue if the provider can assume that the data will not …\nWhether to include empty entries (prefix deletion markers)\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nKey of this entry.\nKey of this entry.\nKey of this entry.\nKey matcher\nWhether to share read or write access to the document\nThe mode of exporting. Setting to <code>ExportMode::TryReference</code> …\nThe filepath to the data\nThe filepath to where the data should be saved\nList of peers to join\nList of peers ids\nDownload policy\nThe download policy\nPrefix to delete.\nQuery to run\nThe number of entries that were removed.\nSize of this entry.\nLive sync status\nValue of this entry.\nMatches any key.\nMatches any author.\nSort ascending\nAuthor matching.\nSort by author, then key.\nSort descending\nDownload policy to decide which content blobs shall be …\nStore that gives read access to download policies for a …\nDownload every key unless it matches one of the filters.\nMatches if the contained bytes and the key are the same.\nOnly keys that are exactly the provided value.\nMatches exactly the provided author.\nFilter strategy used in download policies.\nQuery on all entries without aggregation.\nOutcome of <code>Store::import_namespace</code>\nThe namespace did not exist before and is now inserted.\nSort by key, then author.\nKey matching.\nIn-memory key storage\nThe namespace existed and its capability remains unchanged.\nThe replica does not exist.\nDo not download any key unless it matches one of the …\nError return from <code>Store::open_replica</code>\nOther error while opening the replica.\nMatches if the contained bytes are a prefix of the key.\nAll keys that start with the provided value.\nStore trait for expanded public keys for authors and …\nNote: When using the <code>SingleLatestPerKey</code> query kind, the …\nA query builder for document queries.\nQuery that only returns the latest entry for a key which …\nFields by which the query can be sorted\nSort direction\nThe namespace existed and now has an upgraded capability.\nQuery all records.\nFilter by author.\nCreate a <code>Query::all</code> query filtered by a single author.\nConvert a <code>AuthorId</code> into a <code>AuthorPublicKey</code>.\nConvert a <code>AuthorId</code> into a <code>AuthorPublicKey</code>.\nBuild the query.\nBuild the query.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nOn disk storage for replicas.\nGet the download policy for a document.\nCall to include empty entries (deletion markers).\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nFilter by exact key match.\nCreate a <code>Query::all</code> query filtered by a single key.\nFilter by key prefix.\nCreate a <code>Query::all</code> query filtered by a key prefix.\nSet the maximum number of entries to be returned.\nGet the limit for this query (max. number of entries to …\nCheck if an entry should be downloaded according to this …\nVerifies whether this filter matches a given key\nTest if a key is matched by this <code>KeyFilter</code>.\nTest if an author is matched by this <code>AuthorFilter</code>.\nConvert a <code>NamespaceId</code> into a <code>NamespacePublicKey</code>.\nConvert a <code>NamespaceId</code> into a <code>NamespacePublicKey</code>.\nSet the offset within the result set from where to start …\nGet the offset for this query (number of entries to skip …\nConvert a byte array into a  <code>VerifyingKey</code>.\nQuery only the latest entry for each key, omitting older …\nSet the sort for the query.\nSet the order direction for the query.\nIterator for all content hashes\nIterator over the latest entry per author.\nIterator over parent entries, i.e. entries with the same …\nAn iterator over a range of entries from the records table.\nManages the replicas and authors for an instance.\nA wrapper around <code>Store</code> for a specific <code>NamespaceId</code>\nCreate a new iterator over all content hashes.\nClose a replica.\nGet all content hashes of all replicas in the store.\nDelete an author.\nFlush the current transaction, if any.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nGet an author key from the store.\nGet the download policy for a namespace.\nGet an entry by key and author.\nGet the latest entry for each author in a namespace.\nGet an iterator over entries of a replica.\nGet the peers that have been useful for a document.\nCheck if a <code>AuthorHeads</code> contains entry timestamps that we …\nImport an author key pair.\nImport a new replica namespace.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nConvert an iterator of anything into <code>FallibleIterator</code> by …\nConvert an iterator of anything into <code>FallibleIterator</code> by …\nConvert an iterator of anything into <code>FallibleIterator</code> by …\nConvert an iterator of anything into <code>FallibleIterator</code> by …\nList all author keys in this store.\nList all replica namespaces in this store.\nLoad the replica info from the store.\nCreate a new store in memory.\nCreate a new author key and persist it in the store.\nCreate a new replica for <code>namespace</code> and persist in this …\nOpen a replica from this store.\nCreate or open a store from a <code>path</code> to a database file.\nRegister a peer that has been useful to sync a document.\nRemove a replica.\nSet the download policy for a namespace.\nGet a read-only snapshot of the database.\nGet an owned read-only snapshot of the database.\nConvert an iterator of <code>Result</code>s into <code>FallibleIterator</code> by …\nConvert an iterator of <code>Result</code>s into <code>FallibleIterator</code> by …\nConvert an iterator of <code>Result</code>s into <code>FallibleIterator</code> by …\nConvert an iterator of <code>Result</code>s into <code>FallibleIterator</code> by …\nTimestamps of the latest entry for each author.\nEntry signature is invalid.\nThe capability of the namespace.\nErrors for capability operations\nKind of capability of the namespace.\nThe replica is closed, no operations may be performed.\nThe content is completely available.\nWhether the content status is available on a node.\nCallback that may be set on a replica to determine the …\nA single entry in a <code>Replica</code>\nAttempted to insert an empty entry.\nSignature over an entry.\nEvent emitted by sync when entries are added.\nThe content is partially available.\nError emitted when inserting entries into a <code>Replica</code> failed\nWhether an entry was inserted locally or by a remote peer.\nEntry has length 0 but not the empty hash, or the empty …\nEntry namespace does not match the current replica.\nThe entry was inserted locally.\nA local entry has been added.\nMax time in the future from our wall clock time that we …\nThe content is missing.\nNamespaces are not the same\nA newer entry exists for either this entry’s key or a …\nByte representation of a <code>PeerId</code> from <code>iroh-net</code>.\nProtocol message for the set reconciliation protocol.\nA readable replica.\nRead only access to the namespace.\nError that occurs trying to access the <code>NamespaceSecret</code> of …\nReplica is read only.\nThe data part of an entry in a <code>Replica</code>.\nThe identifier of a record.\nA remote entry has been added.\nLocal representation of a mutable, synchronizable …\nIn memory information about an open replica.\nA signed entry.\nStorage error\nThe entry was received from the remote node identified by …\nOutcome of a sync operation.\nEntry timestamp is too far in the future.\nValidation failure\nReason why entry validation failed\nA writable replica.\nWrite access to the namespace.\nGet this <code>RecordIdentifier</code> as a tuple of byte slices.\nGet this <code>RecordIdentifier</code> as Bytes.\nGet the <code>AuthorId</code> of this entry.\nGet the <code>AuthorId</code> of this record as byte array.\nGet the author bytes of this entry.\nGet the <code>Capability</code> of this <code>Replica</code>.\nReturns true if the replica is closed.\nGet the content <code>Hash</code> of the entry.\nGet the <code>Hash</code> of the content data of this record.\nGet the content length of the entry.\nGet the length of the data addressed by this record’s …\nDecode from byte slice created with <code>Self::encode</code>.\nDelete entries that match the given <code>author</code> and key <code>prefix</code>.\nCreate a tombstone record (empty content)\nCreate a tombstone record with the timestamp set to now.\nSerialize this entry into its canonical byte …\nEncode into a byte array with a limited size.\nGet the <code>Entry</code>.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCreate a new signed entry by signing an entry with the …\nCreate a new signature by signing an entry with the …\nCreate a new signed entries from its parts.\nCreate a <code>Capability</code> from its raw representation.\nGet the timestamp for an author.\nCan this state offer newer stuff to <code>other</code>?\nHashes the given data and inserts it.\nTimestamp of the latest entry for each author in the set …\nGet the <code>NamespaceId</code> for this <code>Capability</code>.\nGet the namespace identifier for this <code>Replica</code>.\nGet the <code>RecordIdentifier</code> for this entry.\nInsert a new record at the given key.\nInsert a new timestamp.\nInsert an entry into this replica which was received from …\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturn <code>true</code> if the entry is empty.\nWhether this <code>AuthorHeads</code> is empty.\nCreate an iterator over the entries in this state.\nGet the key of the entry.\nGet the key of this entry.\nGet the key of this record.\nGet the key of this record as <code>Bytes</code>.\nGet the kind of capability.\nNumber of author-timestamp pairs.\nMerge this capability with another capability.\nMerge another author head state into this one.\nMerge a capability.\nGet the <code>NamespaceId</code> of this entry.\nGet the <code>NamespaceId</code> of this record as byte array.\nCreate a new replica.\nCreate a new replica.\nCreate a new entry\nCreate a new <code>RecordIdentifier</code>.\nCreate a new record.\nCreate a new <code>Record</code> with the timestamp set to now.\nCreate a new empty entry with the current timestamp.\nNumber of entries we received.\nNumber of entries we sent.\nGet the raw representation of this namespace capability.\nGet the <code>Record</code> contained in this entry.\nGet the identifier for an entry in this replica.\nGet the <code>NamespaceSecret</code> of this <code>Capability</code>. Will fail if …\nGet the byte representation of the <code>NamespaceSecret</code> key for …\nSet the content status callback.\nSign this entry with a <code>NamespaceSecret</code> and <code>Author</code>.\nGet the signature.\nSubscribe to insert events.\nGet the number of current event subscribers.\nCreate the initial message for the set reconciliation flow …\nProcess a set reconciliation message from a remote peer.\nGet the timestamp of the entry.\nGet the timestamp of this record.\nGet this <code>RecordIdentifier</code> as a tuple of bytes.\nSerialize this entry into a new vector with its canonical …\nExplicitly unsubscribe a sender.\nValidate that the entry has the empty hash if the length …\nValidate that the entry has the empty hash if the length …\nVerify the signatures on this entry.\nVerify that this signature was created by signing the <code>entry</code>…\nInserted entry.\nInserted entry.\nPeer that provided the inserted entry.\nDocument in which the entry was inserted.\nDocument in which the entry was inserted.\n<code>ContentStatus</code> for this entry in the remote’s replica.\nWhether download policies require the content to be …\nThe peer from which we received this entry.\nWhether the peer claims to have the content blob for this …")