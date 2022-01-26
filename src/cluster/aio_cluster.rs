//! Redis cluster support.
//!
//! This module extends the library to be able to use cluster.
//! ClusterClient implements traits of ConnectionLike and Commands.
//!
//! Note that the cluster support currently does not provide pubsub
//! functionality.
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::ClusterClient;
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::open(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let _: () = connection.set("test", "test_data").unwrap();
//! let rv: String = connection.get("test").unwrap();
//!
//! assert_eq!(rv, "test_data");
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::{cluster_pipe, ClusterClient};
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::open(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let key = "test";
//!
//! let _: () = cluster_pipe()
//!     .rpush(key, "123").ignore()
//!     .ltrim(key, -10, -1).ignore()
//!     .expire(key, 60).ignore()
//!     .query(&mut connection).unwrap();
//! ```
use futures::{AsyncRead, AsyncWrite};
use rand::seq::IteratorRandom;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::Iterator;
use std::thread;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::aio::{connect, Connection, ConnectionLike, RedisRuntime};

use super::{
    cmd, parse_redis_value, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo,
    RedisError, RedisResult, Value,
};

pub use crate::cluster_client::{ClusterClient, ClusterClientBuilder};
use crate::cluster_pipeline::UNROUTABLE_ERROR;
pub use crate::cluster_pipeline::{cluster_pipe, ClusterPipeline};
use crate::cluster_routing::{Routable, RoutingInfo, Slot, SLOT_SIZE};
use crate::RedisFuture;

type SlotMap = BTreeMap<u16, String>;

/// This is a connection of Redis cluster.
pub struct ClusterConnection<C> {
    initial_nodes: Vec<ConnectionInfo>,
    connections: Mutex<HashMap<String, Connection<C>>>,
    slots: Mutex<SlotMap>,
    auto_reconnect: std::sync::Mutex<bool>,
    readonly: bool,
    password: Option<String>,
    tls: Option<TlsMode>,
}

#[derive(Clone, Copy)]
enum TlsMode {
    Secure,
    Insecure,
}

impl TlsMode {
    fn from_insecure_flag(insecure: bool) -> TlsMode {
        if insecure {
            TlsMode::Insecure
        } else {
            TlsMode::Secure
        }
    }
}

impl<C> ClusterConnection<C>
where
    C: Unpin + RedisRuntime + AsyncRead + AsyncWrite + Send,
{
    pub(crate) async fn new(
        initial_nodes: Vec<ConnectionInfo>,
        readonly: bool,
        password: Option<String>,
    ) -> RedisResult<Self> {
        let connections =
            Self::create_initial_connections(&initial_nodes, readonly, password.clone()).await?;
        let connection = ClusterConnection {
            connections: Mutex::new(connections),
            readonly,
            password,
            slots: Mutex::new(SlotMap::new()),
            auto_reconnect: std::sync::Mutex::new(true),
            #[cfg(feature = "tls")]
            tls: {
                if initial_nodes.is_empty() {
                    None
                } else {
                    // TODO: Maybe should run through whole list and make sure they're all matching?
                    match &initial_nodes.get(0).unwrap().addr {
                        ConnectionAddr::Tcp(_, _) => None,
                        ConnectionAddr::TcpTls {
                            host: _,
                            port: _,
                            insecure,
                        } => Some(TlsMode::from_insecure_flag(*insecure)),
                        _ => None,
                    }
                }
            },
            #[cfg(not(feature = "tls"))]
            tls: None,
            initial_nodes,
        };
        connection.refresh_slots().await?;

        Ok(connection)
    }

    /// Set an auto reconnect attribute.
    /// Default value is true;
    pub fn set_auto_reconnect(&self, value: bool) {
        let mut auto_reconnect = self.auto_reconnect.lock().unwrap();
        *auto_reconnect = value;
    }

    /// Check that all connections it has are available (`PING` internally).
    pub async fn check_connection(&mut self) -> bool {
        let mut connections = self.connections.lock().await;
        for (_, conn) in &mut *connections {
            if !conn.check_connection().await {
                return false;
            }
        }
        true
    }

    pub(crate) async fn execute_pipeline(
        &mut self,
        pipe: &ClusterPipeline,
    ) -> RedisResult<Vec<Value>> {
        self.send_recv_and_retry_cmds(pipe.commands()).await
    }

    /// Returns the connection status.
    ///
    /// The connection is open until any `read_response` call recieved an
    /// invalid response from the server (most likely a closed or dropped
    /// connection, otherwise a Redis protocol error). When using unix
    /// sockets the connection is open until writing a command failed with a
    /// `BrokenPipe` error.
    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        readonly: bool,
        password: Option<String>,
    ) -> RedisResult<HashMap<String, Connection<C>>> {
        let mut connections = HashMap::with_capacity(initial_nodes.len());

        for info in initial_nodes.iter() {
            let addr = match info.addr {
                ConnectionAddr::Tcp(ref host, port) => format!("redis://{}:{}", host, port),
                ConnectionAddr::TcpTls {
                    ref host,
                    port,
                    insecure,
                } => {
                    let tls_mode = TlsMode::from_insecure_flag(insecure);
                    build_connection_string(host, Some(port), Some(tls_mode))
                }
                _ => panic!("No reach."),
            };

            if let Ok(mut conn) = connect_async(info.clone(), readonly, password.clone()).await {
                if conn.check_connection().await {
                    connections.insert(addr, conn);
                    break;
                }
            }
        }

        if connections.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "It failed to check startup nodes.",
            )));
        }
        Ok(connections)
    }

    // Query a node to discover slot-> master mappings.
    async fn refresh_slots(&self) -> RedisResult<()> {
        let mut slots = self.slots.lock().await;
        *slots = if self.readonly {
            self.create_new_slots(|slot_data| {
                let replicas = slot_data.replicas();
                if replicas.is_empty() {
                    slot_data.master().to_string()
                } else {
                    replicas.choose(&mut thread_rng()).unwrap().to_string()
                }
            })
            .await?
        } else {
            self.create_new_slots(|slot_data| slot_data.master().to_string())
                .await?
        };
        let mut connections = self.connections.lock().await;
        *connections = {
            // Remove dead connections and connect to new nodes if necessary
            let mut new_connections = HashMap::with_capacity(connections.len());

            for addr in slots.values() {
                if !new_connections.contains_key(addr) {
                    if connections.contains_key(addr) {
                        let mut conn = connections.remove(addr).unwrap();
                        if conn.check_connection().await {
                            new_connections.insert(addr.to_string(), conn);
                            continue;
                        }
                    }

                    if let Ok(mut conn) =
                        connect_async(addr.as_ref(), self.readonly, self.password.clone()).await
                    {
                        if conn.check_connection().await {
                            new_connections.insert(addr.to_string(), conn);
                        }
                    }
                }
            }
            new_connections
        };
        Ok(())
    }

    async fn create_new_slots<F>(&self, mut get_addr: F) -> RedisResult<SlotMap>
    where
        F: FnMut(&Slot) -> String,
    {
        let mut connections = self.connections.lock().await;
        let mut new_slots = None;
        let len = connections.len();
        let mut samples = connections
            .values_mut()
            .choose_multiple(&mut thread_rng(), len);

        for mut conn in samples.iter_mut() {
            if let Ok(mut slots_data) = get_slots(&mut conn, self.tls).await {
                slots_data.sort_by_key(|s| s.start());
                let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
                    if prev_end != slot_data.start() {
                        return Err(RedisError::from((
                            ErrorKind::ResponseError,
                            "Slot refresh error.",
                            format!(
                                "Received overlapping slots {} and {}..{}",
                                prev_end,
                                slot_data.start(),
                                slot_data.end()
                            ),
                        )));
                    }
                    Ok(slot_data.end() + 1)
                })?;

                if usize::from(last_slot) != SLOT_SIZE {
                    return Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "Slot refresh error.",
                        format!("Lacks the slots >= {}", last_slot),
                    )));
                }

                new_slots = Some(
                    slots_data
                        .iter()
                        .map(|slot_data| (slot_data.end(), get_addr(slot_data)))
                        .collect(),
                );
                break;
            }
        }

        match new_slots {
            Some(new_slots) => Ok(new_slots),
            None => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                "didn't get any slots from server".to_string(),
            ))),
        }
    }

    async fn get_connection_by_addr<'a>(
        &self,
        connections: &'a mut HashMap<String, Connection<C>>,
        addr: &str,
    ) -> RedisResult<&'a mut Connection<C>> {
        if connections.contains_key(addr) {
            Ok(connections.get_mut(addr).unwrap())
        } else {
            // Create new connection.
            // TODO: error handling
            let conn = connect_async(addr, self.readonly, self.password.clone()).await?;
            Ok(connections.entry(addr.to_string()).or_insert(conn))
        }
    }
    async fn execute_on_all_nodes<'a, T, F>(&'a self, mut func: F) -> RedisResult<T>
    where
        T: MergeResults,
        F: FnMut(&mut Connection<C>) -> RedisFuture<T>,
    {
        let mut results = HashMap::new();

        let mut connections = self.connections.lock().await;

        for (addr, connection) in connections.iter_mut() {
            results.insert(addr.as_str(), func(connection).await?);
        }

        Ok(T::merge_results(results))
    }

    async fn request<'a, R, T, F>(&'a self, cmd: &'a R, mut func: F) -> RedisResult<T>
    where
        R: ?Sized + Routable,
        T: MergeResults + std::fmt::Debug,
        F: for<'r> FnMut(&'r mut Connection<C>) -> RedisFuture<'r, T>,
    {
        let slot = match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::Random) => None,
            Some(RoutingInfo::Slot(slot)) => Some(slot),
            Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
                return self.execute_on_all_nodes(func).await;
            }
            None => fail!(UNROUTABLE_ERROR),
        };

        let mut retries = 16;
        let mut excludes = HashSet::new();
        let mut redirected = None::<String>;
        let mut is_asking = false;
        loop {
            // Get target address and response.
            let mut connections = self.connections.lock().await;
            let (addr, rv) = {
                let (addr, conn) = if let Some(addr) = redirected.take() {
                    let conn = self
                        .get_connection_by_addr(&mut *connections, &addr)
                        .await?;
                    if is_asking {
                        // if we are in asking mode we want to feed a single
                        // ASKING command into the connection before what we
                        // actually want to execute

                        conn.req_packed_command(&crate::cmd("ASKING")).await?;
                        is_asking = false;
                    }
                    (
                        addr.to_string(),
                        self.get_connection_by_addr(&mut *connections, &addr)
                            .await?,
                    )
                } else if !excludes.is_empty() || slot.is_none() {
                    get_random_connection(&mut *connections, Some(&excludes))
                } else {
                    let slots = self.slots.lock().await;
                    if let Some((_, addr)) = slots.range(&slot.unwrap()..).next() {
                        (
                            addr.to_owned(),
                            self.get_connection_by_addr(&mut *connections, addr).await?,
                        )
                    } else {
                        get_random_connection(&mut *connections, None)
                    }
                };
                (addr, func(conn).await)
            };

            let err = match rv {
                Ok(rv) => return Ok(rv),
                Err(err) if retries == 1 => return Err(err),
                Err(err) => err,
            };

            retries -= 1;

            if err.is_cluster_error() {
                let kind = err.kind();

                if kind == ErrorKind::Ask {
                    redirected = err
                        .redirect_node()
                        .map(|(node, _slot)| build_connection_string(node, None, self.tls));
                    is_asking = true;
                } else if kind == ErrorKind::Moved {
                    // Refresh slots.
                    self.refresh_slots().await?;
                    excludes.clear();

                    // Request again.
                    redirected = err
                        .redirect_node()
                        .map(|(node, _slot)| build_connection_string(node, None, self.tls));
                    is_asking = false;
                    continue;
                } else if kind == ErrorKind::TryAgain || kind == ErrorKind::ClusterDown {
                    // Sleep and retry.
                    let sleep_time = 2u64.pow(16 - retries.max(9)) * 10;
                    thread::sleep(Duration::from_millis(sleep_time));
                    excludes.clear();
                    continue;
                }
            } else if *self.auto_reconnect.lock().unwrap() && err.is_io_error() {
                let new_connections = Self::create_initial_connections(
                    &self.initial_nodes,
                    self.readonly,
                    self.password.clone(),
                )
                .await?;
                {
                    *self.connections.lock().await = new_connections;
                }
                self.refresh_slots().await?;
                excludes.clear();
                continue;
            } else {
                return Err(err);
            }

            excludes.insert(addr);

            if excludes.len() >= connections.len() {
                return Err(err);
            }
        }
    }

    async fn send_recv_and_retry_cmds<'a>(&'a self, cmds: &[Cmd]) -> RedisResult<Vec<Value>> {
        // Vector to hold the results, pre-populated with `Nil` values. This allows the original
        // cmd ordering to be re-established by inserting the response directly into the result
        // vector (e.g., results[10] = response).
        let mut results = vec![Value::Nil; cmds.len()];

        let node_cmds = self.send_all_commands(cmds).await?;
        let to_retry = self.recv_all_commands(&mut results, &node_cmds).await?;

        if to_retry.is_empty() {
            return Ok(results);
        }

        // Refresh the slots to ensure that we have a clean slate for the retry attempts.
        self.refresh_slots().await?;

        // Given that there are commands that need to be retried, it means something in the cluster
        // topology changed. Execute each command seperately to take advantage of the existing
        // retry logic that handles these cases.
        for retry_idx in to_retry {
            let cmd = &cmds[retry_idx];
            results[retry_idx] = self
                .request(cmd, move |conn: &mut Connection<C>| {
                    conn.req_packed_command(cmd)
                })
                .await?;
        }
        Ok(results)
    }

    // Build up a pipeline per node, then send it
    async fn send_all_commands(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut connections = self.connections.lock().await;

        let node_cmds = self.map_cmds_to_nodes(cmds).await?;
        for nc in &node_cmds {
            self.get_connection_by_addr(&mut *connections, &nc.addr)
                .await?
                .send_packed_command(&nc.pipe)
                .await?;
        }
        Ok(node_cmds)
    }

    async fn get_addr_for_cmd(&self, cmd: &Cmd) -> RedisResult<String> {
        let slots = self.slots.lock().await;

        let addr_for_slot = |slot: u16| -> RedisResult<String> {
            let (_, addr) = slots
                .range(&slot..)
                .next()
                .ok_or((ErrorKind::ClusterDown, "Missing slot coverage"))?;
            Ok(addr.to_string())
        };

        match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::Random) => {
                let mut rng = thread_rng();
                Ok(addr_for_slot(rng.gen_range(0..SLOT_SIZE) as u16)?)
            }
            Some(RoutingInfo::Slot(slot)) => Ok(addr_for_slot(slot)?),
            _ => fail!(UNROUTABLE_ERROR),
        }
    }

    async fn map_cmds_to_nodes(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut cmd_map: HashMap<String, NodeCmd> = HashMap::new();

        for (idx, cmd) in cmds.iter().enumerate() {
            let addr = self.get_addr_for_cmd(cmd).await?;
            let nc = cmd_map
                .entry(addr.clone())
                .or_insert_with(|| NodeCmd::new(addr));
            nc.indexes.push(idx);
            cmd.write_packed_command(&mut nc.pipe);
        }

        let mut result = Vec::new();
        for (_, v) in cmd_map.drain() {
            result.push(v);
        }
        Ok(result)
    }

    // Receive from each node, keeping track of which commands need to be retried.
    async fn recv_all_commands(
        &self,
        results: &mut Vec<Value>,
        node_cmds: &[NodeCmd],
    ) -> RedisResult<Vec<usize>> {
        let mut to_retry = Vec::new();
        let mut connections = self.connections.lock().await;
        let mut first_err = None;

        for nc in node_cmds {
            for cmd_idx in &nc.indexes {
                match self
                    .get_connection_by_addr(&mut *connections, &nc.addr)
                    .await?
                    .recv_response()
                    .await
                {
                    Ok(item) => results[*cmd_idx] = item,
                    Err(err) if err.is_cluster_error() => to_retry.push(*cmd_idx),
                    Err(err) => first_err = first_err.or(Some(err)),
                }
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(to_retry),
        }
    }
}

trait MergeResults {
    fn merge_results(_values: HashMap<&str, Self>) -> Self
    where
        Self: Sized;
}

impl MergeResults for Value {
    fn merge_results(values: HashMap<&str, Value>) -> Value {
        let mut items = vec![];
        for (addr, value) in values.into_iter() {
            items.push(Value::Bulk(vec![
                Value::Data(addr.as_bytes().to_vec()),
                value,
            ]));
        }
        Value::Bulk(items)
    }
}

impl MergeResults for Vec<Value> {
    fn merge_results(_values: HashMap<&str, Vec<Value>>) -> Vec<Value> {
        unreachable!("attempted to merge a pipeline. This should not happen");
    }
}

#[derive(Debug)]
struct NodeCmd {
    // The original command indexes
    indexes: Vec<usize>,
    pipe: Vec<u8>,
    addr: String,
}

impl NodeCmd {
    fn new(a: String) -> NodeCmd {
        NodeCmd {
            indexes: vec![],
            pipe: vec![],
            addr: a,
        }
    }
}

impl<C> ConnectionLike for ClusterConnection<C>
where
    C: Unpin + RedisRuntime + AsyncRead + AsyncWrite + Send,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            let value = parse_redis_value(&cmd.get_packed_command())?;
            let func = |conn: &mut Connection<C>| conn.req_packed_command(cmd);
            self.request(&value, func).await
        })
    }

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let value = parse_redis_value(&cmd.get_packed_pipeline())?;
            let func = |conn: &mut Connection<C>| conn.req_packed_commands(cmd, offset, count);
            self.request(&value, func).await
        })
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn check_connection<'a>(&'a mut self) -> futures_util::future::BoxFuture<'a, bool> {
        Box::pin(async move {
            self.req_packed_command(&cmd("PING"))
                .await
                .and_then(|v| crate::from_redis_value::<String>(&v))
                .is_ok()
        })
    }
}

async fn connect_async<T: IntoConnectionInfo, C>(
    info: T,
    readonly: bool,
    password: Option<String>,
) -> RedisResult<Connection<C>>
where
    T: std::fmt::Debug,
    C: Unpin + RedisRuntime + AsyncRead + AsyncWrite + Send,
{
    let mut connection_info = info.into_connection_info()?;
    connection_info.redis.password = password;
    let client = crate::Client::open(connection_info)?;

    let mut con = connect(client.get_connection_info()).await?;
    if readonly {
        cmd("READONLY").query_async(&mut con).await?;
    }
    Ok(con)
}

fn get_random_connection<'a, C>(
    connections: &'a mut HashMap<String, Connection<C>>,
    excludes: Option<&'a HashSet<String>>,
) -> (String, &'a mut Connection<C>) {
    let mut rng = thread_rng();
    let addr = match excludes {
        Some(excludes) if excludes.len() < connections.len() => connections
            .keys()
            .filter(|key| !excludes.contains(*key))
            .choose(&mut rng)
            .unwrap()
            .to_string(),
        _ => connections.keys().choose(&mut rng).unwrap().to_string(),
    };

    let con = connections.get_mut(&addr).unwrap();
    (addr, con)
}

// Get slot data from connection.
async fn get_slots<C>(
    connection: &mut Connection<C>,
    tls_mode: Option<TlsMode>,
) -> RedisResult<Vec<Slot>>
where
    C: Unpin + RedisRuntime + AsyncRead + AsyncWrite + Send,
{
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let value = connection.req_packed_command(&cmd).await?;

    // Parse response.
    let mut result = Vec::with_capacity(2);

    if let Value::Bulk(items) = value {
        let mut iter = items.into_iter();
        while let Some(Value::Bulk(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut nodes: Vec<String> = item
                .into_iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Bulk(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::Data(ref ip) = node[0] {
                            String::from_utf8_lossy(ip)
                        } else {
                            return None;
                        };
                        if ip.is_empty() {
                            return None;
                        }

                        let port = if let Value::Int(port) = node[1] {
                            port as u16
                        } else {
                            return None;
                        };
                        Some(build_connection_string(&ip, Some(port), tls_mode))
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let replicas = nodes.split_off(1);
            result.push(Slot::new(start, end, nodes.pop().unwrap(), replicas));
        }
    }

    Ok(result)
}

fn build_connection_string(host: &str, port: Option<u16>, tls_mode: Option<TlsMode>) -> String {
    let host_port = match port {
        Some(port) => format!("{}:{}", host, port),
        None => host.to_string(),
    };
    match tls_mode {
        None => format!("redis://{}", host_port),
        Some(TlsMode::Insecure) => {
            format!("rediss://{}/#insecure", host_port)
        }
        Some(TlsMode::Secure) => format!("rediss://{}", host_port),
    }
}
