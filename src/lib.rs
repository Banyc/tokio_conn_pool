use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{sync::RwLock as TokioRwLock, task::JoinSet};

const QUEUE_LEN: usize = 16;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const RETRY_INTERVAL: Duration = Duration::from_secs(30);

/// Connect and give the connection to the pool.
///
/// # Example
///
/// ```rust
/// use std::net::SocketAddr;
///
/// use async_trait::async_trait;
/// use tokio::net::TcpStream;
/// use tokio_conn_pool::Connect;
///
/// #[derive(Debug)]
/// struct TcpConnect {
///     pub addr: SocketAddr,
/// }
/// #[async_trait]
/// impl Connect for TcpConnect {
///     type Connection = TcpStream;
///     async fn connect(&self) -> Option<Self::Connection> {
///         TcpStream::connect(self.addr).await.ok()
///     }
/// }
/// ```
#[async_trait]
pub trait Connect: std::fmt::Debug + Sync + Send {
    type Connection;
    /// Connect and give the connection to the pool.
    async fn connect(&self) -> Option<Self::Connection>;
}

/// Do anything necessary to keep this `conn` alive.
///
/// # Example
///
/// ```rust
/// use std::{io, net::SocketAddr, time::Duration};
///
/// use async_trait::async_trait;
/// use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
/// use tokio_conn_pool::Heartbeat;
///
/// #[derive(Debug)]
/// struct TcpHeartbeat;
/// #[async_trait]
/// impl Heartbeat for TcpHeartbeat {
///     type Connection = TcpStream;
///     async fn heartbeat(&self, mut conn: Self::Connection) -> Option<Self::Connection> {
///         tokio::time::timeout(Duration::from_secs(1), async move {
///             conn.write_all(b"ping").await?;
///             let mut buf = [0; 4];
///             conn.read_exact(&mut buf).await?;
///             if &buf != b"pong" {
///                 return Err(io::Error::new(
///                     io::ErrorKind::InvalidData,
///                     "peer sent the wrong pong",
///                 ));
///             }
///             Ok::<Self::Connection, io::Error>(conn)
///         })
///         .await
///         .ok()?
///         .ok()
///     }
/// }
/// ```
#[async_trait]
pub trait Heartbeat: std::fmt::Debug + Sync + Send {
    type Connection;
    /// Do anything necessary to keep this `conn` alive.
    async fn heartbeat(&self, conn: Self::Connection) -> Option<Self::Connection>;
}

/// Store necessary information for the connection pool to maintain connections.
#[derive(Debug)]
pub struct ConnPoolEntry<K, T> {
    pub key: K,
    pub connect: Arc<dyn Connect<Connection = T>>,
    pub heartbeat: Arc<dyn Heartbeat<Connection = T>>,
}
impl<K: Clone, T> Clone for ConnPoolEntry<K, T> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            connect: Arc::clone(&self.connect),
            heartbeat: Arc::clone(&self.heartbeat),
        }
    }
}

/// The pool keeps a set of connections alive for each target.
#[derive(Debug)]
pub struct ConnPool<K, T> {
    queues: HashMap<K, Mutex<ConnQueue<K, T>>>,
}
impl<K, T: Send + Sync + 'static> ConnPool<K, T>
where
    K: std::hash::Hash + Eq + Clone + Send + 'static,
{
    pub fn empty() -> Self {
        Self {
            queues: Default::default(),
        }
    }

    /// # Example
    ///
    /// ```ignore
    /// let entry = ConnPoolEntry {
    ///     key: addr,
    ///     connect: Arc::new(TcpConnect { addr }),
    ///     heartbeat: Arc::new(TcpHeartbeat),
    /// };
    /// let pool = ConnPool::new([entry].into_iter());
    /// ```
    pub fn new(entries: impl Iterator<Item = ConnPoolEntry<K, T>>) -> Self {
        let mut queues = HashMap::new();
        entries.for_each(|e| {
            let key = e.key.clone();
            let mut queue = ConnQueue::new(e);
            for _ in 0..QUEUE_LEN {
                queue.spawn_insert(HEARTBEAT_INTERVAL);
            }
            queues.insert(key, Mutex::new(queue));
        });

        Self { queues }
    }

    /// Pull a connection as fast as possible. It gives up and returns [`None`] if the pulling requires any sort of waiting.
    pub fn pull(&self, key: &K) -> Option<T> {
        let mut queue = match self.queues.get(key).and_then(|queue| queue.try_lock().ok()) {
            Some(queue) => queue,
            None => return None,
        };
        queue.try_swap(HEARTBEAT_INTERVAL)
    }
}

#[derive(Debug)]
struct ConnQueue<K, T> {
    queue: Arc<RwLock<VecDeque<ConnCell<T>>>>,
    connect_tasks: JoinSet<()>,
    entry: ConnPoolEntry<K, T>,
}

impl<K: Send + Clone + 'static, T: Send + Sync + 'static> ConnQueue<K, T> {
    pub fn new(entry: ConnPoolEntry<K, T>) -> Self {
        Self {
            queue: Default::default(),
            connect_tasks: Default::default(),
            entry,
        }
    }

    pub fn spawn_insert(&mut self, heartbeat_interval: Duration) {
        let queue = self.queue.clone();
        let entry = self.entry.clone();
        self.connect_tasks.spawn(async move {
            let conn = loop {
                if let Some(conn) = entry.connect.connect().await {
                    break conn;
                };
                tokio::time::sleep(RETRY_INTERVAL).await;
            };
            let cell = ConnCell::create(conn, Arc::clone(&entry.heartbeat), heartbeat_interval);
            let mut queue = queue.write().unwrap();
            queue.push_back(cell);
        });
    }

    pub fn try_swap(&mut self, heartbeat_interval: Duration) -> Option<T> {
        let res = {
            let mut queue = match self.queue.try_write() {
                Ok(x) => x,
                // The queue is being occupied
                Err(_) => return None,
            };
            let front = match queue.pop_front() {
                Some(x) => x,
                // The queue is empty
                None => return None,
            };
            match front.try_take() {
                TryTake::Ok(conn) => Some(conn),
                TryTake::Occupied => {
                    queue.push_back(front);
                    return None;
                }
                TryTake::Killed => None,
            }
        };

        // Remove the completed task to avoid memory leak
        {
            let waker = noop_waker::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            let ready = self.connect_tasks.poll_join_next(&mut cx);
            let ready = match ready {
                std::task::Poll::Ready(r) => r,
                std::task::Poll::Pending => panic!(),
            };
            ready.unwrap().expect("Pool task panicked");
        }

        // Replenish
        self.spawn_insert(heartbeat_interval);
        assert_eq!(self.connect_tasks.len(), QUEUE_LEN);

        res
    }
}

#[derive(Debug)]
struct ConnCell<T> {
    cell: Arc<TokioRwLock<Option<T>>>,
    _heartbeat_task: JoinSet<()>,
}

impl<T: Send + Sync + 'static> ConnCell<T> {
    pub fn create(
        conn: T,
        heartbeat: Arc<dyn Heartbeat<Connection = T>>,
        heartbeat_interval: Duration,
    ) -> Self {
        let cell = Arc::new(TokioRwLock::new(Some(conn)));
        let mut heartbeat_task = JoinSet::new();
        heartbeat_task.spawn({
            let cell = Arc::clone(&cell);
            async move {
                loop {
                    tokio::time::sleep(heartbeat_interval).await;
                    let mut cell = cell.write().await;
                    let conn = match cell.take() {
                        Some(x) => x,
                        None => break,
                    };
                    let Some(conn) = heartbeat.heartbeat(conn).await else {
                        break;
                    };
                    *cell = Some(conn);
                }
            }
        });
        Self {
            cell,
            _heartbeat_task: heartbeat_task,
        }
    }

    pub fn try_take(&self) -> TryTake<T> {
        let mut cell = match self.cell.try_write() {
            Ok(x) => x,
            Err(_) => return TryTake::Occupied,
        };
        match cell.take() {
            Some(conn) => TryTake::Ok(conn),
            None => TryTake::Killed,
        }
    }
}

enum TryTake<T> {
    Ok(T),
    Occupied,
    Killed,
}

#[cfg(test)]
mod tests {
    use std::{io, net::SocketAddr};

    use swap::Swap;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinSet,
    };

    use super::*;

    async fn spawn_listener() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = [0; 4];
                    loop {
                        if let Err(_e) = stream.read_exact(&mut buf).await {
                            break;
                        }
                        if &buf != b"ping" {
                            panic!();
                        }
                        stream.write_all(b"pong").await.unwrap();
                    }
                });
            }
        });
        addr
    }

    #[derive(Debug)]
    struct TcpConnect {
        pub addr: SocketAddr,
    }
    #[async_trait]
    impl Connect for TcpConnect {
        type Connection = TcpStream;
        async fn connect(&self) -> Option<Self::Connection> {
            TcpStream::connect(self.addr).await.ok()
        }
    }

    #[derive(Debug)]
    struct TcpHeartbeat;
    #[async_trait]
    impl Heartbeat for TcpHeartbeat {
        type Connection = TcpStream;
        async fn heartbeat(&self, mut conn: Self::Connection) -> Option<Self::Connection> {
            tokio::time::timeout(Duration::from_secs(1), async move {
                conn.write_all(b"ping").await?;
                let mut buf = [0; 4];
                conn.read_exact(&mut buf).await?;
                if &buf != b"pong" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "peer sent the wrong pong",
                    ));
                }
                Ok::<Self::Connection, io::Error>(conn)
            })
            .await
            .ok()?
            .ok()
        }
    }

    #[tokio::test]
    async fn take_none() {
        let addr = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        let entry = ConnPoolEntry {
            key: addr,
            connect: Arc::new(TcpConnect { addr }),
            heartbeat: Arc::new(TcpHeartbeat),
        };
        let pool = Swap::new(ConnPool::new([entry].into_iter()));
        let mut join_set = JoinSet::new();
        for _ in 0..100 {
            let pool = pool.clone();
            join_set.spawn(async move {
                let res = pool.inner().pull(&addr);
                assert!(res.is_none());
            });
        }
    }

    #[tokio::test]
    async fn take_some() {
        let addr = spawn_listener().await;
        let entry = ConnPoolEntry {
            key: addr,
            connect: Arc::new(TcpConnect { addr }),
            heartbeat: Arc::new(TcpHeartbeat),
        };
        let pool = ConnPool::new([entry].into_iter());
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for _ in 0..QUEUE_LEN {
                let res = pool.pull(&addr);
                assert!(res.is_some());
            }
        }
    }

    #[tokio::test]
    async fn heartbeat() {
        let addr = spawn_listener().await;
        let entry = ConnPoolEntry {
            key: addr,
            connect: Arc::new(TcpConnect { addr }),
            heartbeat: Arc::new(TcpHeartbeat),
        };
        let pool = ConnPool::new([entry].into_iter());
        tokio::time::sleep(Duration::from_secs(1) + HEARTBEAT_INTERVAL).await;
        for _ in 0..QUEUE_LEN {
            let res = pool.pull(&addr);
            assert!(res.is_some());
        }
    }
}
