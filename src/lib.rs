use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use tokio::{sync::RwLock as TokioRwLock, task::JoinSet};

const QUEUE_LEN: usize = 16;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const RETRY_INTERVAL: Duration = Duration::from_secs(30);

#[async_trait]
pub trait Connect: Sync + Send {
    type Key;
    type Connection;
    async fn connect(&self, key: &Self::Key) -> Option<Self::Connection>;
}

#[async_trait]
pub trait Heartbeat: Sync + Send {
    type Connection;
    async fn heartbeat(&self, conn: Self::Connection) -> Option<Self::Connection>;
}

// #[derive(Debug, Clone)]
pub struct ConnPool<K, T> {
    pool: Arc<ArcSwap<PoolInner<K, T>>>,
}
impl<K, T: Send + Sync + 'static> ConnPool<K, T>
where
    K: std::hash::Hash + Eq + Clone + Send + 'static,
{
    pub fn empty() -> Self {
        Self {
            pool: Arc::new(Arc::new(PoolInner::empty()).into()),
        }
    }

    pub fn new(
        key_connect_pairs: impl Iterator<Item = (K, Arc<dyn Connect<Key = K, Connection = T>>)>,
        heartbeat: Arc<dyn Heartbeat<Connection = T>>,
    ) -> Self {
        Self {
            pool: Arc::new(Arc::new(PoolInner::new(key_connect_pairs, heartbeat)).into()),
        }
    }

    pub fn pull(&self, addr: &K) -> Option<T> {
        self.pool.load().pull(addr)
    }

    pub fn replaced_by(&self, new: Self) {
        self.pool.store(new.pool.load_full());
    }
}
impl<K, T> Clone for ConnPool<K, T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

struct PoolInner<K, T> {
    pool: HashMap<K, Mutex<SocketQueue<K, T>>>,
}
impl<K, T: Send + Sync + 'static> PoolInner<K, T>
where
    K: std::hash::Hash + Eq + Clone + Send + 'static,
{
    pub fn empty() -> Self {
        Self {
            pool: Default::default(),
        }
    }

    pub fn new(
        key_connect_pairs: impl Iterator<Item = (K, Arc<dyn Connect<Key = K, Connection = T>>)>,
        heartbeat: Arc<dyn Heartbeat<Connection = T>>,
    ) -> Self {
        let mut pool = HashMap::new();
        key_connect_pairs.for_each(|(k, c)| {
            let mut queue = SocketQueue::new(k.clone(), c, Arc::clone(&heartbeat));
            for _ in 0..QUEUE_LEN {
                queue.spawn_insert(HEARTBEAT_INTERVAL);
            }
            pool.insert(k, Mutex::new(queue));
        });

        Self { pool }
    }

    pub fn pull(&self, key: &K) -> Option<T> {
        let stream = {
            let mut queue = match self.pool.get(key).and_then(|queue| queue.try_lock().ok()) {
                Some(queue) => queue,
                None => return None,
            };
            queue.try_swap(HEARTBEAT_INTERVAL)
        };
        stream
    }
}

struct SocketQueue<K, T> {
    queue: Arc<RwLock<VecDeque<SocketCell<T>>>>,
    task_handles: JoinSet<()>,
    key: K,
    connect: Arc<dyn Connect<Key = K, Connection = T>>,
    heartbeat: Arc<dyn Heartbeat<Connection = T>>,
}

impl<K: Send + Clone + 'static, T: Send + Sync + 'static> SocketQueue<K, T> {
    pub fn new(
        key: K,
        connect: Arc<dyn Connect<Key = K, Connection = T>>,
        heartbeat: Arc<dyn Heartbeat<Connection = T>>,
    ) -> Self {
        Self {
            queue: Default::default(),
            task_handles: Default::default(),
            key,
            connect,
            heartbeat,
        }
    }

    pub fn spawn_insert(&mut self, heartbeat_interval: Duration) {
        let queue = self.queue.clone();
        let connect = Arc::clone(&self.connect);
        let heartbeat = Arc::clone(&self.heartbeat);
        let key = self.key.clone();
        self.task_handles.spawn(async move {
            loop {
                let Some(connection) = connect.connect(&key).await else {
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                };
                let cell =
                    SocketCell::create(connection, Arc::clone(&heartbeat), heartbeat_interval);
                let mut queue = queue.write().unwrap();
                queue.push_back(cell);
                break;
            }
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
                TryTake::Ok(tcp) => Some(tcp),
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
            let ready = self.task_handles.poll_join_next(&mut cx);
            let ready = match ready {
                std::task::Poll::Ready(r) => r,
                std::task::Poll::Pending => panic!(),
            };
            ready.unwrap().expect("Pool task panicked");
        }

        // Replenish
        self.spawn_insert(heartbeat_interval);
        assert_eq!(self.task_handles.len(), QUEUE_LEN);

        res
    }
}

struct SocketCell<T> {
    cell: Arc<TokioRwLock<Option<T>>>,
    _task_handle: JoinSet<()>,
}

impl<T: Send + Sync + 'static> SocketCell<T> {
    pub fn create(
        connection: T,
        heartbeat: Arc<dyn Heartbeat<Connection = T>>,
        heartbeat_interval: Duration,
    ) -> Self {
        let cell = Arc::new(TokioRwLock::new(Some(connection)));
        let mut task_handle = JoinSet::new();
        task_handle.spawn({
            let cell = Arc::clone(&cell);
            async move {
                loop {
                    tokio::time::sleep(heartbeat_interval).await;
                    let mut cell = cell.write().await;
                    let connection = match cell.take() {
                        Some(x) => x,
                        None => break,
                    };
                    let Some(connection) = heartbeat.heartbeat(connection).await else {
                        break;
                    };
                    *cell = Some(connection);
                }
            }
        });
        Self {
            cell,
            _task_handle: task_handle,
        }
    }

    pub fn try_take(&self) -> TryTake<T> {
        let mut cell = match self.cell.try_write() {
            Ok(x) => x,
            Err(_) => return TryTake::Occupied,
        };
        match cell.take() {
            Some(stream) => TryTake::Ok(stream),
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
    use std::net::SocketAddr;

    use tokio::{
        io::AsyncReadExt,
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
                    let mut buf = [0; 1024];
                    loop {
                        if let Err(_e) = stream.read_exact(&mut buf).await {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    struct TcpConnect;
    #[async_trait]
    impl Connect for TcpConnect {
        type Key = SocketAddr;
        type Connection = TcpStream;
        async fn connect(&self, key: &Self::Key) -> Option<Self::Connection> {
            TcpStream::connect(*key).await.ok()
        }
    }
    struct TcpHeartbeat;
    #[async_trait]
    impl Heartbeat for TcpHeartbeat {
        type Connection = TcpStream;
        async fn heartbeat(&self, conn: Self::Connection) -> Option<Self::Connection> {
            Some(conn)
        }
    }

    #[tokio::test]
    async fn take_none() {
        let addr = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        let connect = Arc::new(TcpConnect) as Arc<dyn Connect<Key = _, Connection = _>>;
        let connect = [(addr, connect)];
        let pool = ConnPool::new(connect.into_iter(), Arc::new(TcpHeartbeat));
        let mut join_set = JoinSet::new();
        for _ in 0..100 {
            let pool = pool.clone();
            join_set.spawn(async move {
                let res = pool.pull(&addr);
                assert!(res.is_none());
            });
        }
    }

    #[tokio::test]
    async fn take_some() {
        let addr = spawn_listener().await;
        let connect = Arc::new(TcpConnect) as Arc<dyn Connect<Key = _, Connection = _>>;
        let connect = [(addr, connect)];
        let pool = ConnPool::new(connect.into_iter(), Arc::new(TcpHeartbeat));
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for _ in 0..QUEUE_LEN {
                let res = pool.pull(&addr);
                assert!(res.is_some());
            }
        }
    }
}
