extern crate rocket_contrib;
extern crate tiberius;
extern crate tokio;
extern crate tokio_util;
extern crate log;
extern crate futures;
extern crate snafu;

use futures::{AsyncRead, AsyncWrite};
use rocket_contrib::{
    databases::{
        r2d2, 
        DbError, 
        DatabaseConfig, 
        Poolable,
    }
};

trait ConnectionImplementation: AsyncRead + AsyncWrite + Unpin + Send {}
impl ConnectionImplementation for tokio_util::compat::Compat<tokio::net::TcpStream> {}

/// A wrapper around the actual Database connection
pub struct Connection {
    inner: tiberius::Client<Box<dyn ConnectionImplementation>>
}

impl Connection {
    /// Utility function providing a blocking way to use the underlying `execute` function
    pub fn execute<'a>(
        &mut self,
        query: impl Into<std::borrow::Cow<'a, str>>,
        params: &[&dyn tiberius::ToSql],
    ) -> tiberius::Result<tiberius::ExecuteResult> {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                self
                    .inner
                    .execute(query, params)
                    .await
            })
            
    }


    /// Utility function providing a blocking way to use the underlying `query` function
    pub fn query<'a, 'b>(
        &'a mut self,
        query: impl Into<std::borrow::Cow<'b, str>>,
        params: &'b [&'b dyn tiberius::ToSql],
    ) -> tiberius::Result<Vec<Vec<tiberius::Row>>>
        where 'a: 'b
    {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let qr = self
                    .inner
                    .query(query, params)
                    .await;

                match qr {
                    Err(e) => Err(e),
                    Ok(qr) => {
                        qr
                            .into_results()
                            .await                  
                    }
                }
            })
    }
}

/// The manager for a MSSQL connection pool
pub struct ConnectionManager {
    config: tiberius::Config,
    runtime: tokio::runtime::Runtime,
}

impl ConnectionManager {
    pub fn new(cfg: &DatabaseConfig) -> Result<Self, Error> {
        use tiberius::{
            Config,
            AuthMethod,
        };

        let mut config = Config::new();
        config.host(cfg.url);

        let port = cfg
            .extras
            .get("port")
            .ok_or(MissingConfig { field: "port"}.build())
            .and_then(|s| {
                s
                    .as_integer()                
                    .map(|i| i as u16)
                    .ok_or(InvalidConfig { field: "port" }.build())
            })?;

        config.port(port);

        let user = cfg
            .extras
            .get("user")
            .ok_or(MissingConfig { field: "user" }.build())
            .and_then(|v| {
                v
                    .as_str()
                    .ok_or(InvalidConfig { field: "user" }.build())
            })?;

        let pass = cfg
            .extras
            .get("pwd")
            .ok_or(MissingConfig { field: "pwd" }.build())
            .and_then(|v| {
                v
                    .as_str()
                    .ok_or(InvalidConfig { field: "pwd" }.build())
            })?;

        config.authentication(AuthMethod::sql_server(user, pass));
        config.trust_cert();

        Ok(Self {
            config,
            runtime: tokio::runtime::Runtime::new().unwrap(),
        })
    }
}

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    Tiberius { inner: tiberius::error::Error },
    #[snafu(display("Missing Configuration field [field: {}]", field))]
    MissingConfig { field: String },
    #[snafu(display("Invalid Configuration field value [field: {}]", field))]
    InvalidConfig { field: String },
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;

    type Error = Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        use tokio_util::compat::TokioAsyncWriteCompatExt;

        let conn: tiberius::Client<Box<dyn ConnectionImplementation>> = self.runtime.block_on(async move {
            let tcp = std::net::TcpStream::connect(self.config.get_addr()).unwrap();
            tcp.set_nodelay(true).unwrap();
    
            let compat_stream = tokio::net::TcpStream::from_std(tcp).unwrap().compat_write();
    
            let boxed_stream: Box<dyn ConnectionImplementation> = Box::new(compat_stream);

            tiberius::Client::connect(self.config.clone(), boxed_stream).await.unwrap()
        });


        Ok(Self::Connection {
            inner: conn
        })

    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.runtime.block_on(async move {
            conn
                .inner
                .execute("SELECT 1;", &[])
                .await
                .map(|_| ())
                .map_err(|err| Error::Tiberius {inner: err})
        })
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

impl Poolable for Connection {
    type Manager = ConnectionManager;
    type Error = DbError<Error>;

    fn pool(config: DatabaseConfig) -> Result<r2d2::Pool<Self::Manager>, Self::Error> {
        let manager = ConnectionManager::new(&config)
            .map_err(DbError::Custom)?;

        r2d2::Pool::builder()
            .max_size(config.pool_size)
            .build(manager)
            .map_err(DbError::PoolError)
    }
}
