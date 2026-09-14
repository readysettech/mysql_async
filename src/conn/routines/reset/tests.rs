use std::{
    io::ErrorKind,
    sync::{Arc, Mutex},
    time::Duration,
};

use mysql_common::constants::{CapabilityFlags, Command};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};

use crate::{
    io::Stream, opts::HostPortOrUrl, prelude::Queryable, queryable::stmt::StmtInner, Conn,
    DriverError, Error,
};

const SETUP: &str = "SET @reset_test = 1";
const DEADLINE: Duration = Duration::from_secs(5);

async fn connection_pair() -> (Conn, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let host = HostPortOrUrl::HostPort {
        host: addr.ip().to_string(),
        port: addr.port(),
        resolved_ips: Some(vec![addr.ip()]),
    };
    let (client, server) = tokio::join!(Stream::connect_tcp(&host, None), listener.accept());
    let mut conn = Conn::empty(Default::default());
    conn.inner.stream = Some(client.unwrap());
    conn.inner.version = (8, 4, 0);
    conn.inner.capabilities = CapabilityFlags::CLIENT_PROTOCOL_41;
    conn.inner.handshake_complete = true;
    (conn, server.unwrap().0)
}

// Each pipelined command starts a separate exchange, so both replies use seq 1.
fn reply(payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes()[..3]);
    bytes.push(1);
    bytes.extend_from_slice(payload);
    bytes
}

fn ok(affected_rows: u8) -> Vec<u8> {
    vec![0, affected_rows, 0, 2, 0, 0, 0]
}

fn server_error(code: u16) -> Vec<u8> {
    let mut payload = vec![0xff];
    payload.extend_from_slice(&code.to_le_bytes());
    payload.extend_from_slice(b"#HY000test error");
    payload
}

async fn expect_command(server: &mut TcpStream, command: Command, data: &[u8]) {
    let mut header = [0; 4];
    server.read_exact(&mut header).await.unwrap();
    assert_eq!(header[3], 0, "{command:?} must start at sequence zero");
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    assert_eq!(len, data.len() + 1);
    let mut payload = vec![0; len];
    server.read_exact(&mut payload).await.unwrap();
    assert_eq!(payload[0], command as u8);
    assert_eq!(&payload[1..], data);
}

async fn expect_pipeline(server: &mut TcpStream) {
    // Do not reply until both commands have arrived.
    expect_command(server, Command::COM_RESET_CONNECTION, b"").await;
    expect_command(server, Command::COM_QUERY, SETUP.as_bytes()).await;
}

async fn expect_closed(server: &mut TcpStream) {
    let result = server.read(&mut [0]).await;
    assert!(
        matches!(result, Ok(0))
            || matches!(&result, Err(err) if err.kind() == ErrorKind::ConnectionReset),
        "expected closed transport, got {result:?}"
    );
}

fn seed_statement_cache(conn: &mut Conn) {
    // COM_STMT_PREPARE_OK: statement 1, no parameters or columns.
    let stmt = StmtInner::from_payload(
        &[0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        conn.id(),
        Arc::from(&b"DO 1"[..]),
    )
    .unwrap();
    conn.cache_stmt(&Arc::new(stmt));
    assert_eq!(conn.stmt_cache_ref().len(), 1);
}

#[tokio::test]
async fn pipeline_server_errors_drain_both_replies_and_allow_ping() {
    for setup_fails in [false, true] {
        timeout(DEADLINE, async {
            let (mut conn, mut server) = connection_pair().await;
            seed_statement_cache(&mut conn);
            let observations = Arc::new(Mutex::new(Vec::new()));
            let observed = observations.clone();
            conn.set_session_observer(move |ok| {
                observed
                    .lock()
                    .unwrap()
                    .push(ok.map(|ok| ok.affected_rows()));
            });
            let client = async {
                let err = conn.reset_with_setup(SETUP).await.unwrap_err();
                match err {
                    Error::Server(err) => {
                        assert_eq!(err.code, if setup_fails { 1002 } else { 1001 });
                    }
                    err => panic!("expected server error, got {err:?}"),
                }
                assert!(!conn.is_disconnected());
                assert_eq!(conn.stmt_cache_ref().len(), 1, "reset was not acknowledged");
                let expected = if setup_fails {
                    vec![None, None, None]
                } else {
                    // Setup OK is observed, then invalidated because reset failed.
                    vec![None, None, Some(7), None]
                };
                assert_eq!(*observations.lock().unwrap(), expected);
                conn.ping().await.unwrap();
                assert_eq!(conn.affected_rows(), 11, "ping must read its own reply");
                conn.disconnect().await.unwrap();
            };
            let peer = async {
                expect_pipeline(&mut server).await;
                let setup = if setup_fails {
                    server_error(1002)
                } else {
                    ok(7)
                };
                let responses = [reply(&server_error(1001)), reply(&setup)].concat();
                // Coalesce to exercise preservation of buffered data during codec swap.
                server.write_all(&responses).await.unwrap();
                expect_command(&mut server, Command::COM_PING, b"").await;
                server.write_all(&reply(&ok(11))).await.unwrap();
                expect_command(&mut server, Command::COM_QUIT, b"").await;
                expect_closed(&mut server).await;
            };
            tokio::join!(client, peer);
        })
        .await
        .expect("server-error exchange timed out");
    }
}

#[tokio::test]
async fn pipeline_protocol_faults_close_instead_of_returning_setup_error() {
    for malformed_reset in [true, false] {
        timeout(DEADLINE, async {
            let (mut conn, mut server) = connection_pair().await;
            let invalid = if malformed_reset { vec![0] } else { vec![1] };
            let client = async {
                let err = conn.reset_with_setup(SETUP).await.unwrap_err();
                match err {
                    Error::Driver(DriverError::UnexpectedPacket { payload }) => {
                        assert_eq!(payload, invalid);
                    }
                    err => panic!("expected protocol error, got {err:?}"),
                }
                assert!(conn.is_disconnected());
                assert!(conn.inner.stream.is_none());
                drop(conn);
            };
            let peer = async {
                expect_pipeline(&mut server).await;
                let responses = if malformed_reset {
                    // A following setup ERR must not mask a truncated reset OK.
                    [reply(&invalid), reply(&server_error(1002))].concat()
                } else {
                    // Column count starts a result set, which setup does not permit.
                    [reply(&ok(0)), reply(&invalid)].concat()
                };
                server.write_all(&responses).await.unwrap();
                expect_closed(&mut server).await;
            };
            tokio::join!(client, peer);
        })
        .await
        .expect("protocol-fault exchange timed out");
    }
}

#[tokio::test]
async fn cancellation_after_reset_ok_marks_disconnected_and_clears_cache() {
    timeout(DEADLINE, async {
        let (mut conn, mut server) = connection_pair().await;
        seed_statement_cache(&mut conn);
        let (reset_seen, received_reset) = oneshot::channel();
        let reset_seen = Mutex::new(Some(reset_seen));
        conn.set_session_observer(move |ok| {
            if ok.is_some() {
                reset_seen.lock().unwrap().take().unwrap().send(()).unwrap();
            }
        });
        let client = async {
            let mut reset = Box::pin(conn.reset_with_setup(SETUP));
            tokio::select! {
                result = &mut reset => panic!("setup has no response: {result:?}"),
                result = received_reset => result.unwrap(),
            }
            // The reset future has processed OK and yielded waiting for setup.
            drop(reset);
            assert!(conn.is_disconnected());
            assert_eq!(conn.stmt_cache_ref().len(), 0);
            conn.clear_session_observer();
            // Cancellation marks the connection unusable but can leave its stream owned.
            if let Some(stream) = conn.inner.stream.take() {
                stream.close().await.unwrap();
            }
            drop(conn);
        };
        let peer = async {
            expect_pipeline(&mut server).await;
            server.write_all(&reply(&ok(0))).await.unwrap();
            // Withhold setup forever; cancellation must not depend on a sleep.
            expect_closed(&mut server).await;
        };
        tokio::join!(client, peer);
    })
    .await
    .expect("cancellation exchange timed out");
}

#[tokio::test]
async fn eof_between_reset_and_setup_marks_disconnected() {
    timeout(DEADLINE, async {
        let (mut conn, mut server) = connection_pair().await;
        seed_statement_cache(&mut conn);
        let client = async {
            let err = conn.reset_with_setup(SETUP).await.unwrap_err();
            assert!(matches!(err, Error::Io(_)), "expected I/O error: {err:?}");
            assert!(conn.is_disconnected());
            assert!(conn.inner.stream.is_none());
            assert_eq!(conn.stmt_cache_ref().len(), 0);
            drop(conn);
        };
        let peer = async {
            expect_pipeline(&mut server).await;
            server.write_all(&reply(&ok(0))).await.unwrap();
            server.shutdown().await.unwrap();
            expect_closed(&mut server).await;
        };
        tokio::join!(client, peer);
    })
    .await
    .expect("EOF exchange timed out");
}
