use std::sync::{Arc, Mutex};

use mysql_async::{
    consts::{CapabilityFlags, StatusFlags},
    prelude::Queryable,
    test_misc::get_opts,
    ChangeUserOpts, Compression, Conn, DriverError, Error, OkPacket, OptsBuilder, Pool,
    PoolConstraints, PoolOpts, SessionStateChange,
};

const TRACK_MODE: &str = "SET session_track_system_variables='*',sql_mode='ANSI_QUOTES'";
const SETUP_LIMIT: usize = 65_536;

fn mode(packet: &OkPacket<'_>) -> Option<String> {
    for event in packet.session_state_info().ok()? {
        if let SessionStateChange::SystemVariables(vars) = event.decode().ok()? {
            for var in vars {
                if var.name_bytes() == b"sql_mode" {
                    return Some(String::from_utf8_lossy(var.value_bytes()).into_owned());
                }
            }
        }
    }
    None
}

fn observe_modes(conn: &mut Conn) -> Arc<Mutex<Vec<Option<String>>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    conn.set_session_observer(move |packet| match packet {
        Some(packet) => {
            if let Some(mode) = mode(packet) {
                observed.lock().unwrap().push(Some(mode));
            }
        }
        None => observed.lock().unwrap().push(None),
    });
    events
}

fn observe_packets(conn: &mut Conn) -> Arc<Mutex<Vec<Option<OkPacket<'static>>>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    conn.set_session_observer(move |packet| observed.lock().unwrap().push(packet.cloned()));
    events
}

async fn supports_reset(conn: &mut Conn) -> bool {
    // The parsed version is public, but the MariaDB discriminator is not.
    let version: String = conn.query_first("SELECT VERSION()").await.unwrap().unwrap();
    if version.contains("MariaDB") {
        conn.server_version() >= (10, 2, 4)
    } else {
        conn.server_version() > (5, 7, 2)
    }
}

async fn reset_conn(opts: OptsBuilder) -> Option<Conn> {
    let mut conn = Conn::new(opts).await.unwrap();
    if supports_reset(&mut conn).await {
        Some(conn)
    } else {
        conn.disconnect().await.unwrap();
        None
    }
}

async fn tracking_conn(opts: OptsBuilder) -> Option<Conn> {
    let mut conn = Conn::new(opts.add_capability(CapabilityFlags::CLIENT_SESSION_TRACK))
        .await
        .unwrap();
    // capabilities() is private. Skip servers without tracking support, then
    // verify that requesting CLIENT_SESSION_TRACK actually yields native data.
    let variable: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'session_track_system_variables'")
        .await
        .unwrap();
    if variable.is_none() {
        conn.disconnect().await.unwrap();
        return None;
    }
    conn.query_drop("SET sql_mode=''").await.unwrap();
    conn.query_drop(TRACK_MODE).await.unwrap();
    assert_eq!(
        conn.last_ok_packet().and_then(mode).as_deref(),
        Some("ANSI_QUOTES"),
        "a server exposing session tracking must return the requested mode change"
    );
    conn.query_drop("SET sql_mode=''").await.unwrap();
    Some(conn)
}

fn padded_setup(len: usize) -> String {
    let prefix = "SET @setup_boundary=1 /*";
    let suffix = "*/";
    let sql = format!(
        "{prefix}{}{suffix}",
        "x".repeat(len - prefix.len() - suffix.len())
    );
    assert_eq!(sql.len(), len);
    sql
}

#[tokio::test]
async fn observes_intermediate_changes_and_error_in_order() {
    let Some(mut conn) = tracking_conn(get_opts()).await else {
        return;
    };
    let events = observe_modes(&mut conn);
    conn.query_drop("SET sql_mode='ANSI_QUOTES'; SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 1")
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            Some("ANSI_QUOTES".into()),
            Some("NO_BACKSLASH_ESCAPES".into())
        ]
    );
    events.lock().unwrap().clear();
    assert!(conn
        .query_drop(
            "SET sql_mode='ANSI_QUOTES'; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='tracking test'"
        )
        .await
        .is_err());
    assert_eq!(
        *events.lock().unwrap(),
        vec![Some("ANSI_QUOTES".into()), None]
    );
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn prepare_headers_do_not_become_session_observations() {
    // Request tracking, but still run on MySQL 5.6 with legacy EOF metadata:
    // ordinary OK/EOF observations do not require CLIENT_SESSION_TRACK.
    let mut conn = Conn::new(get_opts().add_capability(CapabilityFlags::CLIENT_SESSION_TRACK))
        .await
        .unwrap();
    conn.query_drop("SET sql_mode='NO_BACKSLASH_ESCAPES'")
        .await
        .unwrap();
    let stable_flags = StatusFlags::SERVER_STATUS_AUTOCOMMIT
        | StatusFlags::SERVER_STATUS_IN_TRANS
        | StatusFlags::SERVER_STATUS_NO_BACKSLASH_ESCAPES;
    let before = conn.status() & stable_flags;
    assert!(before.contains(StatusFlags::SERVER_STATUS_NO_BACKSLASH_ESCAPES));
    let events = observe_packets(&mut conn);
    for i in 0..16 {
        let stmt = conn.prep(format!("SELECT ? AS n /* {i} */")).await.unwrap();
        assert_eq!(conn.status() & stable_flags, before);
        conn.close(stmt).await.unwrap();
    }
    // EOF packets for parameter/column metadata are legitimate observations.
    // A prepare header misread as an ordinary OK instead reports its statement
    // ID as affected_rows and can overwrite the session status flags.
    for packet in events.lock().unwrap().iter() {
        let packet = packet
            .as_ref()
            .expect("prepare must not invalidate observations");
        assert_eq!(packet.affected_rows(), 0);
        assert_eq!(packet.last_insert_id(), None);
        assert_eq!(packet.status_flags() & stable_flags, before);
    }
    events.lock().unwrap().clear();
    conn.query_drop("SET @prepare_observer_probe=1")
        .await
        .unwrap();
    let packets = events.lock().unwrap().clone();
    assert_eq!(packets.len(), 1, "observer must still receive ordinary OKs");
    assert!(packets[0].is_some());
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn reset_setup_restores_mode_and_keeps_connection_usable() {
    for compression in [None, Some(Compression::default())] {
        let Some(mut conn) = tracking_conn(get_opts().compression(compression)).await else {
            continue;
        };
        if !supports_reset(&mut conn).await {
            conn.disconnect().await.unwrap();
            continue;
        }
        let events = observe_modes(&mut conn);
        for _ in 0..5 {
            conn.query_drop("SET @guard_test=1").await.unwrap();
            assert!(conn.reset_with_setup(TRACK_MODE).await.unwrap());
            let row: (String, Option<u8>) = conn
                .query_first("SELECT @@sql_mode, @guard_test")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row, ("ANSI_QUOTES".into(), None));
        }
        let events = events.lock().unwrap().clone();
        assert_eq!(events.iter().filter(|v| v.is_none()).count(), 5);
        assert_eq!(
            events
                .iter()
                .filter(|v| v.as_deref() == Some("ANSI_QUOTES"))
                .count(),
            5
        );
        conn.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn reset_setup_success_and_error_clear_cached_prepares() {
    for compression in [None, Some(Compression::default())] {
        for (setup, succeeds) in [
            ("SET @setup_ok=1", true),
            ("SET sql_mode='INVALID_MODE'", false),
        ] {
            let Some(mut conn) =
                reset_conn(get_opts().compression(compression).stmt_cache_size(8)).await
            else {
                continue;
            };
            let before = conn.prep("SELECT ? AS n").await.unwrap();
            assert_eq!(
                conn.exec_first::<u8, _, _>(&before, (42,)).await.unwrap(),
                Some(42)
            );
            let result = conn.reset_with_setup(setup).await;
            if succeeds {
                assert!(result.unwrap());
                assert_eq!(
                    conn.query_first::<u8, _>("SELECT @setup_ok").await.unwrap(),
                    Some(1)
                );
            } else {
                assert!(matches!(result, Err(Error::Server(_))));
            }
            assert_eq!(
                conn.query_first::<u8, _>("SELECT 1").await.unwrap(),
                Some(1)
            );
            // Reset deleted the server statement, even if setup subsequently
            // failed. Identical SQL must not reuse the stale cache entry.
            let after = conn.prep("SELECT ? AS n").await.unwrap();
            assert_eq!(
                conn.exec_first::<u8, _, _>(&after, (43,)).await.unwrap(),
                Some(43)
            );
            conn.close(after).await.unwrap();
            conn.disconnect().await.unwrap();
        }
    }
}

#[tokio::test]
async fn reset_setup_drains_abandoned_select() {
    for compression in [None, Some(Compression::default())] {
        let Some(mut conn) = reset_conn(get_opts().compression(compression)).await else {
            continue;
        };
        drop(conn.query_iter("SELECT 1").await.unwrap());
        assert!(conn.reset_with_setup("SET @after_drain=42").await.unwrap());
        assert_eq!(
            conn.query_first::<u8, _>("SELECT @after_drain")
                .await
                .unwrap(),
            Some(42)
        );
        conn.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn reset_and_change_user_invalidate_observations() {
    let mut conn = Conn::new(get_opts()).await.unwrap();
    let reset_supported = supports_reset(&mut conn).await;
    let events = observe_packets(&mut conn);
    assert_eq!(conn.reset().await.unwrap(), reset_supported);
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.is_none())
            .count(),
        usize::from(reset_supported)
    );
    events.lock().unwrap().clear();
    // Do not skip change_user on servers too old for COM_RESET_CONNECTION.
    conn.change_user(ChangeUserOpts::default()).await.unwrap();
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.is_none())
            .count(),
        1
    );
    assert_eq!(
        conn.query_first::<u8, _>("SELECT 1").await.unwrap(),
        Some(1)
    );
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn pending_mode_change_is_observed_before_reset_or_change_user_invalidation() {
    #[derive(Debug)]
    enum Operation {
        Reset,
        ResetWithSetup,
        ChangeUser,
    }

    for compression in [None, Some(Compression::default())] {
        for operation in [
            Operation::Reset,
            Operation::ResetWithSetup,
            Operation::ChangeUser,
        ] {
            let Some(mut conn) = tracking_conn(get_opts().compression(compression)).await else {
                continue;
            };
            if !matches!(operation, Operation::ChangeUser) && !supports_reset(&mut conn).await {
                conn.disconnect().await.unwrap();
                continue;
            }
            let events = observe_modes(&mut conn);
            // The SELECT leaves the SET response unread until the next command
            // drains this abandoned result. A bare SET would already be read by
            // query_iter and would not exercise drain-before-invalidation.
            drop(
                conn.query_iter("SELECT 1; SET sql_mode='ANSI_QUOTES'")
                    .await
                    .unwrap(),
            );
            assert!(events.lock().unwrap().is_empty());
            match operation {
                Operation::Reset => assert!(conn.reset().await.unwrap()),
                Operation::ResetWithSetup => {
                    assert!(conn.reset_with_setup("SET @after_drain=1").await.unwrap())
                }
                Operation::ChangeUser => conn.change_user(ChangeUserOpts::default()).await.unwrap(),
            }
            let recorded = events.lock().unwrap().clone();
            assert!(
                recorded.starts_with(&[Some("ANSI_QUOTES".into()), None]),
                "{operation:?}: {recorded:?}"
            );
            assert_eq!(recorded.iter().filter(|p| p.is_none()).count(), 1);
            assert_eq!(
                conn.query_first::<u8, _>("SELECT 1").await.unwrap(),
                Some(1)
            );
            conn.disconnect().await.unwrap();
        }
    }
}

#[tokio::test]
async fn setup_result_sets_and_multiple_statements_close_the_connection() {
    for compression in [None, Some(Compression::default())] {
        for sql in ["SELECT 1", "SET @a=1; SET @b=2"] {
            let Some(mut conn) = reset_conn(get_opts().compression(compression)).await else {
                continue;
            };
            assert!(conn.reset_with_setup(sql).await.is_err(), "{sql}");
            assert!(conn.is_disconnected(), "{sql}");
            conn.disconnect().await.unwrap();
        }
    }
}

#[tokio::test]
async fn oversized_setup_has_no_side_effects() {
    for compression in [None, Some(Compression::default())] {
        let mut conn = Conn::new(get_opts().compression(compression).stmt_cache_size(8))
            .await
            .unwrap();
        let reset_supported = supports_reset(&mut conn).await;
        let stmt = conn.prep("SELECT ? AS n").await.unwrap();
        conn.query_drop("SET @oversized_guard=42,sql_mode='ANSI_QUOTES'")
            .await
            .unwrap();
        let events = observe_packets(&mut conn);
        drop(
            conn.query_iter("SELECT 1; SET @oversized_pending=43")
                .await
                .unwrap(),
        );
        let before_events = events.lock().unwrap().clone();
        let before_status = conn.status();
        let before_ok = conn.last_ok_packet().cloned();
        let connection_id = conn.id();
        let result = conn.reset_with_setup(&padded_setup(SETUP_LIMIT + 1)).await;
        if reset_supported {
            assert!(matches!(
                result,
                Err(Error::Driver(DriverError::PacketTooLarge))
            ));
        } else {
            // Unsupported reset is a no-op, regardless of the setup text.
            assert!(!result.unwrap());
        }
        assert!(!conn.is_disconnected());
        assert_eq!(conn.id(), connection_id);
        assert_eq!(conn.status(), before_status);
        assert_eq!(conn.last_ok_packet(), before_ok.as_ref());
        assert_eq!(
            *events.lock().unwrap(),
            before_events,
            "rejection must not drain or invalidate"
        );
        let row: (u8, u8, String, Option<u8>) = conn
            .query_first("SELECT @oversized_guard, @oversized_pending, @@sql_mode, @setup_boundary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, (42, 43, "ANSI_QUOTES".into(), None));
        assert_eq!(
            conn.exec_first::<u8, _, _>(&stmt, (44,)).await.unwrap(),
            Some(44)
        );
        let cached = conn.prep("SELECT ? AS n").await.unwrap();
        assert_eq!(
            cached.id(),
            stmt.id(),
            "rejection must preserve the statement cache"
        );
        assert_eq!(
            conn.exec_first::<u8, _, _>(&cached, (45,)).await.unwrap(),
            Some(45)
        );
        let recorded = events.lock().unwrap().clone();
        assert!(
            recorded.len() > before_events.len(),
            "observer must remain installed"
        );
        assert!(recorded.iter().all(Option::is_some));
        conn.close(stmt).await.unwrap();
        conn.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn setup_accepts_exactly_65536_bytes() {
    for compression in [None, Some(Compression::default())] {
        let Some(mut conn) = reset_conn(get_opts().compression(compression)).await else {
            continue;
        };
        conn.query_drop("SET @boundary_guard=42").await.unwrap();
        assert!(conn
            .reset_with_setup(&padded_setup(SETUP_LIMIT))
            .await
            .unwrap());
        let row: (Option<u8>, u8) = conn
            .query_first("SELECT @boundary_guard, @setup_boundary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, (None, 1));
        conn.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn unsupported_reset_returns_false_without_executing_setup() {
    for compression in [None, Some(Compression::default())] {
        let mut conn = Conn::new(get_opts().compression(compression))
            .await
            .unwrap();
        if supports_reset(&mut conn).await {
            conn.disconnect().await.unwrap();
            continue;
        }
        let stmt = conn.prep("SELECT ? AS n").await.unwrap();
        conn.query_drop("SET @unsupported_guard=42").await.unwrap();
        let events = observe_packets(&mut conn);
        assert!(!conn
            .reset_with_setup("SET @unsupported_guard=99")
            .await
            .unwrap());
        assert!(events.lock().unwrap().is_empty());
        assert_eq!(
            conn.query_first::<u8, _>("SELECT @unsupported_guard")
                .await
                .unwrap(),
            Some(42)
        );
        assert_eq!(
            conn.exec_first::<u8, _, _>(&stmt, (43,)).await.unwrap(),
            Some(43)
        );
        events.lock().unwrap().clear();
        conn.change_user(ChangeUserOpts::default()).await.unwrap();
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.is_none())
                .count(),
            1
        );
        assert_eq!(
            conn.query_first::<Option<u8>, _>("SELECT @unsupported_guard")
                .await
                .unwrap(),
            Some(None)
        );
        conn.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn clear_session_observer_stops_callbacks_and_releases_capture() {
    let mut conn = Conn::new(get_opts()).await.unwrap();
    let events = observe_packets(&mut conn);
    conn.query_drop("SET @observer_probe=1").await.unwrap();
    assert!(!events.lock().unwrap().is_empty());
    events.lock().unwrap().clear();
    conn.clear_session_observer();
    conn.clear_session_observer(); // Clearing an empty slot is harmless.
    assert_eq!(Arc::strong_count(&events), 1);
    conn.query_drop("SET @observer_probe=2").await.unwrap();
    conn.change_user(ChangeUserOpts::default()).await.unwrap();
    assert!(events.lock().unwrap().is_empty());
    let replacement = observe_packets(&mut conn);
    conn.query_drop("SET @observer_probe=3").await.unwrap();
    assert!(!replacement.lock().unwrap().is_empty());
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn pool_return_clears_observer_with_and_without_reset() {
    for reset in [true, false] {
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(1, 1).unwrap())
            .with_reset_connection(reset);
        let pool = Pool::new(get_opts().pool_opts(pool_opts));
        let mut conn = pool.get_conn().await.unwrap();
        let connection_id = conn.id();
        let events = observe_packets(&mut conn);
        conn.query_drop("SET @pool_guard=42").await.unwrap();
        assert!(!events.lock().unwrap().is_empty());
        // Pool cleanup must not notify the previous borrower, even while
        // draining its abandoned results or resetting the connection.
        drop(
            conn.query_iter("SELECT 1; SET @pool_pending=43")
                .await
                .unwrap(),
        );
        events.lock().unwrap().clear();
        drop(conn);
        let mut conn = pool.get_conn().await.unwrap();
        assert_eq!(
            conn.id(),
            connection_id,
            "max-one pool should reuse the connection"
        );
        assert_eq!(
            Arc::strong_count(&events),
            1,
            "pool must release the observer capture"
        );
        let row: (Option<u8>, Option<u8>) = conn
            .query_first("SELECT @pool_guard, @pool_pending")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row,
            if reset {
                (None, None)
            } else {
                (Some(42), Some(43))
            }
        );
        assert!(
            events.lock().unwrap().is_empty(),
            "old borrower received callbacks, reset={reset}"
        );
        let next_events = observe_packets(&mut conn);
        conn.query_drop("SET @new_borrower=1").await.unwrap();
        assert!(!next_events.lock().unwrap().is_empty());
        assert!(events.lock().unwrap().is_empty());
        drop(conn);
        pool.disconnect().await.unwrap();
        assert_eq!(Arc::strong_count(&next_events), 1);
    }
}

#[cfg(any(feature = "native-tls-tls", feature = "rustls-tls"))]
#[tokio::test]
async fn reset_setup_works_over_tls() {
    // Probe before forcing TLS: older matrix servers may lack both reset and
    // usable TLS. Keep DATABASE_URL, SSL, and other get_opts settings intact.
    let mut probe = Conn::new(get_opts()).await.unwrap();
    let supported = supports_reset(&mut probe).await;
    let tls: Option<(String, String)> = probe
        .query_first("SHOW VARIABLES LIKE 'have_ssl'")
        .await
        .unwrap();
    probe.disconnect().await.unwrap();
    // Older MariaDB fixtures can support reset but have TLS disabled. Newer
    // MySQL removes have_ssl, so absence alone must not skip TLS coverage.
    if !supported || tls.is_some_and(|(_, value)| value != "YES") {
        return;
    }
    let ssl = mysql_async::SslOpts::default()
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true);
    let Some(mut conn) = tracking_conn(get_opts().prefer_socket(false).ssl_opts(ssl)).await else {
        return;
    };
    assert!(conn.reset_with_setup(TRACK_MODE).await.unwrap());
    assert_eq!(
        conn.query_first::<String, _>("SELECT @@sql_mode")
            .await
            .unwrap()
            .as_deref(),
        Some("ANSI_QUOTES")
    );
    conn.disconnect().await.unwrap();
}
