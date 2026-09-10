use std::sync::{Arc, Mutex};

use mysql_async::{
    consts::CapabilityFlags, prelude::Queryable, test_misc::get_opts, ChangeUserOpts, Conn,
    OkPacket, SessionStateChange,
};

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

#[tokio::test]
async fn observes_intermediate_changes_and_error_in_order() {
    let mut conn = Conn::new(get_opts().add_capability(CapabilityFlags::CLIENT_SESSION_TRACK))
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    conn.set_session_observer(move |packet| {
        if let Some(packet) = packet {
            if let Some(mode) = mode(packet) {
                observed.lock().unwrap().push(Some(mode));
            }
        } else {
            observed.lock().unwrap().push(None);
        }
    });
    conn.query_drop("SET session_track_system_variables='*',sql_mode='ANSI_QUOTES'; SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 1")
        .await.unwrap();
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
    let mut conn = Conn::new(get_opts().add_capability(CapabilityFlags::CLIENT_SESSION_TRACK))
        .await
        .unwrap();
    conn.query_drop("SET session_track_system_variables='*',sql_mode='ANSI_QUOTES'")
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(0));
    let observed = events.clone();
    conn.set_session_observer(move |_| *observed.lock().unwrap() += 1);
    for i in 0..16 {
        let stmt = conn.prep(format!("SELECT ? AS n /* {i} */")).await.unwrap();
        conn.close(stmt).await.unwrap();
    }
    assert_eq!(*events.lock().unwrap(), 0);
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn reset_setup_restores_mode_and_keeps_connection_usable() {
    for compression in [None, Some(mysql_async::Compression::default())] {
        let opts = get_opts()
            .add_capability(CapabilityFlags::CLIENT_SESSION_TRACK)
            .compression(compression);
        let mut conn = Conn::new(opts).await.unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        conn.set_session_observer(move |packet| {
            if let Some(packet) = packet {
                if let Some(mode) = mode(packet) {
                    observed.lock().unwrap().push(Some(mode));
                }
            } else {
                observed.lock().unwrap().push(None);
            }
        });
        for _ in 0..5 {
            conn.query_drop("SET @guard_test=1").await.unwrap();
            conn.reset_with_setup("SET session_track_system_variables='*',sql_mode='ANSI_QUOTES'")
                .await
                .unwrap();
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
async fn setup_error_does_not_desynchronize_following_queries() {
    let mut conn = Conn::new(get_opts()).await.unwrap();
    assert!(conn
        .reset_with_setup("SET sql_mode='INVALID_MODE'")
        .await
        .is_err());
    assert_eq!(
        conn.query_first::<u8, _>("SELECT 1").await.unwrap(),
        Some(1)
    );
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn reset_and_change_user_invalidate_observations() {
    let mut conn = Conn::new(get_opts()).await.unwrap();
    let invalidations = Arc::new(Mutex::new(0));
    let observed = invalidations.clone();
    conn.set_session_observer(move |packet| {
        if packet.is_none() {
            *observed.lock().unwrap() += 1;
        }
    });
    conn.reset().await.unwrap();
    conn.change_user(ChangeUserOpts::default()).await.unwrap();
    assert_eq!(*invalidations.lock().unwrap(), 2);
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn setup_result_sets_close_the_connection() {
    for sql in ["SELECT 1", "SET @a=1; SET @b=2"] {
        let mut conn = Conn::new(get_opts()).await.unwrap();
        assert!(conn.reset_with_setup(sql).await.is_err());
        assert!(conn.is_disconnected());
    }
}

#[cfg(feature = "native-tls-tls")]
#[tokio::test]
async fn reset_setup_works_over_tls() {
    let ssl = mysql_async::SslOpts::default()
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true);
    let mut conn = Conn::new(
        get_opts()
            .ssl_opts(ssl)
            .add_capability(CapabilityFlags::CLIENT_SESSION_TRACK),
    )
    .await
    .unwrap();
    conn.reset_with_setup("SET session_track_system_variables='*',sql_mode='ANSI_QUOTES'")
        .await
        .unwrap();
    assert_eq!(
        conn.query_first::<String, _>("SELECT @@sql_mode")
            .await
            .unwrap()
            .as_deref(),
        Some("ANSI_QUOTES")
    );
    conn.disconnect().await.unwrap();
}
