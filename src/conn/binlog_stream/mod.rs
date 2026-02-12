// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use mysql_common::{
    binlog::{
        consts::{BinlogVersion::Version4, EventType},
        events::{Event, TableMapEvent, TransactionPayloadEvent},
        EventStreamReader,
    },
    io::ParseBuf,
    packets::{ComRegisterSlave, ErrPacket, NetworkStreamTerminator, OkPacketDeserializer},
};

use std::{
    future::Future,
    io::{Cursor, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{connection_like::ConnectionInner, queryable::Queryable};
use crate::{error::DriverError, io::ReadPacket, Conn, Error, IoError, Result};

use self::request::BinlogStreamRequest;

pub mod request;

impl super::Conn {
    /// Turns this connection into a binlog stream.
    ///
    /// You can use SHOW BINARY LOGS to get the current logfile and position from the master.
    /// If the request’s filename is empty, the server will send the binlog-stream of the first known binlog.
    pub async fn get_binlog_stream(
        mut self,
        request: BinlogStreamRequest<'_>,
    ) -> Result<BinlogStream> {
        self.request_binlog(request).await?;

        Ok(BinlogStream::new(self))
    }

    async fn register_as_slave(
        &mut self,
        com_register_slave: ComRegisterSlave<'_>,
    ) -> crate::Result<()> {
        self.query_drop("SET @master_binlog_checksum='ALL'").await?;
        self.write_command(&com_register_slave).await?;

        // Server will respond with OK.
        self.read_packet().await?;

        Ok(())
    }

    async fn request_binlog(&mut self, request: BinlogStreamRequest<'_>) -> crate::Result<()> {
        self.register_as_slave(request.register_slave).await?;
        self.write_command(&request.binlog_request.as_cmd()).await?;
        Ok(())
    }
}

/// Binlog event stream.
///
/// Stream initialization is lazy, i.e. binlog won't be requested until this stream is polled.
pub struct BinlogStream {
    read_packet: ReadPacket<'static, 'static>,
    esr: EventStreamReader,
    // TODO: Use 'static reader here (requires impl on the mysql_common side).
    /// Uncompressed Transaction_payload_event we are iterating over (if any).
    tpe: Option<Cursor<Vec<u8>>>,
}

impl BinlogStream {
    /// `conn` is a `Conn` with `request_binlog` executed on it.
    pub(super) fn new(conn: Conn) -> Self {
        BinlogStream {
            read_packet: ReadPacket::new(conn),
            esr: EventStreamReader::new(Version4),
            tpe: None,
        }
    }

    /// Returns a table map event for the given table id.
    pub fn get_tme(&self, table_id: u64) -> Option<&TableMapEvent<'static>> {
        self.esr.get_tme(table_id)
    }

    /// Closes the stream's `Conn`. Additionally, the connection is dropped, so its associated
    /// pool (if any) will regain a connection slot.
    pub async fn close(self) -> Result<()> {
        match self.read_packet.0.inner {
            // `close_conn` requires ownership of `Conn`. That's okay, because
            // `BinLogStream`'s connection is always owned.
            ConnectionInner::Conn(conn) => {
                if let Err(Error::Io(IoError::Io(ref error))) = conn.close_conn().await {
                    // If the binlog was requested with the flag BINLOG_DUMP_NON_BLOCK,
                    // the connection's file handler will already have been closed (EOF).
                    if error.kind() == ErrorKind::BrokenPipe {
                        return Ok(());
                    }
                }
            }
            ConnectionInner::ConnMut(_) => {}
            ConnectionInner::Tx(_) => {}
        }

        Ok(())
    }
}

impl futures_core::stream::Stream for BinlogStream {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        {
            let Self {
                ref mut tpe,
                ref mut esr,
                ..
            } = *self;

            if let Some(tpe) = tpe.as_mut() {
                match esr.read_decompressed(tpe) {
                    Ok(Some(event)) => return Poll::Ready(Some(Ok(event))),
                    Ok(None) => self.tpe = None,
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                }
            }
        }

        let packet = match ready!(Pin::new(&mut self.read_packet).poll(cx)) {
            Ok(packet) => packet,
            Err(err) => return Poll::Ready(Some(Err(err.into()))),
        };

        let first_byte = packet.first().copied();

        if first_byte == Some(255) {
            if let Ok(ErrPacket::Error(err)) =
                ParseBuf(&packet).parse(self.read_packet.conn_ref().capabilities())
            {
                return Poll::Ready(Some(Err(From::from(err))));
            }
        }

        if first_byte == Some(254)
            && packet.len() < 8
            && ParseBuf(&packet)
                .parse::<OkPacketDeserializer<NetworkStreamTerminator>>(
                    self.read_packet.conn_ref().capabilities(),
                )
                .is_ok()
        {
            return Poll::Ready(None);
        }

        if first_byte == Some(0) {
            let event_data = &packet[1..];
            match self.esr.read(event_data) {
                Ok(Some(event)) => {
                    if event.header().event_type_raw() == EventType::TRANSACTION_PAYLOAD_EVENT as u8
                    {
                        #[allow(clippy::single_match)]
                        match event.read_event::<TransactionPayloadEvent<'_>>() {
                            Ok(e) => self.tpe = Some(Cursor::new(e.danger_decompress())),
                            Err(_) => (/* TODO: Log the error */),
                        }
                    }
                    Poll::Ready(Some(Ok(event)))
                }
                Ok(None) => Poll::Ready(None),
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            }
        } else {
            Poll::Ready(Some(Err(DriverError::UnexpectedPacket {
                payload: packet.to_vec(),
            }
            .into())))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::StreamExt;
    use mysql_common::binlog::{consts::EventType, events::EventData};
    use tokio::time::timeout;

    use crate::prelude::*;
    use crate::{test_misc::get_opts, *};

    async fn gen_dummy_data(conn: &mut Conn) -> super::Result<()> {
        "CREATE TABLE IF NOT EXISTS customers (customer_id int not null)"
            .ignore(&mut *conn)
            .await?;

        let mut tx = conn.start_transaction(Default::default()).await?;
        for i in 0_u8..100 {
            "INSERT INTO customers(customer_id) VALUES (?)"
                .with((i,))
                .ignore(&mut tx)
                .await?;
        }
        tx.commit().await?;

        "DROP TABLE customers".ignore(conn).await?;

        Ok(())
    }

    async fn create_binlog_stream_conn(pool: Option<&Pool>) -> super::Result<(Conn, Vec<u8>, u64)> {
        let mut conn = match pool {
            None => Conn::new(get_opts()).await.unwrap(),
            Some(pool) => pool.get_conn().await.unwrap(),
        };

        if conn.server_version() >= (8, 0, 31) && conn.server_version() < (9, 0, 0) {
            let _ = "SET binlog_transaction_compression=ON"
                .ignore(&mut conn)
                .await;
        }

        if let Ok(Some(gtid_mode)) = "SELECT @@GLOBAL.GTID_MODE"
            .first::<String, _>(&mut conn)
            .await
        {
            if !gtid_mode.starts_with("ON") {
                panic!(
                    "GTID_MODE is disabled \
                        (enable using --gtid_mode=ON --enforce_gtid_consistency=ON)"
                );
            }
        }

        let row: crate::Row = "SHOW BINARY LOGS".first(&mut conn).await.unwrap().unwrap();
        let filename = row.get(0).unwrap();
        let position = row.get(1).unwrap();

        gen_dummy_data(&mut conn).await.unwrap();
        Ok((conn, filename, position))
    }

    #[tokio::test]
    async fn should_read_binlog() -> super::Result<()> {
        read_binlog_streams_and_close_their_connections(None, (12, 13, 14))
            .await
            .unwrap();

        let pool = Pool::new(get_opts());
        read_binlog_streams_and_close_their_connections(Some(&pool), (15, 16, 17))
            .await
            .unwrap();

        // Disconnecting the pool verifies that closing the binlog connections
        // left the pool in a sane state.
        timeout(Duration::from_secs(10), pool.disconnect())
            .await
            .unwrap()
            .unwrap();

        Ok(())
    }

    async fn read_binlog_streams_and_close_their_connections(
        pool: Option<&Pool>,
        binlog_server_ids: (u32, u32, u32),
    ) -> super::Result<()> {
        // iterate using COM_BINLOG_DUMP
        let (conn, filename, pos) = create_binlog_stream_conn(pool).await.unwrap();
        let is_mariadb = conn.inner.is_mariadb;

        let mut binlog_stream = conn
            .get_binlog_stream(
                BinlogStreamRequest::new(binlog_server_ids.0)
                    .with_filename(&filename)
                    .with_pos(pos),
            )
            .await
            .unwrap();

        let mut events_num = 0;
        while let Ok(Some(event)) = timeout(Duration::from_secs(10), binlog_stream.next()).await {
            let event = event.unwrap();
            events_num += 1;

            // assert that event type is known
            event.header().event_type().unwrap();

            // iterate over rows of an event
            if let EventData::RowsEvent(re) = event.read_data()?.unwrap() {
                let tme = binlog_stream.get_tme(re.table_id());
                for row in re.rows(tme.unwrap()) {
                    row.unwrap();
                }
            }
        }
        assert!(events_num > 0);
        timeout(Duration::from_secs(10), binlog_stream.close())
            .await
            .unwrap()
            .unwrap();

        if !is_mariadb {
            // iterate using COM_BINLOG_DUMP_GTID
            let (conn, filename, pos) = create_binlog_stream_conn(pool).await.unwrap();

            let mut binlog_stream = conn
                .get_binlog_stream(
                    BinlogStreamRequest::new(binlog_server_ids.1)
                        .with_gtid()
                        .with_filename(&filename)
                        .with_pos(pos),
                )
                .await
                .unwrap();

            events_num = 0;
            while let Ok(Some(event)) = timeout(Duration::from_secs(10), binlog_stream.next()).await
            {
                let event = event.unwrap();
                events_num += 1;

                // assert that event type is known
                event.header().event_type().unwrap();

                // iterate over rows of an event
                if let EventData::RowsEvent(re) = event.read_data()?.unwrap() {
                    let tme = binlog_stream.get_tme(re.table_id());
                    for row in re.rows(tme.unwrap()) {
                        row.unwrap();
                    }
                }
            }
            assert!(events_num > 0);
            timeout(Duration::from_secs(10), binlog_stream.close())
                .await
                .unwrap()
                .unwrap();
        }

        // iterate using COM_BINLOG_DUMP with BINLOG_DUMP_NON_BLOCK flag
        let (conn, filename, pos) = create_binlog_stream_conn(pool).await.unwrap();

        let mut binlog_stream = conn
            .get_binlog_stream(
                BinlogStreamRequest::new(binlog_server_ids.2)
                    .with_filename(&filename)
                    .with_pos(pos)
                    .with_non_blocking(),
            )
            .await
            .unwrap();

        events_num = 0;
        while let Some(event) = binlog_stream.next().await {
            let event = event.unwrap();
            events_num += 1;
            event.header().event_type().unwrap();
            event.read_data().unwrap();
        }
        assert!(events_num > 0);
        timeout(Duration::from_secs(10), binlog_stream.close())
            .await
            .unwrap()
            .unwrap();

        Ok(())
    }

    /// Parses GTID_EXECUTED and returns the max GNO for a given UUID and optional tag.
    ///
    /// Parses a GTID_EXECUTED string and returns the maximum GNO for
    /// the given UUID and tag namespace.
    ///
    /// GTID_EXECUTED format: all namespaces for one UUID appear in a single
    /// comma-separated entry. Tokens after `uuid:` are colon-separated;
    /// a token starting with a digit is an interval, and one starting with
    /// `[a-z_]` is a tag name for subsequent intervals.
    ///
    /// Examples:
    ///   `uuid:1-58`                                — untagged intervals only
    ///   `uuid:1-58:tag:1-5:10`                     — untagged 1-58, tag 1-5 and 10
    ///   `uuid:1-58:t1:1-5:t2:1-3,other_uuid:1-10` — two UUIDs, mixed tagged/untagged
    ///
    /// When `tag` is `None`, returns the max GNO from untagged intervals.
    /// When `tag` is `Some(t)`, returns the max GNO from intervals under that tag.
    fn max_executed_gno(gtid_executed: &str, target_uuid: &str, tag: Option<&str>) -> u64 {
        let mut max = 0u64;
        for entry in gtid_executed.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let parts: Vec<&str> = entry.splitn(2, ':').collect();
            if parts.len() < 2 {
                continue;
            }
            if !parts[0].trim().eq_ignore_ascii_case(target_uuid) {
                continue;
            }
            // Walk the colon-separated tokens to parse interleaved tags and intervals.
            // current_tag tracks which namespace we're in (None = untagged).
            let mut current_tag: Option<&str> = None;
            for token in parts[1].split(':') {
                let first_char = token.chars().next().unwrap_or('0');
                if first_char.is_ascii_lowercase() || first_char == '_' {
                    // This token is a tag name; subsequent intervals belong to it.
                    current_tag = Some(token);
                } else {
                    // This token is an interval (e.g. "1-58" or "10").
                    if current_tag == tag {
                        if let Some(end) = token.split('-').last() {
                            if let Ok(n) = end.parse::<u64>() {
                                max = max.max(n);
                            }
                        }
                    }
                }
            }
        }
        max
    }

    /// Parses GTID_EXECUTED into a list of `Sid` for a given UUID.
    ///
    /// Returns one Sid per namespace (untagged + each tag) with the
    /// correct intervals. This produces a GTID set that exactly matches
    /// the server's state, avoiding "Replica has more GTIDs" errors
    /// from over-claiming with a single large interval.
    fn parse_sids_from_gtid_executed<'a>(
        gtid_executed: &str,
        target_uuid: &str,
    ) -> Vec<Sid<'a>> {
        let uuid_bytes = parse_uuid_bytes(target_uuid);
        let mut sids = Vec::new();
        for entry in gtid_executed.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let parts: Vec<&str> = entry.splitn(2, ':').collect();
            if parts.len() < 2 {
                continue;
            }
            if !parts[0].trim().eq_ignore_ascii_case(target_uuid) {
                continue;
            }
            // Walk tokens: tag names start with [a-z_], intervals start with digit.
            let mut current_tag: Option<String> = None;
            let mut current_intervals: Vec<GnoInterval> = Vec::new();

            let flush = |tag: &Option<String>,
                         intervals: &mut Vec<GnoInterval>,
                         sids: &mut Vec<Sid<'a>>,
                         uuid: [u8; 16]| {
                if !intervals.is_empty() {
                    let mut sid = Sid::new(uuid);
                    if let Some(t) = tag {
                        sid = sid.with_tag(Tag::new(t.clone()).unwrap());
                    }
                    for iv in intervals.drain(..) {
                        sid = sid.with_interval(iv);
                    }
                    sids.push(sid);
                }
            };

            for token in parts[1].split(':') {
                let first_char = token.chars().next().unwrap_or('0');
                if first_char.is_ascii_lowercase() || first_char == '_' {
                    // Flush previous namespace
                    flush(&current_tag, &mut current_intervals, &mut sids, uuid_bytes);
                    current_tag = Some(token.to_owned());
                } else {
                    // Parse interval "start-end" or "start"
                    let mut parts_iter = token.split('-');
                    if let Some(start_str) = parts_iter.next() {
                        if let Ok(start) = start_str.parse::<u64>() {
                            let end = parts_iter
                                .next()
                                .and_then(|s| s.parse::<u64>().ok())
                                .unwrap_or(start);
                            // GnoInterval uses [start, end) convention
                            current_intervals.push(GnoInterval::new(start, end + 1));
                        }
                    }
                }
            }
            // Flush last namespace
            flush(&current_tag, &mut current_intervals, &mut sids, uuid_bytes);
        }
        sids
    }

    fn parse_uuid_bytes(uuid_str: &str) -> [u8; 16] {
        let hex: String = uuid_str.replace('-', "");
        let mut bytes = [0u8; 16];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        bytes
    }

    /// Generates a transaction using a tagged GTID (MySQL 8.4+).
    ///
    /// Executes `SET GTID_NEXT='<uuid>:<tag>:<gno>'` to assign a tagged GTID to the
    /// transaction, then commits a simple INSERT, and resets `GTID_NEXT` to `AUTOMATIC`.
    async fn gen_tagged_gtid_data(
        conn: &mut Conn,
        table: &str,
        uuid: &str,
        tag: &str,
        gno: u64,
    ) -> super::Result<()> {
        conn.query_drop(format!("SET GTID_NEXT='{uuid}:{tag}:{gno}'"))
            .await?;
        conn.query_drop("BEGIN").await?;
        conn.query_drop(format!("INSERT INTO {table} VALUES (1)"))
            .await?;
        conn.query_drop("COMMIT").await?;
        conn.query_drop("SET GTID_NEXT='AUTOMATIC'").await?;
        Ok(())
    }

    #[tokio::test]
    async fn should_read_tagged_gtid_from_binlog() -> super::Result<()> {
        let mut conn = Conn::new(get_opts()).await?;

        // Tagged GTIDs require MySQL >= 8.4
        if conn.server_version() < (8, 4, 0) || conn.inner.is_mariadb {
            eprintln!(
                "SKIPPED: tagged GTIDs require MySQL >= 8.4 (server is {:?})",
                conn.server_version()
            );
            conn.disconnect().await?;
            return Ok(());
        }

        if let Ok(Some(gtid_mode)) = "SELECT @@GLOBAL.GTID_MODE"
            .first::<String, _>(&mut conn)
            .await
        {
            if !gtid_mode.starts_with("ON") {
                eprintln!("SKIPPED: GTID_MODE is not ON (got {gtid_mode})");
                conn.disconnect().await?;
                return Ok(());
            }
        }

        conn.query_drop("CREATE TABLE IF NOT EXISTS tagged_gtid_test (id INT NOT NULL)")
            .await?;

        let server_uuid: String =
            "SELECT @@server_uuid".first(&mut conn).await?.unwrap();

        // Find next available GNO for our tag, using only the "readtest" tag namespace
        let gtid_executed: String = "SELECT @@GLOBAL.GTID_EXECUTED"
            .first(&mut conn)
            .await?
            .unwrap();
        let tag_gno = max_executed_gno(&gtid_executed, &server_uuid, Some("readtest")) + 1;

        // Get current binlog position before our tagged transaction
        let row: crate::Row = "SHOW BINARY LOGS".first(&mut conn).await?.unwrap();
        let filename: Vec<u8> = row.get(0).unwrap();
        let position: u64 = row.get(1).unwrap();

        // Generate a transaction with a tagged GTID
        gen_tagged_gtid_data(&mut conn, "tagged_gtid_test", &server_uuid, "readtest", tag_gno)
            .await?;

        // Now read the binlog stream starting from our saved position
        let mut binlog_stream = conn
            .get_binlog_stream(
                BinlogStreamRequest::new(42)
                    .with_gtid()
                    .with_filename(&filename)
                    .with_pos(position),
            )
            .await?;

        let mut found_tagged_gtid = false;
        while let Ok(Some(event)) =
            timeout(Duration::from_secs(10), binlog_stream.next()).await
        {
            let event = event?;

            if event.header().event_type() == Ok(EventType::GTID_TAGGED_LOG_EVENT) {
                if let Some(EventData::GtidEvent(gtid)) = event.read_data()? {
                    if gtid.is_tagged()
                        && gtid.tag().map(|t| t.as_str()) == Some("readtest")
                        && gtid.gno() == tag_gno
                    {
                        found_tagged_gtid = true;
                        break;
                    }
                }
            }
        }

        assert!(
            found_tagged_gtid,
            "GTID_TAGGED_LOG_EVENT with tag 'readtest' and gno {tag_gno} not found in binlog stream"
        );

        binlog_stream.close().await?;

        // Cleanup
        let mut conn = Conn::new(get_opts()).await?;
        conn.query_drop("DROP TABLE IF EXISTS tagged_gtid_test")
            .await?;
        conn.disconnect().await?;

        Ok(())
    }

    #[tokio::test]
    async fn should_replicate_with_tagged_gtid_set() -> super::Result<()> {
        let mut conn = Conn::new(get_opts()).await?;

        // Tagged GTIDs require MySQL >= 8.4
        if conn.server_version() < (8, 4, 0) || conn.inner.is_mariadb {
            eprintln!(
                "SKIPPED: tagged GTIDs require MySQL >= 8.4 (server is {:?})",
                conn.server_version()
            );
            conn.disconnect().await?;
            return Ok(());
        }

        if let Ok(Some(gtid_mode)) = "SELECT @@GLOBAL.GTID_MODE"
            .first::<String, _>(&mut conn)
            .await
        {
            if !gtid_mode.starts_with("ON") {
                eprintln!("SKIPPED: GTID_MODE is not ON (got {gtid_mode})");
                conn.disconnect().await?;
                return Ok(());
            }
        }

        conn.query_drop("CREATE TABLE IF NOT EXISTS tagged_gtid_test2 (id INT NOT NULL)")
            .await?;

        let server_uuid: String =
            "SELECT @@server_uuid".first(&mut conn).await?.unwrap();
        let uuid_bytes = parse_uuid_bytes(&server_uuid);

        // Get current GTID_EXECUTED and compute next GNOs per namespace
        let gtid_executed: String = "SELECT @@GLOBAL.GTID_EXECUTED"
            .first(&mut conn)
            .await?
            .unwrap();
        let max_repltest = max_executed_gno(&gtid_executed, &server_uuid, Some("repltest"));

        let tag_gno = max_repltest + 1;

        // Generate two tagged GTID transactions
        gen_tagged_gtid_data(
            &mut conn, "tagged_gtid_test2", &server_uuid, "repltest", tag_gno,
        )
        .await?;
        gen_tagged_gtid_data(
            &mut conn, "tagged_gtid_test2", &server_uuid, "repltest", tag_gno + 1,
        )
        .await?;

        // Build the exclude set from the server's actual GTID_EXECUTED.
        // We must use the exact intervals (not a single [1, max]) because
        // the server may have gaps, and over-claiming causes error 1236.
        let gtid_executed_now: String = "SELECT @@GLOBAL.GTID_EXECUTED"
            .first(&mut conn)
            .await?
            .unwrap();
        let mut exclude_sids =
            parse_sids_from_gtid_executed(&gtid_executed_now, &server_uuid);

        // Remove the second tagged transaction (tag_gno + 1) from the
        // repltest Sid so the server sends it to us. Since tag_gno+1
        // was just appended, it will be at the end of the last interval.
        for sid in &mut exclude_sids {
            if sid.tag().map(|t| t.as_str()) == Some("repltest") {
                let mut new_sid = Sid::new(uuid_bytes)
                    .with_tag(Tag::new("repltest").unwrap());
                for iv in sid.intervals() {
                    let end = iv.end(); // exclusive
                    if end == tag_gno + 2 {
                        // This interval ends at tag_gno+1 (inclusive).
                        // Trim to exclude tag_gno+1.
                        if iv.start() < tag_gno + 1 {
                            new_sid =
                                new_sid.with_interval(GnoInterval::new(iv.start(), tag_gno + 1));
                        }
                        // else: interval is exactly [tag_gno+1, tag_gno+2) — skip it
                    } else {
                        new_sid = new_sid.with_interval(*iv);
                    }
                }
                *sid = new_sid;
                break;
            }
        }

        let mut binlog_stream = conn
            .get_binlog_stream(
                BinlogStreamRequest::new(43)
                    .with_gtid()
                    .with_gtid_set(exclude_sids),
            )
            .await?;

        let mut found_second_tagged = false;
        while let Ok(Some(event)) =
            timeout(Duration::from_secs(10), binlog_stream.next()).await
        {
            let event = event?;

            if event.header().event_type() == Ok(EventType::GTID_TAGGED_LOG_EVENT) {
                if let Some(EventData::GtidEvent(gtid)) = event.read_data()? {
                    if gtid.is_tagged()
                        && gtid.tag().map(|t| t.as_str()) == Some("repltest")
                        && gtid.gno() == tag_gno + 1
                    {
                        found_second_tagged = true;
                        break;
                    }
                }
            }
        }

        assert!(
            found_second_tagged,
            "Expected tagged GTID repltest:{} in binlog stream",
            tag_gno + 1,
        );

        binlog_stream.close().await?;

        // Cleanup
        let mut conn = Conn::new(get_opts()).await?;
        conn.query_drop("DROP TABLE IF EXISTS tagged_gtid_test2")
            .await?;
        conn.disconnect().await?;

        Ok(())
    }
}
