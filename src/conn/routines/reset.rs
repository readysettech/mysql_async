use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::{
    constants::{CapabilityFlags, Command},
    io::ParseBuf,
    packets::{CommonOkPacket, OkPacketDeserializer},
};
#[cfg(feature = "tracing")]
use tracing::debug_span;

use crate::Conn;

use super::Routine;

#[cfg(test)]
mod tests;

/// Read exactly one ordinary OK, rejecting result sets and additional results.
async fn read_ok(conn: &mut Conn) -> crate::Result<()> {
    let packet = conn.read_packet().await?;
    if packet.first() != Some(&0)
        || ParseBuf(&packet)
            .parse::<OkPacketDeserializer<CommonOkPacket>>(conn.capabilities())
            .is_err()
        || conn.more_results_exists()
    {
        return Err(crate::DriverError::UnexpectedPacket {
            payload: packet.to_vec(),
        }
        .into());
    }
    Ok(())
}

/// A routine that performs `COM_RESET_CONNECTION`.
#[derive(Debug, Copy, Clone)]
pub struct ResetRoutine;

impl Routine<()> for ResetRoutine {
    fn call<'a>(self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>>
    where
        Self: 'a,
    {
        #[cfg(feature = "tracing")]
        let span = debug_span!("mysql_async::reset", mysql_async.connection.id = conn.id());

        let fut = async move {
            conn.write_command_data(Command::COM_RESET_CONNECTION, &[])
                .await?;
            read_ok(conn).await?;
            conn.clear_reset_state();
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = instrument_result!(fut, span);

        fut.boxed()
    }
}

/// Reset followed by one OK-producing initialization statement.
#[derive(Debug)]
pub struct ResetSetupRoutine<'a>(pub &'a str);

impl Routine<()> for ResetSetupRoutine<'_> {
    fn call<'a>(self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>>
    where
        Self: 'a,
    {
        #[cfg(feature = "tracing")]
        let span = debug_span!(
            "mysql_async::reset_with_setup",
            mysql_async.connection.id = conn.id()
        );

        let fut = async move {
            // reset_inner drains before invalidating; never drain old packets
            // inside this routine after the observer has seen the reset boundary.
            debug_assert!(!conn.has_pending_result());
            if conn.has_capabilities(CapabilityFlags::CLIENT_COMPRESS) {
                // Keep both commands in the same routine and apply the same
                // response contract as the pipeline, not query_drop's draining.
                conn.write_command_data(Command::COM_RESET_CONNECTION, &[])
                    .await?;
                read_ok(conn).await?;
                conn.clear_reset_state();
                conn.write_command_data(Command::COM_QUERY, self.0).await?;
                return read_ok(conn).await;
            }

            let codec = conn.stream_mut()?.write_reset_and_setup(self.0).await?;
            let reset_result = read_ok(conn).await;
            match &reset_result {
                Ok(()) => conn.clear_reset_state(),
                // Setup was already sent. Consume its reply even if reset failed.
                Err(crate::Error::Server(_)) => (),
                // Malformed/transport failures are fatal; do not reuse the stream.
                Err(_) => return reset_result,
            }
            conn.stream_mut()?.resume_setup_response(codec)?;
            // A setup error takes precedence if both commands fail.
            read_ok(conn).await?;
            if reset_result.is_err() {
                conn.invalidate_session_observations();
            }
            reset_result
        };

        #[cfg(feature = "tracing")]
        let fut = instrument_result!(fut, span);

        fut.boxed()
    }
}
