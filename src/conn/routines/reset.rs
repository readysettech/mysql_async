use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;
#[cfg(feature = "tracing")]
use tracing::debug_span;

use crate::Conn;

use super::Routine;

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
            conn.read_packet().await?;
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
        async move {
            conn.clean_dirty().await?;
            let codec = conn.stream_mut()?.write_reset_and_setup(self.0).await?;
            let reset_result = conn.read_packet().await;
            if let Err(error) = &reset_result {
                if !matches!(error, crate::Error::Server(_)) {
                    return reset_result.map(|_| ());
                }
            }
            let reset_result = reset_result.and_then(|packet| {
                if packet.first() != Some(&0) || conn.more_results_exists() {
                    Err(crate::DriverError::UnexpectedPacket {
                        payload: packet.to_vec(),
                    }
                    .into())
                } else {
                    Ok(packet)
                }
            });
            if reset_result.is_ok() {
                conn.clear_reset_state();
            }
            conn.stream_mut()?.resume_setup_response(codec);
            let setup_packet = conn.read_packet().await?;
            if setup_packet.first() != Some(&0) || conn.more_results_exists() {
                return Err(crate::DriverError::UnexpectedPacket {
                    payload: setup_packet.to_vec(),
                }
                .into());
            }
            if reset_result.is_err() {
                conn.invalidate_session_observations();
            }
            reset_result.map(|_| ())
        }
        .boxed()
    }
}
