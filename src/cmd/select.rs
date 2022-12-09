use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::instrument;

/// https://redis.io/commands/select/
/// SELECT index
#[derive(Debug, Default)]
pub struct Select {
    id: u64,
}

impl Select {
    pub fn new(id: u64) -> Select {
        Select { id }
    }

    /// Parse a `Select` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `Select` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Select` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing `Select` and an optional message.
    ///
    /// ```text
    /// Select [message]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Select> {
        match parse.next_string() {
            Ok(msg) => Ok(Select::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Select::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Apply the `Select` command and return the message.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(Bytes::from(msg)),
        };

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Select` command to send
    /// to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("Select".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(Bytes::from(msg));
        }
        frame
    }
}
