use crate::frame::{Frame, FrameError};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // The first step is to check if enough data has been buffered to parse
            // a single frame. This step is usually much faster than doing a full
            // parse of the frame, and allows us to skip allocating data structures
            // to hold the frame data unless we know the full frame has been
            // received.
            let rframe = Frame::parse(&self.buffer);

            match rframe {
                Ok((frame, _)) => {
                    // Discard the parsed data from the read buffer.
                    //
                    // When `advance` is called on the read buffer, all of the data
                    // up to `len` is discarded. The details of how this works is
                    // left to `BytesMut`. This is often done by moving an internal
                    // cursor, but it may be done by reallocating and copying data.
                    self.buffer.advance(frame.len());

                    // Return the parsed frame to the caller.
                    return Ok(Some(frame));
                }
                Err(FrameError::Incomplete) => {
                    // There is not enough buffered data to read a frame. Attempt to
                    // read more data from the socket.
                    //
                    // On success, the number of bytes is returned. `0` indicates "end
                    // of stream".
                    if 0 == self.stream.read_buf(&mut self.buffer).await? {
                        // The remote closed the connection. For this to be a clean
                        // shutdown, there should be no data in the read buffer. If
                        // there is, this means that the peer closed the socket while
                        // sending a frame.
                        if self.buffer.is_empty() {
                            return Ok(None);
                        } else {
                            return Err("connection reset by peer".into());
                        }
                    }

                    return Ok(None);
                }
                Err(err) => return Err(Box::new(err)),
            }
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.stream.write_all(&frame.as_bytes());

        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await
    }

    /// Write a decimal frame to the stream
    async fn write_decimal(&mut self, val: i64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
