//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use std::fmt;

use atoi::atoi;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
pub enum Frame {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(FrameArray),
}

impl std::cmp::PartialEq<str> for Frame {
    fn eq(&self, other: &str) -> bool {
        self.as_bytes() == other
    }
}

impl std::cmp::PartialEq<String> for Frame {
    fn eq(&self, other: &String) -> bool {
        self.as_bytes() == other
    }
}

#[derive(Clone, Debug)]
pub struct FrameArray {
    inner: Vec<Frame>,
}

impl FrameArray {
    pub fn with_capacity(n: usize) -> FrameArray {
        FrameArray {
            inner: Vec::with_capacity(n),
        }
    }

    pub fn from_vec(inner: Vec<Frame>) -> FrameArray {
        FrameArray { inner }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Frame> {
        self.inner.iter()
    }

    pub fn as_slice(&self) -> &[Frame] {
        self.inner.as_slice()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn push_bulk(&mut self, bytes: Bytes) {
        self.inner.push(Frame::Bulk(bytes));
    }

    pub fn push_int(&mut self, value: i64) {
        self.inner.push(Frame::Integer(value));
    }
}

impl IntoIterator for FrameArray {
    type Item = Frame;

    type IntoIter = std::vec::IntoIter<Frame>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl Default for FrameArray {
    fn default() -> Self {
        FrameArray {
            inner: Vec::default(),
        }
    }
}

#[derive(Debug)]
pub enum FrameError {
    Incomplete,
    InvalidInteger,
    BadFormat,
}

impl Frame {
    pub fn parse<'a>(src: &'a [u8]) -> Result<(Frame, &'a [u8]), FrameError> {
        match peek_u8_at(src, 0)? {
            b'+' => {
                let (res, leftover) = get_bytes_until_crlf(get_bytes_from(src, 1)?)?;
                Ok((Frame::Simple(res.into()), leftover))
            }
            b'-' => {
                let (res, leftover) = get_bytes_until_crlf(get_bytes_from(src, 1)?)?;
                Ok((Frame::Error(res.into()), leftover))
            }
            b':' => {
                let (res, leftover) = get_bytes_until_crlf(get_bytes_from(src, 1)?)?;
                let res = atoi::<i64>(res).ok_or(FrameError::InvalidInteger)?;
                Ok((Frame::Integer(res), leftover))
            }
            b'$' => {
                // $<decimal>\r\n<string>\r\n
                let (len, leftover) = get_decimal_until_crlf(get_bytes_from(src, 1)?)?;
                if len < 0 {
                    Ok((Frame::Null, leftover))
                } else if peek_u8_at(leftover, len as usize)? == b'\r'
                    && peek_u8_at(leftover, len as usize + 1)? == b'\n'
                {
                    Ok((
                        Frame::Bulk(leftover[..len as usize].into()),
                        get_bytes_from(leftover, len as usize + 2)?,
                    ))
                } else {
                    Err(FrameError::BadFormat)
                }
            }
            b'*' => {
                // TODO: Check for negative count?
                let (mut count, mut leftover) = get_decimal_until_crlf(get_bytes_from(src, 1)?)?;
                let mut result = Vec::with_capacity(count as usize);

                while count != 0 && leftover.len() != 0 {
                    let (frame, new_leftover) = Frame::parse(leftover)?;
                    result.push(frame);
                    leftover = new_leftover;
                    count -= 1;
                }

                if peek_u8_at(leftover, 0)? == b'\r' && peek_u8_at(leftover, 0)? == b'\r' {
                    Ok((Frame::Array(FrameArray::from_vec(result)), leftover))
                } else {
                    Err(FrameError::BadFormat)
                }
            }
            _ => Err(FrameError::BadFormat),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Frame::Simple(b) => b.len(),
            Frame::Error(b) => b.len(),
            // TODO: non-allocating alternative?
            Frame::Integer(b) => format!("{}", b).len(),
            Frame::Bulk(b) => b.len(),
            Frame::Null => 4,
            Frame::Array(b) => b.iter().map(|v| v.len()).sum(),
        }
    }

    pub fn as_name(&self) -> &'static str {
        match self {
            Frame::Simple(_) => "simple",
            Frame::Error(_) => "error",
            Frame::Integer(_) => "integer",
            Frame::Bulk(_) => "bulk",
            Frame::Null => "null",
            Frame::Array(_) => "array",
        }
    }

    pub fn as_bytes(&self) -> Bytes {
        match self {
            Frame::Simple(b) => {
                let mut result = Vec::with_capacity(1 + b.len() + 2);
                result.write_u8(b'+');
                result.write_all(&b);
                result.write_u8(b'\r');
                result.write_u8(b'\n');
                result.into()
            }
            Frame::Error(b) => {
                let mut result = Vec::with_capacity(1 + b.len() + 2);
                result.write_u8(b'-');
                result.write_all(&b);
                result.write_u8(b'\r');
                result.write_u8(b'\n');
                result.into()
            }
            Frame::Integer(b) => {
                let str = format!("{}", b);
                let mut result = Vec::with_capacity(1 + str.len() + 2);
                result.write_u8(b':');
                result.write_all(str.as_bytes());
                result.write_u8(b'\r');
                result.write_u8(b'\n');
                result.into()
            }
            Frame::Bulk(b) => {
                let len = format!("{}", b.len());
                let mut result = Vec::with_capacity(1 + len.len() + 2 + b.len() + 2);
                result.write_u8(b'$');
                result.write_all(len.as_bytes());
                result.write_u8(b'\r');
                result.write_u8(b'\n');
                result.write_all(b);
                result.write_u8(b'\r');
                result.write_u8(b'\n');
                result.into()
            }
            Frame::Null => {
                let mut result = Vec::with_capacity(5);
                result.write_u8(b'$');
                result.write_u8(b'-');
                result.write_u8(b'1');
                result.write_u8(b'\r');
                result.write_u8(b'\n');
                result.into()
            }
            Frame::Array(b) => {
                let len = format!("{}", b.len());
                let mut result = Vec::with_capacity(
                    1 + len.len() + 2 + b.iter().map(|v| v.len()).sum::<usize>() + 2,
                );
                result.write_u8(b'*');
                result.write_all(len.as_bytes());
                for v in b.iter() {
                    result.write_all(&v.as_bytes());
                }
                result.write_u8(b'\r');
                result.write_u8(b'\n');
                result.into()
            }
        }
    }

    pub fn to_error(&self) -> crate::Error {
        format!("did not expect to get {}", self.as_name()).into()
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Frame::Simple(msg) => write!(fmt, "simple:{:?}", msg),
            Frame::Error(msg) => write!(fmt, "error:{:?}", msg),
            Frame::Integer(msg) => write!(fmt, "integer:{:?}", msg),
            Frame::Bulk(msg) => write!(fmt, "bulk{:?}", msg),
            Frame::Null => write!(fmt, "nil"),
            Frame::Array(msg) => write!(fmt, "{:?}", msg),
        }
    }
}

fn peek_u8_at(src: &[u8], at: usize) -> Result<u8, FrameError> {
    if at < src.len() {
        return Ok(src[at]);
    } else {
        return Err(FrameError::Incomplete);
    }
}

fn get_bytes_from<'a>(src: &'a [u8], at: usize) -> Result<&'a [u8], FrameError> {
    if at < src.len() {
        Ok(&src[at..])
    } else {
        Err(FrameError::Incomplete)
    }
}

/// Parses something like :<decimal>\r\n
fn get_decimal_until_crlf<'a>(src: &'a [u8]) -> Result<(i64, &'a [u8]), FrameError> {
    let (str, leftover) = get_bytes_until_crlf(src)?;
    if str.len() > 3 {
        let res = atoi::<i64>(&str[1..str.len() - 2]).ok_or(FrameError::InvalidInteger)?;
        Ok((res, leftover))
    } else {
        Err(FrameError::Incomplete)
    }
}

/// Returns bytes until the first CRLF
fn get_bytes_until_crlf<'a>(src: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), FrameError> {
    if src.len() >= 2 {
        for i in 0..src.len() - 1 {
            if src[i] == b'\r' && src[i + 1] == b'\n' {
                return Ok((&src[..i], &src[i..]));
            }
        }
    }
    Err(FrameError::Incomplete)
}

impl std::error::Error for FrameError {}

impl fmt::Display for FrameError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FrameError::Incomplete => "frame requires more bytes".fmt(fmt),
            FrameError::InvalidInteger => "frame contains invalid integer".fmt(fmt),
            FrameError::BadFormat => "frame contains bad format".fmt(fmt),
        }
    }
}
