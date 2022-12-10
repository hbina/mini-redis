//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use std::fmt;

use atoi::atoi;

#[derive(Clone, Debug)]
pub enum Frame {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<Frame>),
}

#[derive(Clone, Debug)]
pub enum IntermediateFrame<'a> {
    Simple(&'a [u8]),
    Error(&'a [u8]),
    Integer(&'a [u8]),
    Bulk(&'a [u8]),
    Null,
    Array(&'a [u8]),
}

#[derive(Debug)]
pub enum FrameError {
    Incomplete,
    InvalidInteger,
    BadFormat,
}

impl<'a> IntermediateFrame<'a> {
    pub fn parse_intermediate(src: &[u8]) -> Result<(IntermediateFrame<'a>, &'a [u8]), FrameError> {
        match peek_u8_at(src, 0)? {
            b'+' => {
                let (res, leftover) = get_bytes_until_crlf(get_bytes_from(src, 1)?)?;
                Ok((Frame::Simple(res), leftover))
            }
            b'-' => {
                let (res, leftover) = get_bytes_until_crlf(get_bytes_from(src, 1)?)?;
                Ok((Frame::Error(res), leftover))
            }
            b':' => {
                let (res, leftover) = get_bytes_until_crlf(get_bytes_from(src, 1)?)?;
                if atoi::<i64>(res).is_none() {
                    Err(FrameError::InvalidInteger)
                } else {
                    Ok((Frame::Integer(res), leftover))
                }
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
                        Frame::Bulk(&leftover[0..len as usize]),
                        get_bytes_from(src, 2)?,
                    ))
                } else {
                    Err(FrameError::BadFormat)
                }
            }
            b'*' => {
                let mut end_idx = 0;
                let (mut clrf_count, mut total_leftover) =
                    get_decimal_until_crlf(get_bytes_from(src, 1)?)?;

                while clrf_count != 0 && total_leftover.len() != 0 {
                    let (frame, new_leftover) = Frame::parse(total_leftover)?;
                    total_leftover = new_leftover;
                    end_idx += frame.len();
                }

                if peek_u8_at(total_leftover, 0)? == b'\r'
                    && peek_u8_at(total_leftover, 0)? == b'\r'
                {
                    Ok((Frame::Array(&src[1..end_idx]), total_leftover))
                } else {
                    Err(FrameError::BadFormat)
                }
            }
            _ => unimplemented!(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Frame::Simple(b) => b.len(),
            Frame::Error(b) => b.len(),
            Frame::Integer(b) => b.len(),
            Frame::Bulk(b) => b.len(),
            Frame::Null => 4,
            Frame::Array(b) => b.len(),
        }
    }

    pub fn to_frame(self) -> Frame {
        match self {
            IntermediateFrame::Simple(b) => Frame::Simple(b.to_owned()),
            IntermediateFrame::Error(b) => Frame::Error(b.to_owned()),
            IntermediateFrame::Integer(res) => Frame::Integer(
                atoi::<i64>(res).expect("We already verified that it is a valid integer"),
            ),
            IntermediateFrame::Bulk(b) => Frame::Bulk(b.to_owned()),
            IntermediateFrame::Null => Frame::Null,
            IntermediateFrame::Array(b) => todo!(),
        }
    }
}

impl<'a> fmt::Display for IntermediateFrame<'a> {
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

fn get_bytes_from(src: &[u8], at: usize) -> Result<&[u8], FrameError> {
    if at < src.len() {
        Ok(&src[at..])
    } else {
        Err(FrameError::Incomplete)
    }
}

/// Parses something like :<decimal>\r\n
fn get_decimal_until_crlf(src: &[u8]) -> Result<(i64, &[u8]), FrameError> {
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
