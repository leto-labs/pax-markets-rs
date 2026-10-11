//! In-memory transport for the pinned production WebSocket parser.
use std::io::{self, Read, Write};
use tokio_tungstenite::tungstenite::{
    Message, WebSocket,
    protocol::{
        Role, WebSocketConfig,
        frame::{
            Frame,
            coding::{Data, OpCode},
        },
    },
};

pub struct Wire<'a> {
    pub input: &'a [u8],
    pub chunk: usize,
    pub reads: u64,
    pub writes: usize,
    read_quota: Option<usize>,
}
impl Wire<'_> {
    /// Expose an exact next wire prefix for a streaming retry component.
    pub fn allow_next(&mut self, bytes: usize) -> io::Result<()> {
        if bytes > self.input.len() || self.read_quota.is_some_and(|left| left != 0) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.read_quota = Some(bytes);
        Ok(())
    }
}
impl Read for Wire<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.input.is_empty() || self.read_quota == Some(0) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = out
            .len()
            .min(self.chunk)
            .min(self.input.len())
            .min(self.read_quota.unwrap_or(usize::MAX));
        out[..n].copy_from_slice(&self.input[..n]);
        self.input = &self.input[n..];
        if let Some(left) = &mut self.read_quota {
            *left -= n;
        }
        self.reads += 1;
        Ok(n)
    }
}
impl Write for Wire<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writes += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[derive(Default)]
struct Encode(Vec<u8>);
impl Read for Encode {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}
impl Write for Encode {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
/// Generate unmasked server frames using the pinned library, outside timing.
pub fn encode(raw: &[u8], fragmented: bool) -> Vec<u8> {
    let mut socket = WebSocket::from_raw_socket(Encode::default(), Role::Server, None);
    if fragmented && raw.len() > 1 {
        let split = raw.len() / 2;
        socket
            .write(Message::Frame(Frame::message(
                raw[..split].to_vec(),
                OpCode::Data(Data::Text),
                false,
            )))
            .unwrap();
        socket
            .write(Message::Ping(b"control".as_slice().into()))
            .unwrap();
        socket
            .write(Message::Frame(Frame::message(
                raw[split..].to_vec(),
                OpCode::Data(Data::Continue),
                true,
            )))
            .unwrap();
    } else {
        socket
            .write(Message::Frame(Frame::message(
                raw.to_vec(),
                OpCode::Data(Data::Text),
                true,
            )))
            .unwrap();
    }
    socket.flush().unwrap();
    socket.into_inner().0
}
pub fn parser(input: &[u8], chunk: usize, buffer: usize, limit: usize) -> WebSocket<Wire<'_>> {
    assert!(chunk > 0 && buffer > 0);
    WebSocket::from_raw_socket(
        Wire {
            input,
            chunk,
            reads: 0,
            writes: 0,
            read_quota: None,
        },
        Role::Client,
        Some(
            WebSocketConfig::default()
                .read_buffer_size(buffer)
                .max_message_size(Some(limit))
                .max_frame_size(Some(limit)),
        ),
    )
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)] // Also compiled by a harness=false benchmark target.
    use super::*;
    #[test]
    fn gated_wire_refuses_read_ahead_and_unconsumed_budget_changes() {
        let mut wire = Wire {
            input: b"abcdef",
            chunk: 3,
            reads: 0,
            writes: 0,
            read_quota: None,
        };
        wire.allow_next(2).unwrap();
        assert_eq!(
            wire.allow_next(1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let mut bytes = [0u8; 8];
        assert_eq!(wire.read(&mut bytes).unwrap(), 2);
        assert_eq!(&bytes[..2], b"ab");
        assert_eq!(
            wire.read(&mut bytes).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            wire.allow_next(5).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        wire.allow_next(4).unwrap();
        assert_eq!(wire.read(&mut bytes).unwrap(), 3);
        assert_eq!(&bytes[..3], b"cde");
        assert_eq!(wire.read(&mut bytes).unwrap(), 1);
        assert_eq!(bytes[0], b'f');
        assert_eq!(wire.reads, 3);
        assert!(wire.input.is_empty());
    }
    #[test]
    fn fragmentation_controls_utf8_and_read_ahead_preserve_order() {
        let texts = ["one", "€🙂€🙂", "three"];
        for chunk in [1, 7, 65536] {
            let wire: Vec<_> = texts
                .iter()
                .flat_map(|s| encode(s.as_bytes(), true))
                .collect();
            let mut socket = parser(&wire, chunk, 1024, 65536);
            let mut got = Vec::new();
            let mut pings = 0;
            while got.len() < texts.len() {
                match socket.read().unwrap() {
                    Message::Text(text) => got.push(text.to_string()),
                    Message::Ping(_) => pings += 1,
                    message => panic!("unexpected {message:?}"),
                }
            }
            assert_eq!(got, texts);
            assert_eq!(pings, 3);
            assert!(socket.get_ref().writes > 0);
        }
    }
    #[test]
    fn malformed_utf8_rsv_continuation_and_limits_are_rejected() {
        for wire in [
            vec![0x81, 1, 0xff],
            vec![0xc1, 0],
            vec![0x80, 0],
            vec![0x09, 0],
        ] {
            assert!(parser(&wire, 1, 1024, 1024).read().is_err());
        }
        let oversized = encode(&[b'x'; 4096], false);
        assert!(matches!(
            parser(&oversized, 17, 1024, 1024).read(),
            Err(tokio_tungstenite::tungstenite::Error::Capacity(_))
        ));
        let invalid = encode(&[0xf0, 0x28, 0x8c, 0xbc], true);
        let mut socket = parser(&invalid, 1, 1024, 1024);
        // The parser may reject as soon as an invalid prefix is visible, before
        // the interleaved control frame, or at final assembly.
        for _ in 0..2 {
            match socket.read() {
                Err(tokio_tungstenite::tungstenite::Error::Utf8(_)) => return,
                Ok(Message::Ping(_)) => {}
                other => panic!("invalid text accepted or misclassified: {other:?}"),
            }
        }
        panic!("invalid text was not rejected");
    }
}
