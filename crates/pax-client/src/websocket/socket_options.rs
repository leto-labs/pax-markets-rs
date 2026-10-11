//! Optional cold TCP readbacks; values describe reservation, not effective windows.
use serde::{Deserialize, Serialize};

/// Options read from the connected Linux socket after WebSocket establishment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TcpOptions {
    /// Whether Nagle's algorithm is disabled.
    pub tcp_nodelay: bool,
    /// Current TCP maximum segment size, in bytes.
    pub tcp_maxseg: u32,
    /// Linux-returned receive reservation, including kernel bookkeeping.
    pub receive_buffer: usize,
    /// Linux-returned send reservation, including kernel bookkeeping.
    pub send_buffer: usize,
}

/// A readback failure does not change connection health or socket settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Unavailable {
    /// This connector does not expose a supported TCP socket.
    UnsupportedTransport,
    /// A Linux option query failed with this errno.
    Syscall {
        /// Operating-system error number from the failed query.
        errno: i32,
    },
}

/// Latest connection-only observation, qualified by the enclosing health generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Readback {
    /// All four option queries succeeded.
    Available {
        /// Actual option values; not negotiated effective TCP windows.
        options: TcpOptions,
    },
    /// No usable values were retained.
    Unavailable {
        /// Typed failure; partial results are withheld.
        error: Unavailable,
    },
}

#[cfg(all(target_os = "linux", feature = "socket-readbacks"))]
pub(crate) fn read(socket: &tokio::net::TcpStream) -> Readback {
    use nix::sys::socket::{getsockopt, sockopt};
    let options = (|| -> nix::Result<TcpOptions> {
        Ok(TcpOptions {
            tcp_nodelay: getsockopt(socket, sockopt::TcpNoDelay)?,
            tcp_maxseg: getsockopt(socket, sockopt::TcpMaxSeg)?,
            receive_buffer: getsockopt(socket, sockopt::RcvBuf)?,
            send_buffer: getsockopt(socket, sockopt::SndBuf)?,
        })
    })();
    match options {
        Ok(options) => Readback::Available { options },
        Err(error) => Readback::Unavailable {
            error: Unavailable::Syscall {
                errno: error as i32,
            },
        },
    }
}

#[cfg(all(test, target_os = "linux", feature = "socket-readbacks"))]
mod tests;
