//! Portable socket I/O with explicitly unavailable Linux timestamp sources.
use super::Stamp;

pub(crate) struct SocketTimestamps {
    pub software: Stamp,
    pub hardware: Stamp,
}
impl SocketTimestamps {
    pub fn new(
        _: &tokio::net::TcpStream,
        config: &crate::websocket::receive::ReceiveConfig,
    ) -> Self {
        Self {
            software: Stamp::unavailable(if config.software_timestamps {
                "Linux timestamps unsupported on this OS"
            } else {
                "not requested"
            }),
            hardware: Stamp::unavailable(if config.hardware.is_some() {
                "Linux timestamps unsupported on this OS"
            } else {
                "not requested"
            }),
        }
    }
    pub fn configure(&mut self, _: crate::websocket::receive::policy::ReceivePolicy) {}
    pub fn counts(
        &self,
    ) -> (
        Option<crate::measurement::SelectionCounts>,
        Option<crate::measurement::SelectionCounts>,
    ) {
        (None, None)
    }
    pub fn take_work(&mut self) -> crate::websocket::receive::ReadDetail {
        crate::websocket::receive::ReadDetail::default()
    }
    pub fn poll_read(
        &mut self,
        stream: std::pin::Pin<&mut tokio::net::TcpStream>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(stream, cx, buf)
    }
}
