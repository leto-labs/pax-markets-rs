//! Bounded, initialized recvmsg destinations and nested operation attribution.
use super::*;
impl SocketTimestamps {
    pub fn poll_read(
        &mut self,
        mut stream: Pin<&mut TcpStream>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.sampler.rate().enabled() {
            self.software = Stamp::unavailable("timestamp collection disabled");
            self.hardware = Stamp::unavailable("timestamp collection disabled");
            if !self.detail_sampler.rate().enabled() {
                return stream.as_mut().poll_read(cx, buf);
            }
        }
        if !self.enabled && !self.detail_sampler.rate().enabled() {
            return stream.as_mut().poll_read(cx, buf);
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let selected_detail = cfg!(feature = "receive-stages") && self.detail_sampler.observe();
        loop {
            let start = selected_detail.then(Instant::now);
            let ready = stream.poll_read_ready(cx);
            record(&mut self.work.readiness, start);
            let result = match ready {
                Poll::Ready(Ok(_)) => stream.try_io(Interest::READABLE, || {
                    self.read_once(&stream, buf, selected_detail)
                }),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {
                    self.work.pending = self.work.pending.saturating_add(1);
                    return Poll::Pending;
                }
            };
            match result {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => return Poll::Ready(Err(e)),
                Ok(ReadResult {
                    bytes,
                    truncated,
                    stamps,
                    selected,
                }) => {
                    let start = selected_detail.then(Instant::now);
                    buf.advance(bytes);
                    record(&mut self.work.delivery, start);
                    self.work.bytes = self.work.bytes.saturating_add(bytes as u64);
                    self.work.reads = self.work.reads.saturating_add(u64::from(bytes != 0));
                    if bytes != 0 {
                        if !selected {
                            self.software = Stamp::unavailable("timestamp read not selected");
                            self.hardware = Stamp::unavailable("timestamp read not selected");
                            return Poll::Ready(Ok(()));
                        }
                        let start = selected_detail.then(Instant::now);
                        let now = Instant::now();
                        let missing = if truncated {
                            "ancillary metadata truncated"
                        } else {
                            "no ancillary timestamp in this TCP read"
                        };
                        self.software = Stamp::unavailable(if self.want_software {
                            missing
                        } else {
                            "not requested"
                        });
                        self.hardware = Stamp::unavailable(if self.phc.is_some() {
                            missing
                        } else {
                            self.hardware_reason
                        });
                        if let Some(ts) = stamps {
                            if self.want_software {
                                self.system_identity.attempted();
                                self.software = align_stamp_with_identity(
                                    ts.system,
                                    "kernel-software/realtime",
                                    &mut self.system,
                                    Calibration::system().map(Some),
                                    now,
                                    Some(&self.system_identity),
                                );
                            }
                            if let Some(clock) = &mut self.phc {
                                self.hardware = clock.stamp(ts.hw_raw, now);
                            }
                        }
                        record(&mut self.work.metadata, start);
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
    fn read_once(
        &mut self,
        stream: &TcpStream,
        buf: &mut ReadBuf<'_>,
        selected_detail: bool,
    ) -> io::Result<ReadResult> {
        let start = selected_detail.then(Instant::now);
        // Safe caller-owned destination; only uninitialized tail bytes are zeroed.
        let initialized = buf.capacity() - buf.initialized().len();
        let destination = buf.initialize_unfilled();
        let mut iov = [IoSliceMut::new(destination)];
        record(&mut self.work.preparation, start);
        self.work.initialized_bytes = self
            .work
            .initialized_bytes
            .saturating_add(initialized as u64);
        let start = selected_detail.then(Instant::now);
        let result = recvmsg::<()>(
            stream.as_raw_fd(),
            &mut iov,
            Some(&mut self.ancillary),
            MsgFlags::MSG_DONTWAIT,
        )
        .map_err(io::Error::from);
        record(&mut self.work.syscall, start);
        if result
            .as_ref()
            .is_err_and(|e| e.kind() == io::ErrorKind::WouldBlock)
        {
            self.work.would_block = self.work.would_block.saturating_add(1);
        }
        let message = result?;
        let start = selected_detail.then(Instant::now);
        let bytes = message.bytes;
        let selected = bytes != 0 && self.sampler.observe();
        let truncated = message.flags.contains(MsgFlags::MSG_CTRUNC);
        let mut stamps = None;
        if selected
            && !truncated
            && let Ok(messages) = message.cmsgs()
        {
            for control in messages {
                if let ControlMessageOwned::ScmTimestampsns(ts) = control {
                    stamps = Some(ts);
                }
            }
        }
        record(&mut self.work.metadata, start);
        Ok(ReadResult {
            bytes,
            truncated,
            stamps,
            selected,
        })
    }
}
fn record(work: &mut ProbeWork, start: Option<Instant>) {
    let elapsed = start.map(|at| ns(at.elapsed()));
    work.record(elapsed, elapsed);
}

struct ReadResult {
    bytes: usize,
    truncated: bool,
    stamps: Option<nix::sys::socket::Timestamps>,
    selected: bool,
}
