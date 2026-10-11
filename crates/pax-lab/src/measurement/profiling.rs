//! Explicit perf-stat region gates. Keep these out of normal latency measurements.
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, Read, Write},
};
/// A caller-provided control/ack pair for Linux `perf stat --control fd:...`.
/// Opened descriptors are duplicated through procfs; no descriptor ownership is
/// stolen. The launcher must bound the process group if perf stops responding.
pub struct Gate {
    control: File,
    ack: BufReader<File>,
}
impl Gate {
    /// Open inherited descriptors; fails if procfs or either descriptor is absent.
    pub fn from_fds(control: u32, ack: u32) -> io::Result<Self> {
        Ok(Self {
            control: OpenOptions::new()
                .write(true)
                .open(format!("/proc/self/fd/{control}"))?,
            ack: BufReader::new(File::open(format!("/proc/self/fd/{ack}"))?),
        })
    }
    /// Enable/disable events and wait for perf's acknowledgment. The enabled region
    /// includes acknowledgment return and disable-command tail; measure an empty gate.
    pub fn set(&mut self, enabled: bool) -> io::Result<()> {
        self.control
            .write_all(if enabled { b"enable\n" } else { b"disable\n" })?;
        self.control.flush()?;
        read_ack(&mut self.ack)
    }
}
fn read_ack(reader: &mut impl Read) -> io::Result<()> {
    // Linux v6.8 evlist__ctlfd_ack writes sizeof("ack\n"), including the NUL.
    // Other versions may write only the line. Consume at most one residual NUL
    // from the preceding reply; never scan or allocate an unbounded response.
    // https://github.com/torvalds/linux/blob/v6.8/tools/perf/util/evlist.c#L1942-L1954
    let mut reply = [0u8; 4];
    reader.read_exact(&mut reply[..1])?;
    if reply[0] == 0 {
        reader.read_exact(&mut reply)?;
    } else {
        reader.read_exact(&mut reply[1..])?;
    }
    if &reply != b"ack\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "perf control acknowledgment missing",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_perf_replies_with_and_without_c_terminator() {
        for data in [b"ack\nack\n".as_slice(), b"ack\n\0ack\n\0"] {
            let mut reader = data;
            read_ack(&mut reader).unwrap();
            read_ack(&mut reader).unwrap();
            assert!(read_ack(&mut reader).is_err());
        }
        assert!(read_ack(&mut b"nope\n".as_slice()).is_err());
        assert!(read_ack(&mut b"\0\0ack\n".as_slice()).is_err());
    }
}
