use super::*;
use std::io::Cursor;

#[test]
fn partial_input_empty_reads_and_final_truncation_keep_exact_wire_intervals() {
    let mut input = Input::new(Cursor::new([23, 3, 3, 0, 3, 11, 12, 13, 23, 3, 3, 0, 0]));
    let mut out = [0; 4096];
    assert_eq!(input.read(&mut []).unwrap(), 0);
    assert_eq!(input.offset(), 0);
    assert_eq!(input.read(&mut out[..2]).unwrap(), 2);
    assert_eq!(
        input.finish().unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(input.read(&mut out).unwrap(), 3);
    assert!(input.completed().is_none());
    assert_eq!(input.read(&mut out).unwrap(), 3);
    assert_eq!(
        input.completed(),
        Some(Record {
            ordinal: 1,
            start: 0,
            end: 8
        })
    );
    input.finish().unwrap();
    assert_eq!(input.read(&mut out).unwrap(), 5);
    assert_eq!(
        input.completed(),
        Some(Record {
            ordinal: 2,
            start: 8,
            end: 13
        })
    );
    assert_eq!(input.read(&mut out).unwrap(), 0);
    input.finish().unwrap();
    assert_eq!(input.get_ref().position(), 13);
}

#[test]
fn temporary_input_errors_preserve_state_but_invalid_lengths_are_terminal() {
    struct Fault {
        kind: io::ErrorKind,
    }
    impl Read for Fault {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(self.kind.into())
        }
    }
    struct Oversized;
    impl Read for Oversized {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            Ok(out.len() + 1)
        }
    }
    let mut state = Limiter::default();
    let mut out = [0; 32];
    state.read(&mut &[23, 3][..], &mut out).unwrap();
    for kind in [io::ErrorKind::WouldBlock, io::ErrorKind::Interrupted] {
        assert_eq!(
            state
                .read(&mut Fault { kind }, &mut out)
                .unwrap_err()
                .kind(),
            kind
        );
        assert_eq!(state.offset(), 2);
    }
    state.read(&mut &[3, 0, 0][..], &mut out).unwrap();
    assert_eq!(state.completed().unwrap().end, 5);
    assert_eq!(
        state.read(&mut Oversized, &mut out).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(state.completed().is_none());
    assert_eq!(
        state
            .read(&mut &[23, 3, 3, 0, 0][..], &mut out)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        state.finish().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn offset_overflow_is_terminal_and_write_forwarding_does_not_advance_input() {
    let mut state = Limiter::default();
    state.state.offset = u64::MAX;
    assert_eq!(
        state.read(&mut &[23][..], &mut [0; 5]).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(state.completed().is_none());
    let mut input = Input::new(Vec::new());
    input.write_all(b"one").unwrap();
    assert_eq!(
        input
            .write_vectored(&[io::IoSlice::new(b"two"), io::IoSlice::new(b"three")])
            .unwrap(),
        8
    );
    input.flush().unwrap();
    assert_eq!(input.offset(), 0);
    assert!(input.completed().is_none());
    assert_eq!(input.into_inner(), b"onetwothree");
}
