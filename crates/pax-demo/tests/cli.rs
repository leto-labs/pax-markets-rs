//! Public command-line contract.

use std::process::Command;

#[test]
fn help_lists_read_only_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_pax-demo"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["markets", "observe", "capture", "replay", "serve"] {
        assert!(help.contains(command));
    }
}

#[test]
fn unknown_command_is_not_reported_as_success() {
    let output = Command::new(env!("CARGO_BIN_EXE_pax-demo"))
        .arg("unrecognized-command")
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn rejected_receive_options_fail_before_setup() {
    for option in ["--receive-spin-ns", "--initialized-read-bytes"] {
        let output = Command::new(env!("CARGO_BIN_EXE_pax-demo"))
            .args(["serve", "--fixture", option, "0"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("unexpected argument")
        );
    }
}
