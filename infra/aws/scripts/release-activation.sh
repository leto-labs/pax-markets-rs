#!/usr/bin/env bash
# Included in the generated guest script while its operator lock is held.
# Caller supplies release, old, optional stage and verify_previous_release().
# shellcheck disable=SC2154
pax_activation_exit() {
    local failure=$?
    trap - EXIT
    # A second cancellation must not interrupt bounded restoration halfway through.
    # The outer SSM deadline still bounds an unresponsive guest/child.
    trap '' INT TERM HUP
    if (( failure != 0 && rollback_armed == 1 )); then
        if [[ -n "$old" && "$old" != "$release" ]]; then
            if ln -sfn "$old" /opt/pax/current.next &&
                mv -Tf /opt/pax/current.next /opt/pax/current &&
                ln -sfn /opt/pax/current/pax-demo.service /etc/systemd/system/pax-demo.service &&
                systemctl daemon-reload &&
                systemctl enable --force pax-demo.service >/dev/null &&
                systemctl restart pax-demo.service &&
                verify_previous_release; then
                echo 'release failed; previous release restored and synchronized' >&2
            else
                echo 'release failed; previous release recovery could not be verified' >&2
            fi
        elif [[ "$old" == "$release" ]]; then
            echo 'activation failed for existing release; service retained without health proof' >&2
        else
            systemctl stop pax-demo.service || true
            echo 'release failed; no previous healthy release available' >&2
        fi
    fi
    if [[ -n "${stage:-}" ]]; then rm -rf -- "$stage"; fi
    exit "$failure"
}

arm_release_rollback() {
    rollback_armed=1
    trap pax_activation_exit EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    trap 'exit 129' HUP
}
