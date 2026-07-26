# E2E bug protocol (fleet / pure client)

Protocol for bugs found in live or e2e testing of the fleet-of-remotes work
(`HERDR_PURE_CLIENT=1`, PR #27 stack).

## The rule

Every user-found bug gets a **red-first regression test at the narrowest
seam** before the fix:

1. Diagnose the root cause down to the exact seam (intent dispatch, the
   PaneContent seam, catalog/compose, event emission, headless gating, ...).
2. Write the regression test at that seam and confirm it **fails** for the
   diagnosed reason (red), not for a fixture or compile reason.
3. Apply the fix, confirm the test is green, run the full gate.
4. Add a row to the bug log below, then commit the bug as its own
   `fix:` commit referencing the ticket.

## Bug log

| Date | Bug | Root cause | Regression test | Fixed in |
| --- | --- | --- | --- | --- |
| 2026-07-25 | Sidebar "menu" button not clickable in pure client | Click opened `Mode::GlobalMenu`, then `dispatch_mouse_intent`'s unsupported-modal guard reverted the mode on the same event; no key arms for the menu modes | `src/client_state/intent.rs::launcher_click_opens_a_global_menu_that_survives_dispatch` (also `pure_client_global_menu_offers_keybinds_and_detach_only`, `run.rs::global_menu_and_keybind_help_keys_are_handled_client_side`) | b1f9be1 |
| 2026-07-25 | Cursor not shown in session panes in pure client | Replica `PaneContent::render` ignored `show_cursor` and never set the frame cursor; replica `cursor_state` also returned pane-local instead of frame-absolute coordinates | `src/terminal/content.rs::replica_render_sets_the_frame_cursor_when_shown` (also `replica_cursor_state_matches_runtime_coordinates`) | bd6f8d1 |
| 2026-07-25 | Directory and git branch never update in spaces pane | `WorkspaceInfo` carried no git fields; git refresh and OSC 7 cwd reports emitted no catalog events; headless git refresh loop gated on legacy app clients only, so fleet remotes never computed branch/ahead-behind | `src/app/mod.rs::git_status_change_emits_workspace_updated_catalog_event`, `::terminal_cwd_report_emits_pane_and_workspace_catalog_events` (also `compose.rs::compose_carries_workspace_git_facts_into_the_projection`, `headless.rs::catalog_session_enables_headless_git_refresh`) | b56711f |

## Running the e2e environment

- Docker fleet: containers `fleet-alpha`, `fleet-bravo`, `fleet-charlie`
  running sshd + a herdr server each, with matching ssh aliases in
  `~/.ssh/config`.
- Remotes config: `~/.config/herdr-dev/remotes.toml` lists the three
  remotes (name, `user@host` target, session).
- Always scrub inherited herdr env when launching from inside a herdr
  session, and isolate the session:

  ```bash
  env -u HERDR_SOCKET_PATH -u HERDR_CLIENT_SOCKET_PATH -u HERDR_STARTUP_CWD \
      -u HERDR_SESSION -u HERDR_ENV \
      HERDR_PURE_CLIENT=1 cargo run -- --session e2e-test
  ```

- `--remote <name>` runs an ephemeral fleet-of-one against a single remote.

## Manual e2e checklist

Walk this after fleet-affecting changes:

- [ ] Chips: toggle a remote, solo a remote, collapse to today's view.
- [ ] Pane focus + typing reaches the focused remote pane.
- [ ] **Cursor visible** in the focused pane (block/beam at the shell prompt).
- [ ] Local scrollback scrolls; jump-to-top backfills history.
- [ ] Copy mode + selection; OSC52 copy lands in the host clipboard.
- [ ] Sidebar menu button opens; every visible menu row does something
      (keybinds, detach); no silently dead buttons.
- [ ] Directory and branch tokens live-update after `cd` and
      `git checkout` inside a pane (spaces pane label, `{branch}`,
      `{git_status}`).
- [ ] Notifications surface; window title updates.
- [ ] Remote kill/heal: `docker stop`/`docker start` a fleet container;
      chip shows down, reconnect heals it.
- [ ] Version skew: the remedy line names the machine and the fix.
- [ ] Attach + takeover from a second client behaves.
- [ ] `--remote` fleet-of-one offers to save the remote on exit.
