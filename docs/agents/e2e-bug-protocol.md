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
| 2026-07-26 | `prefix+shift+n` (and every other binding) dead in the pure client | The `Mode::Navigate` arm of the pure client's `handle_key` handled only `q`: it swallowed the prefix key, and - unlike the legacy navigate mode, which runs prefix bindings leaderlessly - it ran none of the bare keys its own NAVIGATE bar advertises. Nothing reached `dispatch_prefix_intent` while the composed view had no workspace, the very screen that tells the user to press prefix+shift+n | `src/client_state/run.rs::the_prefix_key_opens_prefix_mode_with_no_workspace_in_view`, `::navigate_mode_runs_prefix_intents_without_the_prefix` (also `intent.rs::new_workspace_intent_targets_the_in_view_remote_with_an_empty_catalog`) | ba98edb |
| 2026-07-26 | The local connection does not behave like a remote one | `establish_local` connected synchronously on the run-loop thread, so local's `Connecting` state was overwritten between two renders (no spinner) and a wedged local server blocked every remote for the hello timeout; local also raised a status line no remote gets and sent unlabeled notifications | `src/client_state/run.rs::local_connects_off_thread_so_its_chip_can_show_connecting`, `::a_fleet_reports_local_transport_loss_in_the_chip_not_a_toast`, `::a_single_remote_client_still_reports_transport_loss_in_the_status_line`, `::a_local_session_opens_its_link_and_resyncs_like_a_remote`, `fleet_view.rs::notification_labels_name_the_remote_only_in_a_real_fleet` | 3303ea0 |
| 2026-07-25 | Directory and git branch never update in spaces pane | `WorkspaceInfo` carried no git fields; git refresh and OSC 7 cwd reports emitted no catalog events; headless git refresh loop gated on legacy app clients only, so fleet remotes never computed branch/ahead-behind | `src/app/mod.rs::git_status_change_emits_workspace_updated_catalog_event`, `::terminal_cwd_report_emits_pane_and_workspace_catalog_events` (also `compose.rs::compose_carries_workspace_git_facts_into_the_projection`, `headless.rs::catalog_session_enables_headless_git_refresh`) | b56711f |
| 2026-07-26 | Clicking a pane scrollbar above the thumb panics a debug build; scrollbar click and drag never paged history in | `scrollbar_thumb_grab_offset` used `bool::then_some(row - thumb.top)`, whose argument is evaluated eagerly, so every click above the thumb underflowed `u16`; separately the scrollbar gestures scroll through the pane-content seam and no client backfill was requested for them | `src/ui.rs::a_click_above_the_thumb_grabs_nothing_instead_of_underflowing`, `run.rs::clicking_the_pane_scrollbar_track_pages_in_more_history`, `::dragging_the_pane_scrollbar_thumb_pages_in_more_history` | 9478be09 (swept into an unrelated commit, not isolated as its own `fix:`) |
| 2026-07-26 | Backfilled scrollback silently moved selections and search hits onto different text | `apply_history_response` and `apply_tail` return the rows a prepend added and document that callers must re-base absolute-row state by it; the pure client discarded both return values, so absolute rows kept their old numbers after every existing row had shifted down | `src/app/state.rs::prepended_history_renumbers_only_the_receiving_pane`, `selection.rs::prepended_history_shifts_a_selection_onto_the_same_text`, `run.rs::a_landed_history_page_renumbers_the_selection_it_pushed_down`, `::a_page_that_bakes_when_the_alternate_screen_ends_renumbers_too`, `::a_landed_history_page_leaves_other_panes_selections_alone` | fee09af2 |
| 2026-07-26 | `herdr client` panicked with a raw OS error whenever stdout was not a TTY | The pure client became the default and calls `ratatui::init()`, which panics on a non-terminal stdout, before anything checks or reports a problem; the legacy client's preflight connect-and-report never ran | `tests/client_mode.rs::a_client_without_a_terminal_says_so_instead_of_panicking` (and `::legacy_client_server_unreachable_shows_clear_error` for the path it replaced) | e85a835e |
| 2026-07-27 | `herdr bridge` reported "no server running" against a live server, parking the remote as `stopped` | `ensure_server_running` gated on `is_server_listening()`, which probes the *client* socket, while the bridge pumps against the *API* socket. The server binds the API socket ~250ms before the client socket, so a bridge arriving in that window saw a live API socket but no client socket and exited with `BRIDGE_NO_SERVER_EXIT` | `src/server/autodetect.rs::the_api_socket_probe_sees_a_server_the_client_socket_probe_misses` (end-to-end: `tests/cli/remote.rs::bridge_subcommand_pumps_the_framed_protocol_to_the_api_socket`) | this commit |

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
- [ ] **Local renders as a chip like any remote**: hue dot, gutter and
      `· local` token on its spaces, toggle/solo work on it, and the last
      remote in view is refused.
- [ ] **Local kill/heal**: stop and restart the local server; its chip goes
      hollow, spins, then fills - with no status-line toast while the chip
      strip is on screen. `kill -STOP` (socket open, nothing answered) must
      go hollow too, on the heartbeat clock.
- [ ] **Hidden chip strip**: with the sidebar collapsed, or on a mobile-width
      terminal, local transport loss comes back as a toast - the dot is not
      on screen to carry it.
- [ ] `prefix+shift+n` creates a space, including from the empty
      "No workspaces yet" screen and on a solo'd remote with no spaces; on
      that screen the bare keys the NAVIGATE bar names act too (`c`, `v`,
      `-`, `x`, `z`).
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
