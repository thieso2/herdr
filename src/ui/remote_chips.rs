//! Fleet chip strip atop the sidebar.
//!
//! One chip per configured remote, wrapping across as many rows as needed,
//! in the tab bar's visual language: in-view chips get the surface
//! background, filtered-out chips dim. Connection state lives in the chip
//! dot only — hue for connected in-view remotes, a spinner while
//! connecting, a hollow dot when offline, and a greyed-out chip when the
//! protocol windows are incompatible. Geometry is computed here (pure over
//! plain data) and stored in `ViewState`; render only draws.

use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use super::text::{display_width_u16, truncate_end};
use crate::app::state::{RemoteChipConnection, RemoteChipState, SidebarSection};
use crate::app::AppState;

/// Header row plus one trailing gap row around the wrapped chip rows.
const STRIP_CHROME_ROWS: u16 = 2;
/// Minimum sidebar rows the sections below the strip keep for themselves.
const MIN_SECTION_ROWS: u16 = 6;

/// Computed strip geometry: the reserved rows and the per-chip hit rects.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RemoteChipStripLayout {
    /// Full-width rows reserved atop the sidebar; `Rect::default()` when no
    /// strip is shown.
    pub(crate) strip_rect: Rect,
    /// Per-chip hit rects, parallel to `AppState::remote_chips`. Chips that
    /// did not fit get zero-width rects, mirroring the tab bar's overflow
    /// treatment.
    pub(crate) chip_hit_areas: Vec<Rect>,
}

/// The width of one chip: `" ● name "`.
fn chip_width(chip: &RemoteChipState) -> u16 {
    display_width_u16(&chip.name).saturating_add(4)
}

/// Lays the strip out over the top of `sidebar_area` and returns the
/// remaining sidebar rows. No strip (empty chips, collapsed sidebar, or a
/// sidebar too small to spare rows) returns the area untouched.
pub(crate) fn split_sidebar_for_chip_strip(
    chips: &[RemoteChipState],
    sidebar_area: Rect,
    sidebar_collapsed: bool,
    fleet_client: bool,
) -> (RemoteChipStripLayout, Rect) {
    // The fleet client owns the strip whether or not it has a chip to put in
    // it: an empty `remotes.toml` is the state a fresh install starts in, and
    // the header is where adding the first remote lives. Every other path
    // composes no chips at all, so this keeps their geometry untouched.
    if (chips.is_empty() && !fleet_client) || sidebar_collapsed || sidebar_area.width <= 2 {
        return (RemoteChipStripLayout::default(), sidebar_area);
    }
    // The last column belongs to the `│` separator, like every sidebar
    // section; chips keep a one-column left margin like the section headers.
    let content_w = sidebar_area.width.saturating_sub(1);
    let max_rows = sidebar_area
        .height
        .saturating_sub(MIN_SECTION_ROWS)
        .saturating_sub(STRIP_CHROME_ROWS);
    if max_rows == 0 {
        return (RemoteChipStripLayout::default(), sidebar_area);
    }

    let mut hit_areas = Vec::with_capacity(chips.len());
    // Rows that actually hold a chip: a wrap that overflows `max_rows`
    // bumps `row` before the fit check rejects the chip, and that empty
    // row must not be reserved in the strip.
    let mut placed_rows: u16 = 0;
    let mut row: u16 = 0;
    let mut x: u16 = 1;
    for chip in chips {
        let width = chip_width(chip).min(content_w.saturating_sub(1));
        if x + width > content_w && x > 1 {
            row += 1;
            x = 1;
        }
        if row >= max_rows || x + width > content_w {
            hit_areas.push(Rect::default());
            continue;
        }
        hit_areas.push(Rect::new(
            sidebar_area.x + x,
            sidebar_area.y + 1 + row,
            width,
            1,
        ));
        placed_rows = row + 1;
        x = x + width + 1;
    }

    let chip_rows = placed_rows.max(1);
    let strip_h = chip_rows + STRIP_CHROME_ROWS;
    let strip_rect = Rect::new(sidebar_area.x, sidebar_area.y, sidebar_area.width, strip_h);
    let rest = Rect::new(
        sidebar_area.x,
        sidebar_area.y + strip_h,
        sidebar_area.width,
        sidebar_area.height.saturating_sub(strip_h),
    );
    (
        RemoteChipStripLayout {
            strip_rect,
            chip_hit_areas: hit_areas,
        },
        rest,
    )
}

/// Draws the strip from the geometry computed into `ViewState`.
pub(crate) fn render_remote_chip_strip(app: &AppState, frame: &mut Frame) {
    let area = app.view.remote_chip_strip_rect;
    // The rect is the whole gate: an empty fleet still draws its header, which
    // is the only place a first remote can be added from.
    if area.width == 0 || area.height == 0 {
        return;
    }
    let p = &app.palette;

    // The strip rows carry the same `│` separator column as the sections
    // below so the sidebar edge stays continuous.
    let sep_style = if matches!(app.mode, crate::app::Mode::Navigate) {
        Style::default().fg(p.accent)
    } else {
        Style::default().fg(p.surface_dim)
    };
    let sep_x = area.x + area.width.saturating_sub(1);
    let buf = frame.buffer_mut();
    for y in area.y..area.y + area.height {
        buf[(sep_x, y)].set_symbol("│");
        buf[(sep_x, y)].set_style(sep_style);
    }

    // The `add` label is gone: adding a remote is a named action in this
    // header's menu, so there is exactly one way to do it and the header's
    // right-hand slot means the same thing on all three sections.
    crate::ui::render_sidebar_section_header(
        frame,
        app.view.sidebar_remotes_header_rect,
        " remotes",
        crate::ui::sidebar_section_marker_visible(app, SidebarSection::Remotes)
            .then(|| Style::default().fg(p.overlay0)),
        p,
    );

    for (chip, rect) in app.remote_chips.iter().zip(&app.view.remote_chip_hit_areas) {
        if rect.width == 0 {
            continue;
        }
        render_chip(app, chip, *rect, frame);
    }
}

fn render_chip(app: &AppState, chip: &RemoteChipState, rect: Rect, frame: &mut Frame) {
    let p = &app.palette;
    // Both terminal states dim: neither is coming back on its own, and the
    // user has to do something about it.
    let greyed = matches!(
        chip.connection,
        RemoteChipConnection::Incompatible | RemoteChipConnection::Stopped
    );
    let base = if chip.in_view && !greyed {
        Style::default().fg(p.text).bg(p.surface0)
    } else if chip.in_view {
        Style::default()
            .fg(p.overlay0)
            .bg(p.surface0)
            .add_modifier(Modifier::DIM)
    } else {
        Style::default().fg(p.overlay0).add_modifier(Modifier::DIM)
    };
    let (dot, dot_color) = match chip.connection {
        RemoteChipConnection::Connected => (
            "●",
            if chip.in_view {
                p.remote_hue(chip.hue_index)
            } else {
                p.overlay0
            },
        ),
        RemoteChipConnection::Connecting => (
            crate::ui::spinner_frame(app.spinner_tick),
            if chip.in_view { p.yellow } else { p.overlay0 },
        ),
        // A stopped remote is reachable but idle: a filled-but-dim dot
        // reads as "there, not running", distinct from offline's hollow one.
        RemoteChipConnection::Stopped => ("◍", p.overlay0),
        RemoteChipConnection::Offline | RemoteChipConnection::Incompatible => ("○", p.overlay0),
    };
    let dot_style = base.fg(dot_color);
    let name_w = rect.width.saturating_sub(4) as usize;
    let name = super::text::truncate_end(&chip.name, name_w);
    let spans = vec![
        Span::styled(" ", base),
        Span::styled(dot.to_owned(), dot_style),
        Span::styled(format!(" {name} "), base),
    ];
    frame.render_widget(Paragraph::new(Line::from(spans)), rect);
}

// ---------------------------------------------------------------------
// Add/edit-remote dialog: the same modal grammar as the worktree dialogs.
// ---------------------------------------------------------------------

// The dialog and hit-test helpers are consumed only by the unix-only
// pure-client run path (#20/#23); the strip renderer itself is shared.
#[cfg_attr(windows, allow(dead_code))]
const REMOTE_EDIT_POPUP_WIDTH: u16 = 52;
#[cfg_attr(windows, allow(dead_code))]
const REMOTE_EDIT_POPUP_HEIGHT: u16 = 14;

/// Width and height of the start-a-stopped-remote confirmation.
const REMOTE_START_POPUP_WIDTH: u16 = 52;
const REMOTE_START_POPUP_HEIGHT: u16 = 10;

/// Inner rect of the start-remote confirmation, for render and hit-testing.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_start_inner_rect(area: Rect) -> Option<Rect> {
    super::widgets::centered_popup_rect(area, REMOTE_START_POPUP_WIDTH, REMOTE_START_POPUP_HEIGHT)
        .map(|popup| {
            Rect::new(
                popup.x + 1,
                popup.y + 1,
                popup.width.saturating_sub(2),
                popup.height.saturating_sub(2),
            )
        })
}

/// (start, cancel) button rects inside the confirmation.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_start_button_rects(inner: Rect) -> (Rect, Rect) {
    let rects = super::widgets::action_button_row_rects(
        inner,
        &[
            super::widgets::ActionButtonSpec {
                hint: Some("↵"),
                label: "start",
            },
            super::widgets::ActionButtonSpec {
                hint: Some("esc"),
                label: "not now",
            },
        ],
        2,
        inner.height.saturating_sub(1),
    );
    (rects[0], rects[1])
}

/// Draws the start-a-stopped-remote confirmation over the composed view.
///
/// Follows the add/edit dialog's shell, header and button row so the fleet's
/// two modals read as one family.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn render_remote_start_overlay(
    app: &AppState,
    prompt: &crate::client_state::remote_start::RemoteStartPrompt,
    frame: &mut Frame,
) {
    use ratatui::layout::{Constraint, Layout};
    use ratatui::text::{Line, Span};
    use ratatui::widgets::{Paragraph, Wrap};

    let area = frame.area();
    super::dim_background(frame, area);
    let Some(inner) = super::widgets::render_modal_shell(
        frame,
        area,
        REMOTE_START_POPUP_WIDTH,
        REMOTE_START_POPUP_HEIGHT,
        &app.palette,
    ) else {
        return;
    };
    if inner.height < 6 {
        return;
    }
    let p = &app.palette;
    let rows = Layout::vertical([
        Constraint::Length(1), // header
        Constraint::Length(1), // spacer
        Constraint::Length(2), // body
        Constraint::Length(2), // detail / error
        Constraint::Min(0),
    ])
    .areas::<5>(inner);

    super::widgets::render_modal_header(frame, rows[0], "start remote", p);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(prompt.name.clone(), Style::default().fg(p.text)),
            Span::styled(
                format!(" has no {} server running.", crate::identity::BRAND),
                Style::default().fg(p.subtext0),
            ),
        ]))
        .wrap(Wrap { trim: true }),
        rows[2],
    );

    // The failure of a previous attempt outranks the explanation: it is what
    // the user needs to act on.
    let (detail, detail_style) = match prompt.error.as_deref() {
        Some(error) => (
            format!("could not start it: {error}"),
            Style::default().fg(p.red),
        ),
        // Which machine the daemon lands on is the whole question this
        // prompt asks, so it must not name the wrong one: a local runtime
        // starts here, by re-running this program.
        None if prompt.local => (
            format!(
                "Starting one runs a {} daemon on this machine.",
                crate::identity::BRAND
            ),
            Style::default().fg(p.subtext0),
        ),
        None => (
            format!(
                "Starting one runs a {} daemon on that machine.",
                crate::identity::BRAND
            ),
            Style::default().fg(p.subtext0),
        ),
    };
    frame.render_widget(
        Paragraph::new(detail)
            .style(detail_style)
            .wrap(Wrap { trim: true }),
        rows[3],
    );

    let (start_rect, cancel_rect) = remote_start_button_rects(inner);
    super::widgets::render_action_button(
        frame,
        start_rect,
        Some("↵"),
        "start",
        Style::default()
            .fg(super::widgets::panel_contrast_fg(p))
            .bg(p.accent)
            .add_modifier(Modifier::BOLD),
    );
    super::widgets::render_action_button(
        frame,
        cancel_rect,
        Some("esc"),
        "not now",
        Style::default()
            .fg(p.text)
            .bg(p.surface0)
            .add_modifier(Modifier::BOLD),
    );
}

/// The remotes list popup. Wider than the single-remote dialog because a row
/// carries name, target, session and a status label side by side.
const REMOTE_LIST_POPUP_WIDTH: u16 = 66;
/// Chrome rows around the list: border, header, gaps, key hints, `[done]`.
const REMOTE_LIST_CHROME_ROWS: u16 = 10;
const REMOTE_LIST_MIN_HEIGHT: u16 = 12;
const REMOTE_LIST_MAX_HEIGHT: u16 = 26;

/// Popup height for `count` rows, clamped so a large fleet scrolls rather
/// than filling the screen and a small one does not float in empty space.
fn remote_list_popup_height(count: usize) -> u16 {
    let wanted = REMOTE_LIST_CHROME_ROWS.saturating_add(count.min(u16::MAX as usize) as u16);
    wanted.clamp(REMOTE_LIST_MIN_HEIGHT, REMOTE_LIST_MAX_HEIGHT)
}

// Consumed only by the unix-only pure-client run path.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_list_inner_rect(area: Rect, count: usize) -> Option<Rect> {
    super::widgets::centered_popup_rect(
        area,
        REMOTE_LIST_POPUP_WIDTH,
        remote_list_popup_height(count),
    )
    .map(|popup| {
        Rect::new(
            popup.x + 1,
            popup.y + 1,
            popup.width.saturating_sub(2),
            popup.height.saturating_sub(2),
        )
    })
}

/// The stacked areas of the list modal, so render and hit-test agree.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
fn remote_list_areas(inner: Rect) -> super::widgets::ModalStackAreas {
    super::widgets::modal_stack_areas(inner, 1, 2, 1, 1)
}

/// Per-row hit rects, parallel to the modal's rows.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_list_row_rects(inner: Rect, count: usize) -> Vec<Rect> {
    super::widgets::modal_choice_rows(remote_list_areas(inner).content, count, 1)
}

/// The `[done]` control, so closing is a mouse action as well as an Escape.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_list_done_rect(inner: Rect) -> Rect {
    let Some(actions) = remote_list_areas(inner).actions else {
        return Rect::default();
    };
    super::widgets::action_button_row_rects(
        actions,
        &[super::widgets::ActionButtonSpec {
            hint: Some("esc"),
            label: "done",
        }],
        2,
        0,
    )
    .first()
    .copied()
    .unwrap_or_default()
}

/// Draws the fleet as a list: every configured remote, disabled ones
/// included, with a live status dot.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn render_remote_list_overlay(
    app: &AppState,
    list: &crate::client_state::remote_list::RemoteListState,
    frame: &mut Frame,
) {
    let area = frame.area();
    super::dim_background(frame, area);
    let Some(inner) = super::widgets::render_modal_shell(
        frame,
        area,
        REMOTE_LIST_POPUP_WIDTH,
        remote_list_popup_height(list.rows.len()),
        &app.palette,
    ) else {
        return;
    };
    let p = &app.palette;
    let areas = remote_list_areas(inner);
    super::widgets::render_modal_header(frame, areas.header, "remotes", p);

    if list.rows.is_empty() {
        frame.render_widget(
            Paragraph::new(" no remotes configured").style(Style::default().fg(p.overlay0)),
            areas.content,
        );
    }

    for (row, rect) in list
        .rows
        .iter()
        .zip(remote_list_row_rects(inner, list.rows.len()))
    {
        let selected = list
            .rows
            .get(list.selected)
            .is_some_and(|current| current.entry.name == row.entry.name);
        let base = if selected {
            Style::default()
                .fg(p.text)
                .bg(p.surface0)
                .add_modifier(Modifier::BOLD)
        } else if row.entry.enabled {
            Style::default().fg(p.text)
        } else {
            // A disabled remote stays visible so it can be found again and
            // re-enabled, but reads as out of the fleet.
            Style::default().fg(p.overlay0).add_modifier(Modifier::DIM)
        };
        let target = row.entry.target.as_deref().unwrap_or("local");
        // Name and target are what tell two similarly named remotes apart.
        let name = truncate_end(&row.entry.name, 16);
        let target = truncate_end(target, 22);
        let session = truncate_end(&row.entry.session, 10);
        let line = Line::from(vec![
            Span::styled(
                format!(" {} ", row.status.dot()),
                Style::default().fg(
                    if row.status == crate::client_state::remote_list::RemoteListStatus::Connected {
                        p.remote_hue(row.entry.hue.unwrap_or(0))
                    } else {
                        p.overlay0
                    },
                ),
            ),
            Span::styled(format!("{name:<16} "), base),
            Span::styled(format!("{target:<22} "), Style::default().fg(p.subtext0)),
            Span::styled(format!("{session:<10} "), Style::default().fg(p.overlay0)),
            Span::styled(row.status.label(), Style::default().fg(p.overlay0)),
        ]);
        frame.render_widget(Paragraph::new(line).style(base), rect);
    }

    // A keyboard-driven list should not need documentation to use.
    if let Some(footer) = areas.footer {
        let hint = |key: &'static str, label: &'static str| {
            vec![
                Span::styled(
                    key,
                    Style::default().fg(p.accent).add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!(" {label}  "), Style::default().fg(p.overlay0)),
            ]
        };
        let mut keys = Vec::new();
        keys.extend(hint("↑↓", "select"));
        keys.extend(hint("⇧↑↓", "reorder"));
        keys.extend(hint("space", "enable"));
        keys.extend(hint("↵", "edit"));
        frame.render_widget(Paragraph::new(Line::from(keys)), footer);

        let mut more = Vec::new();
        more.extend(hint("s", "start/stop"));
        more.extend(hint("del", "remove"));
        let second = Rect::new(
            footer.x,
            footer.y.saturating_add(1),
            footer.width,
            footer.height.saturating_sub(1),
        );
        if second.height > 0 {
            frame.render_widget(Paragraph::new(Line::from(more)), second);
        }
    }

    // A refused write surfaces here and leaves the file untouched.
    if let Some(error) = &list.error {
        if let Some(actions) = areas.actions {
            let row = Rect::new(actions.x, actions.y, actions.width.saturating_sub(10), 1);
            frame.render_widget(
                Paragraph::new(truncate_end(error, row.width as usize))
                    .style(Style::default().fg(p.red)),
                row,
            );
        }
    }

    if areas.actions.is_some() {
        super::widgets::render_action_button(
            frame,
            remote_list_done_rect(inner),
            Some("esc"),
            "done",
            Style::default().fg(p.text).bg(p.surface0),
        );
    }
}

/// Inner rect of the add/edit-remote dialog, for render and hit-testing.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_edit_inner_rect(area: Rect) -> Option<Rect> {
    super::widgets::centered_popup_rect(area, REMOTE_EDIT_POPUP_WIDTH, REMOTE_EDIT_POPUP_HEIGHT)
        .map(|popup| {
            Rect::new(
                popup.x + 1,
                popup.y + 1,
                popup.width.saturating_sub(2),
                popup.height.saturating_sub(2),
            )
        })
}

/// (save, cancel) button rects inside the dialog.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_edit_button_rects(inner: Rect) -> (Rect, Rect) {
    let rects = super::widgets::action_button_row_rects(
        inner,
        &[
            super::widgets::ActionButtonSpec {
                hint: Some("↵"),
                label: "save",
            },
            super::widgets::ActionButtonSpec {
                hint: Some("esc"),
                label: "cancel",
            },
        ],
        2,
        inner.height.saturating_sub(1),
    );
    (rects[0], rects[1])
}

/// Draws the add/edit-remote dialog over the composed view.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn render_remote_edit_overlay(
    app: &AppState,
    dialog: &crate::client_state::remote_edit::RemoteEditState,
    frame: &mut Frame,
) {
    use ratatui::layout::{Constraint, Layout};
    use ratatui::widgets::{Clear, Wrap};

    let area = frame.area();
    super::dim_background(frame, area);
    let Some(inner) = super::widgets::render_modal_shell(
        frame,
        area,
        REMOTE_EDIT_POPUP_WIDTH,
        REMOTE_EDIT_POPUP_HEIGHT,
        &app.palette,
    ) else {
        return;
    };
    if inner.height < 10 {
        return;
    }
    let p = &app.palette;
    let rows = Layout::vertical([
        Constraint::Length(1), // header
        Constraint::Length(1), // name label
        Constraint::Length(1), // name input
        Constraint::Length(1), // target label
        Constraint::Length(1), // target input
        Constraint::Length(1), // session label
        Constraint::Length(1), // session input
        Constraint::Length(1), // error / hint
        Constraint::Min(0),
    ])
    .areas::<9>(inner);

    let title = if dialog.is_edit() {
        "edit remote"
    } else {
        "add remote"
    };
    super::widgets::render_modal_header(frame, rows[0], title, p);

    // ssh is a transport, not the definition of a remote: an entry with no
    // target is a runtime on this machine, reached over its API socket. The
    // placeholder is where that is discoverable - the field is otherwise
    // indistinguishable from one the user simply has not filled in yet.
    let fields = [
        ("name", &dialog.name, "", rows[1], rows[2]),
        (
            "ssh target",
            &dialog.target,
            "this machine (no ssh)",
            rows[3],
            rows[4],
        ),
        ("session", &dialog.session, "", rows[5], rows[6]),
    ];
    for (field_idx, (label, value, placeholder, label_rect, input_rect)) in
        fields.into_iter().enumerate()
    {
        frame.render_widget(
            Paragraph::new(format!(" {label}")).style(Style::default().fg(p.overlay0)),
            label_rect,
        );
        let focused = dialog.focused_field == field_idx;
        let cursor = if focused { "█" } else { "" };
        let input_style = Style::default()
            .fg(if focused { p.text } else { p.subtext0 })
            .bg(p.surface0);
        frame.render_widget(Clear, input_rect);
        let line = if value.is_empty() && !placeholder.is_empty() {
            Line::from(vec![
                Span::styled(format!(" {cursor}"), input_style),
                Span::styled(
                    placeholder.to_owned(),
                    input_style.fg(p.overlay0).add_modifier(Modifier::DIM),
                ),
            ])
        } else {
            Line::from(Span::styled(format!(" {value}{cursor}"), input_style))
        };
        frame.render_widget(Paragraph::new(line).style(input_style), input_rect);
    }

    if let Some(error) = &dialog.error {
        frame.render_widget(
            Paragraph::new(format!(" {error}"))
                .style(Style::default().fg(p.red))
                .wrap(Wrap { trim: false }),
            rows[7],
        );
    } else {
        let hint = if dialog.is_edit() {
            " ctrl-d removes this remote"
        } else {
            " leave ssh target blank for a local runtime"
        };
        frame.render_widget(
            Paragraph::new(hint).style(Style::default().fg(p.overlay0).add_modifier(Modifier::DIM)),
            rows[7],
        );
    }

    let (save_rect, cancel_rect) = remote_edit_button_rects(inner);
    super::widgets::render_action_button(
        frame,
        save_rect,
        Some("↵"),
        "save",
        Style::default()
            .fg(super::widgets::panel_contrast_fg(p))
            .bg(p.accent)
            .add_modifier(Modifier::BOLD),
    );
    super::widgets::render_action_button(
        frame,
        cancel_rect,
        Some("esc"),
        "cancel",
        Style::default()
            .fg(p.text)
            .bg(p.surface0)
            .add_modifier(Modifier::BOLD),
    );
}

/// The chip index under a point, if any.
// Consumed only by the unix-only pure-client run path (#20/#23).
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn remote_chip_at(app: &AppState, column: u16, row: u16) -> Option<usize> {
    app.view.remote_chip_hit_areas.iter().position(|rect| {
        rect.width > 0 && rect.contains(ratatui::layout::Position::new(column, row))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::state::RemoteChipConnection;

    fn chip(name: &str) -> RemoteChipState {
        RemoteChipState {
            name: name.to_owned(),
            hue_index: 0,
            in_view: true,
            connection: RemoteChipConnection::Connected,
        }
    }

    #[test]
    fn no_chips_reserves_no_rows_and_keeps_the_sidebar_untouched() {
        let area = Rect::new(0, 0, 26, 20);
        let (layout, rest) = split_sidebar_for_chip_strip(&[], area, false, false);
        assert_eq!(layout, RemoteChipStripLayout::default());
        assert_eq!(rest, area);

        // A collapsed sidebar never shows the strip either.
        let (layout, rest) = split_sidebar_for_chip_strip(&[chip("a")], area, true, true);
        assert_eq!(layout.strip_rect, Rect::default());
        assert_eq!(rest, area);
    }

    #[test]
    fn chips_wrap_across_rows_and_shift_the_sections_down() {
        let area = Rect::new(0, 0, 26, 24);
        let chips = vec![chip("local"), chip("buildbox"), chip("gpu-01")];
        let (layout, rest) = split_sidebar_for_chip_strip(&chips, area, false, true);

        // " ● local " (9) + gap + " ● buildbox " (12) = 22 <= 25 fits row 0;
        // gpu-01 wraps to row 1.
        assert_eq!(layout.chip_hit_areas.len(), 3);
        assert_eq!(layout.chip_hit_areas[0], Rect::new(1, 1, 9, 1));
        assert_eq!(layout.chip_hit_areas[1], Rect::new(11, 1, 12, 1));
        assert_eq!(layout.chip_hit_areas[2], Rect::new(1, 2, 10, 1));

        // header + two chip rows + gap row.
        assert_eq!(layout.strip_rect, Rect::new(0, 0, 26, 4));
        assert_eq!(rest, Rect::new(0, 4, 26, 20));
    }

    #[test]
    fn a_tiny_sidebar_drops_the_strip_rather_than_the_sections() {
        let area = Rect::new(0, 0, 26, 7);
        let (layout, rest) =
            split_sidebar_for_chip_strip(&[chip("a"), chip("b")], area, false, true);
        assert_eq!(layout.strip_rect, Rect::default());
        assert_eq!(rest, area);
    }

    #[test]
    fn overflowing_chips_do_not_reserve_a_phantom_blank_row() {
        // Height 9 leaves exactly one chip row: every chip lands on row 0
        // or is dropped, and the strip must not reserve a blank second row.
        let area = Rect::new(0, 0, 26, 9);
        let chips = vec![chip("local"), chip("buildbox"), chip("gpu-01")];
        let (layout, rest) = split_sidebar_for_chip_strip(&chips, area, false, true);
        assert!(layout.chip_hit_areas[0].width > 0);
        assert!(layout.chip_hit_areas[1].width > 0);
        assert_eq!(
            layout.chip_hit_areas[2],
            Rect::default(),
            "gpu-01 does not fit the single row"
        );
        // header + one chip row + gap: no phantom blank chip row.
        assert_eq!(layout.strip_rect.height, 3);
        assert_eq!(rest.height, 6, "sections keep their guaranteed rows");
    }

    #[test]
    fn chip_hit_testing_resolves_by_index() {
        let area = Rect::new(0, 0, 26, 24);
        let chips = vec![chip("local"), chip("gpu-01")];
        let (layout, _) = split_sidebar_for_chip_strip(&chips, area, false, true);
        let mut app = crate::app::AppState::test_new();
        app.remote_chips = chips;
        app.view.remote_chip_hit_areas = layout.chip_hit_areas;
        app.view.remote_chip_strip_rect = layout.strip_rect;
        assert_eq!(remote_chip_at(&app, 2, 1), Some(0));
        assert_eq!(remote_chip_at(&app, 12, 1), Some(1));
        assert_eq!(remote_chip_at(&app, 25, 1), None);
    }
}
