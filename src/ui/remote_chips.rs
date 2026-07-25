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

use super::text::display_width_u16;
use crate::app::state::{RemoteChipConnection, RemoteChipState};
use crate::app::AppState;

/// Header row plus one trailing gap row around the wrapped chip rows.
const STRIP_CHROME_ROWS: u16 = 2;
/// Label of the add affordance in the strip header.
const ADD_LABEL: &str = "add";
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
    /// The `add` affordance in the strip header.
    pub(crate) add_hit_area: Rect,
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
) -> (RemoteChipStripLayout, Rect) {
    if chips.is_empty() || sidebar_collapsed || sidebar_area.width <= 2 {
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
        x = x + width + 1;
    }

    let chip_rows = row + 1;
    let strip_h = chip_rows + STRIP_CHROME_ROWS;
    let strip_rect = Rect::new(sidebar_area.x, sidebar_area.y, sidebar_area.width, strip_h);
    let add_w = display_width_u16(ADD_LABEL);
    let add_hit_area = Rect::new(
        sidebar_area.x + content_w.saturating_sub(add_w + 1),
        sidebar_area.y,
        add_w,
        1,
    );
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
            add_hit_area,
        },
        rest,
    )
}

/// Draws the strip from the geometry computed into `ViewState`.
pub(crate) fn render_remote_chip_strip(app: &AppState, frame: &mut Frame) {
    let area = app.view.remote_chip_strip_rect;
    if area.width == 0 || area.height == 0 || app.remote_chips.is_empty() {
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

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            " remotes",
            Style::default().fg(p.overlay0).add_modifier(Modifier::BOLD),
        ))),
        Rect::new(area.x, area.y, area.width.saturating_sub(1), 1),
    );
    if app.view.remote_add_hit_area.width > 0 && app.mouse_capture {
        frame.render_widget(
            Paragraph::new(Span::styled(ADD_LABEL, Style::default().fg(p.overlay0))),
            app.view.remote_add_hit_area,
        );
    }

    for (chip, rect) in app.remote_chips.iter().zip(&app.view.remote_chip_hit_areas) {
        if rect.width == 0 {
            continue;
        }
        render_chip(app, chip, *rect, frame);
    }
}

fn render_chip(app: &AppState, chip: &RemoteChipState, rect: Rect, frame: &mut Frame) {
    let p = &app.palette;
    let greyed = chip.connection == RemoteChipConnection::Incompatible;
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

/// The chip index under a point, if any.
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
        let (layout, rest) = split_sidebar_for_chip_strip(&[], area, false);
        assert_eq!(layout, RemoteChipStripLayout::default());
        assert_eq!(rest, area);

        // A collapsed sidebar never shows the strip either.
        let (layout, rest) = split_sidebar_for_chip_strip(&[chip("a")], area, true);
        assert_eq!(layout.strip_rect, Rect::default());
        assert_eq!(rest, area);
    }

    #[test]
    fn chips_wrap_across_rows_and_shift_the_sections_down() {
        let area = Rect::new(0, 0, 26, 24);
        let chips = vec![chip("local"), chip("buildbox"), chip("gpu-01")];
        let (layout, rest) = split_sidebar_for_chip_strip(&chips, area, false);

        // " ● local " (9) + gap + " ● buildbox " (12) = 22 <= 25 fits row 0;
        // gpu-01 wraps to row 1.
        assert_eq!(layout.chip_hit_areas.len(), 3);
        assert_eq!(layout.chip_hit_areas[0], Rect::new(1, 1, 9, 1));
        assert_eq!(layout.chip_hit_areas[1], Rect::new(11, 1, 12, 1));
        assert_eq!(layout.chip_hit_areas[2], Rect::new(1, 2, 10, 1));

        // header + two chip rows + gap row.
        assert_eq!(layout.strip_rect, Rect::new(0, 0, 26, 4));
        assert_eq!(rest, Rect::new(0, 4, 26, 20));
        assert!(layout.add_hit_area.width > 0);
    }

    #[test]
    fn a_tiny_sidebar_drops_the_strip_rather_than_the_sections() {
        let area = Rect::new(0, 0, 26, 7);
        let (layout, rest) = split_sidebar_for_chip_strip(&[chip("a"), chip("b")], area, false);
        assert_eq!(layout.strip_rect, Rect::default());
        assert_eq!(rest, area);
    }

    #[test]
    fn chip_hit_testing_resolves_by_index() {
        let area = Rect::new(0, 0, 26, 24);
        let chips = vec![chip("local"), chip("gpu-01")];
        let (layout, _) = split_sidebar_for_chip_strip(&chips, area, false);
        let mut app = crate::app::AppState::test_new();
        app.remote_chips = chips;
        app.view.remote_chip_hit_areas = layout.chip_hit_areas;
        app.view.remote_chip_strip_rect = layout.strip_rect;
        assert_eq!(remote_chip_at(&app, 2, 1), Some(0));
        assert_eq!(remote_chip_at(&app, 12, 1), Some(1));
        assert_eq!(remote_chip_at(&app, 25, 1), None);
    }
}
