//! Host terminal setup and restore for every Herdr client.
//!
//! Both the legacy thin client and the framed pane-stream attach client take
//! over the user's real terminal and must hand it back exactly as they found
//! it — on clean exit, on error, and on panic. That is one behavior, so it
//! lives in one module: [`setup_terminal`] / [`setup_direct_attach_terminal`]
//! return a [`TerminalGuard`] whose `Drop` runs [`restore_terminal_state`],
//! and the same restore function is what client panic hooks install.

use std::io::{self, Write as _};

use crossterm::event::{
    DisableBracketedPaste, DisableFocusChange, DisableMouseCapture, EnableBracketedPaste,
    EnableFocusChange, EnableMouseCapture,
};
#[cfg(not(windows))]
use crossterm::event::{PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags};
use crossterm::execute;
use crossterm::terminal::{DisableLineWrap, EnableLineWrap};

/// Sets up the terminal for client mode (raw mode, optional mouse, keyboard enhancements).
///
/// Returns a guard that restores the terminal when dropped.
pub(super) fn setup_terminal(mouse_capture: bool) -> io::Result<TerminalGuard> {
    setup_terminal_with_capabilities(true, mouse_capture)
}

/// Sets up a direct attach terminal.
///
/// Direct attach forwards stdin to the attached PTY. It enables mouse capture
/// so wheel events can drive the attached viewport or be forwarded to child
/// programs that requested mouse input.
pub(super) fn setup_direct_attach_terminal() -> io::Result<TerminalGuard> {
    setup_terminal_with_capabilities(false, true)
}

pub(super) fn setup_terminal_with_capabilities(
    enable_client_protocols: bool,
    mouse_capture: bool,
) -> io::Result<TerminalGuard> {
    ratatui::init();
    crate::terminal_modes::clear_host_mouse_reporting(&mut io::stdout())?;
    let host_color_scheme_reports =
        should_enable_host_color_scheme_reports(enable_client_protocols);

    if enable_client_protocols {
        if mouse_capture {
            set_mouse_capture(true)?;
        } else {
            set_mouse_capture(false)?;
        }
        execute!(io::stdout(), EnableBracketedPaste, EnableFocusChange)?;
        if host_color_scheme_reports {
            write_host_color_scheme_report_mode(&mut io::stdout(), true)?;
        }
        push_keyboard_enhancement_flags()?;
    } else {
        if should_query_host_terminal_theme() {
            write_host_color_scheme_report_mode(&mut io::stdout(), false)?;
        }
        if mouse_capture {
            set_mouse_capture(true)?;
        } else {
            set_mouse_capture(false)?;
        }
    }

    #[cfg(windows)]
    let windows_virtual_terminal_input =
        if enable_client_protocols && windows_vti_input_backend_enabled() {
            enable_windows_virtual_terminal_input()
        } else {
            WindowsVirtualTerminalInputSetup::default()
        };

    #[cfg(windows)]
    if enable_client_protocols
        && windows_vti_input_backend_enabled()
        && windows_virtual_terminal_input.active
        && windows_win32_input_mode_enabled()
    {
        if let Err(err) = enable_windows_win32_input_mode(&mut io::stdout()) {
            if let Some(mode) = windows_virtual_terminal_input.restore_mode {
                restore_windows_input_mode_value(mode);
            }
            return Err(err);
        }
    }

    let modify_other_keys_mode = enable_client_protocols
        .then(crate::input::host_modify_other_keys_mode)
        .flatten();
    if let Some(mode) = modify_other_keys_mode {
        io::stdout().write_all(mode.set_sequence())?;
        io::stdout().flush()?;
    }

    execute!(io::stdout(), DisableLineWrap)?;

    Ok(TerminalGuard {
        reset_modify_other_keys: modify_other_keys_mode.is_some(),
        reset_host_color_scheme_reports: host_color_scheme_reports,
        #[cfg(windows)]
        restore_windows_input_mode: windows_virtual_terminal_input.restore_mode,
    })
}

pub(super) fn should_enable_host_color_scheme_reports(enable_client_protocols: bool) -> bool {
    enable_client_protocols && should_query_host_terminal_theme()
}

/// Guard that restores the terminal when dropped.
pub(super) struct TerminalGuard {
    pub(super) reset_modify_other_keys: bool,
    pub(super) reset_host_color_scheme_reports: bool,
    #[cfg(windows)]
    pub(super) restore_windows_input_mode: Option<u32>,
}

pub(super) fn write_host_color_scheme_report_mode(
    writer: &mut impl io::Write,
    enabled: bool,
) -> io::Result<()> {
    let sequence = if enabled {
        crate::terminal_theme::HOST_COLOR_SCHEME_REPORT_ENABLE_SEQUENCE
    } else {
        crate::terminal_theme::HOST_COLOR_SCHEME_REPORT_DISABLE_SEQUENCE
    };
    writer.write_all(sequence.as_bytes())?;
    writer.flush()
}

pub(super) fn write_terminal_restore_postlude(
    writer: &mut impl io::Write,
    reset_host_color_scheme_reports: bool,
) -> io::Result<()> {
    if reset_host_color_scheme_reports {
        writer.write_all(
            crate::terminal_theme::HOST_COLOR_SCHEME_REPORT_DISABLE_SEQUENCE.as_bytes(),
        )?;
    }
    // Restore a visible cursor and reset DECSCUSR back to the terminal default.
    writer.write_all(b"\x1b[?25h\x1b[0 q")?;
    writer.flush()
}

#[cfg(windows)]
#[derive(Default)]
pub(super) struct WindowsVirtualTerminalInputSetup {
    pub(super) active: bool,
    pub(super) restore_mode: Option<u32>,
}

#[cfg(windows)]
pub(super) fn enable_windows_virtual_terminal_input() -> WindowsVirtualTerminalInputSetup {
    use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetStdHandle, SetConsoleMode, ENABLE_VIRTUAL_TERMINAL_INPUT,
        STD_INPUT_HANDLE,
    };

    let handle: HANDLE = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        tracing::warn!("failed to get Windows console input handle for VT input");
        return WindowsVirtualTerminalInputSetup::default();
    }

    let mut mode = 0;
    if unsafe { GetConsoleMode(handle, &mut mode) } == 0 {
        tracing::warn!("failed to read Windows console input mode for VT input");
        return WindowsVirtualTerminalInputSetup::default();
    }

    let desired = windows_virtual_terminal_input_mode(mode);
    if desired == mode {
        return WindowsVirtualTerminalInputSetup {
            active: true,
            restore_mode: None,
        };
    }

    if unsafe { SetConsoleMode(handle, desired) } == 0 {
        tracing::warn!("failed to enable Windows virtual terminal input");
        return WindowsVirtualTerminalInputSetup::default();
    }

    let mut applied = 0;
    if unsafe { GetConsoleMode(handle, &mut applied) } == 0 {
        tracing::warn!("failed to verify Windows virtual terminal input mode");
        let _ = unsafe { SetConsoleMode(handle, mode) };
        return WindowsVirtualTerminalInputSetup::default();
    }
    if applied & ENABLE_VIRTUAL_TERMINAL_INPUT == 0 {
        tracing::warn!("Windows virtual terminal input bit did not stick");
        let _ = unsafe { SetConsoleMode(handle, mode) };
        return WindowsVirtualTerminalInputSetup::default();
    }

    WindowsVirtualTerminalInputSetup {
        active: true,
        restore_mode: Some(mode),
    }
}

#[cfg(windows)]
pub(super) fn windows_vti_input_backend_enabled() -> bool {
    std::env::var("HERDR_WINDOWS_INPUT_BACKEND")
        .map(|backend| !backend.eq_ignore_ascii_case("crossterm"))
        .unwrap_or(true)
}

#[cfg(any(windows, test))]
pub(super) fn windows_virtual_terminal_input_mode(mode: u32) -> u32 {
    mode | 0x0200
}

#[cfg(windows)]
pub(super) fn restore_windows_input_mode_value(mode: u32) {
    use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Console::{GetStdHandle, SetConsoleMode, STD_INPUT_HANDLE};

    let handle: HANDLE = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return;
    }
    if unsafe { SetConsoleMode(handle, mode) } == 0 {
        tracing::warn!("failed to restore Windows console input mode");
    }
}

pub(super) fn set_mouse_capture(enabled: bool) -> io::Result<()> {
    crate::terminal_modes::clear_host_mouse_reporting(&mut io::stdout())?;
    if enabled {
        execute!(io::stdout(), EnableMouseCapture)
    } else {
        match execute!(io::stdout(), DisableMouseCapture) {
            Ok(()) => Ok(()),
            #[cfg(windows)]
            Err(err) if err.to_string() == "Initial console modes not set" => Ok(()),
            Err(err) => Err(err),
        }
    }
}

pub(super) fn restore_terminal_state(
    reset_modify_other_keys: bool,
    reset_host_color_scheme_reports: bool,
    #[cfg(windows)] restore_windows_input_mode: Option<u32>,
) {
    let _ = super::clear_received_kitty_graphics(&mut io::stdout());

    // Reset modifyOtherKeys if we enabled it.
    if reset_modify_other_keys {
        let _ = io::stdout().write_all(b"\x1b[>4;0m");
        let _ = io::stdout().flush();
    }

    let _ = pop_keyboard_enhancement_flags();

    let _ = execute!(
        io::stdout(),
        EnableLineWrap,
        DisableFocusChange,
        DisableBracketedPaste,
        DisableMouseCapture
    );
    let _ = crate::terminal_modes::clear_host_mouse_reporting(&mut io::stdout());
    #[cfg(windows)]
    if let Some(mode) = restore_windows_input_mode {
        restore_windows_input_mode_value(mode);
    }

    ratatui::restore();
    let _ = write_terminal_restore_postlude(&mut io::stdout(), reset_host_color_scheme_reports);

    #[cfg(windows)]
    if windows_vti_input_backend_enabled() && windows_win32_input_mode_enabled() {
        let _ = disable_windows_win32_input_mode(&mut io::stdout());
    }
}

#[cfg(not(windows))]
pub(super) fn push_keyboard_enhancement_flags() -> io::Result<()> {
    execute!(
        io::stdout(),
        PushKeyboardEnhancementFlags(crate::input::ime_compatible_keyboard_enhancement_flags())
    )
}

#[cfg(windows)]
pub(super) fn push_keyboard_enhancement_flags() -> io::Result<()> {
    Ok(())
}

#[cfg(not(windows))]
pub(super) fn pop_keyboard_enhancement_flags() -> io::Result<()> {
    execute!(io::stdout(), PopKeyboardEnhancementFlags)
}

#[cfg(windows)]
pub(super) fn pop_keyboard_enhancement_flags() -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
pub(super) fn windows_win32_input_mode_enabled() -> bool {
    std::env::var("HERDR_WINDOWS_INPUT_PROBE")
        .map(|probe| probe.eq_ignore_ascii_case("win32"))
        .unwrap_or(true)
}

#[cfg(windows)]
pub(super) fn enable_windows_win32_input_mode(writer: &mut impl std::io::Write) -> io::Result<()> {
    writer.write_all(b"\x1b[?9001h")?;
    writer.flush()
}

#[cfg(windows)]
pub(super) fn disable_windows_win32_input_mode(writer: &mut impl std::io::Write) -> io::Result<()> {
    writer.write_all(b"\x1b[?9001l")?;
    writer.flush()
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal_state(
            self.reset_modify_other_keys,
            self.reset_host_color_scheme_reports,
            #[cfg(windows)]
            self.restore_windows_input_mode,
        );
    }
}

pub(super) fn query_host_terminal_theme() {
    let _ = write_host_terminal_theme_query(io::stdout());
}

pub(super) fn should_query_host_terminal_theme() -> bool {
    !cfg!(windows)
}

pub(super) fn write_host_terminal_theme_query(mut writer: impl io::Write) -> io::Result<()> {
    let query = crate::terminal_theme::host_terminal_theme_query_sequence();
    writer.write_all(query.as_bytes())?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_host_terminal_theme_query_emits_osc_queries() {
        let mut output = Vec::new();
        write_host_terminal_theme_query(&mut output).unwrap();
        assert_eq!(
            output,
            crate::terminal_theme::host_terminal_theme_query_sequence().as_bytes()
        );
    }

    #[test]
    fn write_host_color_scheme_report_mode_emits_mode_sequences() {
        let mut output = Vec::new();
        write_host_color_scheme_report_mode(&mut output, true).unwrap();
        write_host_color_scheme_report_mode(&mut output, false).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(
            crate::terminal_theme::HOST_COLOR_SCHEME_REPORT_ENABLE_SEQUENCE.as_bytes(),
        );
        expected.extend_from_slice(
            crate::terminal_theme::HOST_COLOR_SCHEME_REPORT_DISABLE_SEQUENCE.as_bytes(),
        );
        assert_eq!(output, expected);
    }

    #[test]
    fn host_terminal_theme_query_is_disabled_on_windows() {
        assert_eq!(should_query_host_terminal_theme(), !cfg!(windows));
    }

    #[test]
    fn color_scheme_reports_are_enabled_only_for_full_clients() {
        assert_eq!(
            should_enable_host_color_scheme_reports(true),
            !cfg!(windows)
        );
        assert!(!should_enable_host_color_scheme_reports(false));
    }

    #[test]
    fn terminal_restore_postlude_restores_visible_default_cursor() {
        let mut output = Vec::new();
        write_terminal_restore_postlude(&mut output, false).unwrap();
        assert_eq!(output, b"\x1b[?25h\x1b[0 q");
    }

    #[test]
    fn terminal_restore_postlude_disables_color_scheme_reports_when_enabled() {
        let mut output = Vec::new();
        write_terminal_restore_postlude(&mut output, true).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(
            crate::terminal_theme::HOST_COLOR_SCHEME_REPORT_DISABLE_SEQUENCE.as_bytes(),
        );
        expected.extend_from_slice(b"\x1b[?25h\x1b[0 q");
        assert_eq!(output, expected);
    }

    #[test]
    fn windows_virtual_terminal_input_mode_sets_only_vti_bit() {
        assert_eq!(windows_virtual_terminal_input_mode(0x01f0), 0x03f0);
        assert_eq!(windows_virtual_terminal_input_mode(0x03f0), 0x03f0);
    }
}
