use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Position},
    text::Line,
    widgets::{Block, Borders, Paragraph},
};

use unicode_width::UnicodeWidthChar;

#[derive(Debug)]
pub enum LoginStatus {
    NotSent,
    Fail,
    Success,
    Quit,
}

#[derive(Debug)]
pub struct LoginWindow {
    input: Vec<char>,
    server: String,
    limit: usize,
    status: LoginStatus,
}

impl LoginWindow {
    pub fn new(server: &str, limit: usize) -> Self {
        Self {
            input: Vec::new(),
            server: server.to_string(),
            limit: limit,
            status: LoginStatus::NotSent,

        }
    }

    // Retreival

    pub fn input(&self) -> &[char] {
        &self.input
    }

    pub fn server(&self) -> &str {
        &self.server
    }
    
    pub fn status(&self) -> &LoginStatus {
    	&self.status
    }

    // Input manipulation

    pub fn add_input(&mut self, c: char) {
        if self.input.len() < self.limit {
            self.input.push(c);
        }
    }

    pub fn remove_input(&mut self) {
        self.input.pop();
    }

    pub fn send_input(&mut self) -> String {
        self.input.drain(..).collect()
    }

    // Status manipulation

    pub fn set_notsent(&mut self) {
		self.status = LoginStatus::NotSent;
	}

	pub fn set_fail(&mut self) {
		self.status = LoginStatus::Fail;
	}

	pub fn set_success(&mut self) {
		self.status = LoginStatus::Success;
	}

	pub fn set_quit(&mut self) {
		self.status = LoginStatus::Quit;
	}

    // Rendering

    pub fn render(&self, frame: &mut Frame) {
		let area = frame.area();

		// Centre the login window
		let width = area.width.saturating_sub(2).min(56);
		let height = area.height.saturating_sub(2).min(9);

		let horizontal = Layout::default()
			.direction(Direction::Horizontal)
			.constraints([
				Constraint::Min(0),
				Constraint::Length(width),
				Constraint::Min(0),
			])
			.split(area);

		let vertical = Layout::default()
			.direction(Direction::Vertical)
			.constraints([
				Constraint::Min(0),
				Constraint::Length(height),
				Constraint::Min(0),
			])
			.split(horizontal[1]);

		let window_area = vertical[1];

		// Outer window
		let window = Block::default()
			.title(
				Line::from("Floppy 🐰")
					.alignment(Alignment::Center),
			)
			.title_bottom(
				Line::from("[ Enter - login | Esc - close ]")
					.alignment(Alignment::Center),
			)
			.borders(Borders::ALL);

		let inner_area = window.inner(window_area);

		frame.render_widget(window, window_area);

		// Centre the input box vertically
		let input_vertical = Layout::default()
			.direction(Direction::Vertical)
			.constraints([
				Constraint::Min(0),
				Constraint::Length(3),
				Constraint::Min(0),
			])
			.split(inner_area);

		// Centre the input box horizontally
		let input_width = inner_area.width.saturating_sub(4).min(48);

		let input_horizontal = Layout::default()
			.direction(Direction::Horizontal)
			.constraints([
				Constraint::Min(0),
				Constraint::Length(input_width),
				Constraint::Min(0),
			])
			.split(input_vertical[1]);

		let input_area = input_horizontal[1];

		// Input widget
		let input_block = Block::default()
			.borders(Borders::ALL)
			.title(" Username ");

		let input_inner = input_block.inner(input_area);

		let input: String = self.input.iter().collect();

		let paragraph = Paragraph::new(input)
			.block(input_block);

		frame.render_widget(paragraph, input_area);

		// Position cursor after the current input
		let cursor_width: u16 = self.input
			.iter()
			.map(|c| c.width().unwrap_or(0) as u16)
			.sum();

		let cursor_x = input_inner.x
			+ cursor_width.min(input_inner.width.saturating_sub(1));

		frame.set_cursor_position(Position::new(
			cursor_x,
			input_inner.y,
		));
	}
}
