use std::collections::{HashMap, VecDeque};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::Color,
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};

use unicode_width::UnicodeWidthChar;

#[derive(Debug)]
pub struct ChatWindow {
    input: Vec<char>,
    messages: VecDeque<Line<'static>>,
    username: String,
    i_limit: usize,
    m_limit: usize,
    quit: bool,
}

impl ChatWindow {
    pub fn new(username: String, i_limit: usize, m_limit: usize) -> Self {
        Self {
            input: Vec::new(),
            messages: VecDeque::new(),
            username: username,
            i_limit: i_limit,
            m_limit: m_limit,
            quit: false,
        }
    }

    // Input

    pub fn input(&self) -> &[char] {
        &self.input
    }
 
    pub fn add_input(&mut self, c: char) {
        if self.input.len() < self.i_limit {
            self.input.push(c);
        }
    }

    pub fn remove_input(&mut self) {
        self.input.pop();
    }

    pub fn send_input(&mut self) -> String {
        self.input.drain(..).collect::<String>()
    }

    // Messages

    pub fn add_message(&mut self, message: String) {
        self.push_line(Line::from(message));
    }

    pub fn add_unknown_message(&mut self, id: u16, body: String) {
        self.push_line(Line::from(vec![
            Span::styled(id.to_string(), Color::Red),
            Span::raw(format!("> {body}")),
        ]));
    }

    fn push_line(&mut self, message: Line<'static>) {
        if self.messages.len() >= self.m_limit {
            self.messages.pop_front();
        }
        self.messages.push_back(message);
    }

    // Username

    pub fn username(&self)  -> &str {
        &self.username
    }

    pub fn change_username(&mut self, username: &str) {
        self.username = String::from(username)
    }

    // Quit

    pub fn state(&self) -> bool {
        self.quit
    }

    pub fn quit(&mut self) {
        self.quit = true;
    }

    // Rendering

	pub fn render(&self, frame: &mut Frame, users: &HashMap<u16, String>) {
		let area = frame.area();

		let horizontal = Layout::default()
			.direction(Direction::Horizontal)
			.constraints([
				Constraint::Percentage(80),
				Constraint::Percentage(20),
			])
			.split(area);

		let vertical = Layout::default()
			.direction(Direction::Vertical)
			.constraints([
				Constraint::Min(0),
				Constraint::Length(4),
			])
			.split(horizontal[0]);

		let output_block = Block::default()
			.title("Chat")
			.borders(Borders::ALL);

		let output_area = output_block.inner(vertical[0]);

		let messages: Vec<_> = self.messages.iter().cloned().collect();
		let width = usize::from(output_area.width.max(1));
		let total_lines: usize = messages
			.iter()
			.map(|line| line.width().div_ceil(width).max(1))
			.sum();

		let visible_height = output_area.height as usize;
		let offset = visible_height.saturating_sub(total_lines);
		let scroll = total_lines.saturating_sub(visible_height);

		let output = Paragraph::new(messages)
			.wrap(Wrap { trim: false })
			.scroll((scroll as u16, 0));

		frame.render_widget(output_block, vertical[0]);

		let message_area = Rect::new(
			output_area.x,
			output_area.y + offset as u16,
			output_area.width,
			output_area.height - offset as u16,
		);

		frame.render_widget(output, message_area);

		let input_block = Block::default()
			.borders(Borders::ALL);

		let input_area = input_block.inner(vertical[1]);

		let input_string: String = self.input().iter().collect();

		let input_width = input_area.width as usize;

		if input_width > 0 {
			let total_width: usize = self.input()
				.iter()
				.map(|c| UnicodeWidthChar::width(*c).unwrap_or(0))
				.sum();

			let cursor_line = total_width / input_width;
			let cursor_column = total_width % input_width;

			let scroll = cursor_line.saturating_sub(1);

			let input = Paragraph::new(input_string)
				.wrap(Wrap { trim: false })
				.scroll((scroll as u16, 0));

			frame.render_widget(input_block, vertical[1]);
			frame.render_widget(input, input_area);

			frame.set_cursor_position(Position::new(
				input_area.x + cursor_column as u16,
				input_area.y + (cursor_line - scroll) as u16,
			));
		} else {
			frame.render_widget(input_block, vertical[1]);
		}

		let mut usernames: Vec<_> = users.values().map(String::as_str).collect();
		usernames.sort_unstable();
		let user_items = usernames.into_iter().map(ListItem::new);

		let users = List::new(user_items)
			.block(
				Block::default()
					.title("Users")
					.borders(Borders::ALL),
			);

		frame.render_widget(users, horizontal[1]);
	} 
}
