// ParquetGrip - A high-performance Parquet file viewer.
// Copyright (c) 2026 Edward (wansanai)
// SPDX-License-Identifier: MIT

use eframe::egui;
use egui_extras::Column;
use std::sync::{Arc, mpsc};
use std::collections::HashMap;
use egui_dock::{DockArea, DockState, Style, TabViewer};
use egui_dock::tab_viewer::OnCloseResponse;
use serde::{Deserialize, Serialize};

mod backend;
use backend::{Backend, BackendMessage};

#[derive(Serialize, Deserialize, Clone)]
struct LogEntry {
    time: String,
    path: String,
    sql: String,
    error: Option<String>,
}

fn main() -> eframe::Result<()> {
    let icon_bytes = include_bytes!("../assets/icon.png");
    let icon = match image::load_from_memory(icon_bytes) {
        Ok(image) => {
            let image = image.to_rgba8();
            let (width, height) = image.dimensions();
            Some(egui::IconData { rgba: image.into_raw(), width, height })
        }
        Err(_) => None,
    };

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1024.0, 768.0])
            .with_drag_and_drop(true)
            .with_icon(icon.unwrap_or_default()),
        ..Default::default()
    };
    
    eframe::run_native(
        "ParquetGrip",
        native_options,
        Box::new(|cc| Ok(Box::new(ParquetApp::new(cc)))),
    )
}

#[derive(Serialize, Deserialize)]
struct Tab {
    path: String,
    name: String,
    #[serde(skip)]
    schema: Vec<String>,
    #[serde(skip)]
    data: Vec<Vec<String>>,
    #[serde(skip)]
    row_count: usize,
    #[serde(skip)]
    status: String,
    current_page: usize,
    page_size: usize,
    #[serde(skip)]
    total_rows: usize,
    filter: String,
    sort: String,
    #[serde(skip)]
    last_error: Option<String>,
    #[serde(skip)]
    jump_page_buffer: String,
}

impl Tab {
    fn new(path: String) -> Self {
        let name = std::path::Path::new(&path).file_name().and_then(|n| n.to_str()).unwrap_or(&path).to_string();
        Self {
            path, name, schema: Vec::new(), data: Vec::new(), row_count: 0, status: "Opening...".to_string(),
            current_page: 1, page_size: 1000, total_rows: 0, filter: String::new(), sort: String::new(),
            last_error: None, jump_page_buffer: "1".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
struct ParquetApp {
    #[serde(skip)]
    backend: Arc<Backend>,
    #[serde(skip)]
    rx: mpsc::Receiver<BackendMessage>,
    #[serde(skip)]
    tx_to_ui: mpsc::Sender<BackendMessage>,
    tabs: HashMap<String, Tab>,
    dock_state: DockState<String>,
    #[serde(skip)]
    logs: Vec<LogEntry>,
    #[serde(skip)]
    show_console: bool,
}

impl Default for ParquetApp {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            backend: Arc::new(Backend::new()), rx, tx_to_ui: tx, tabs: HashMap::new(),
            dock_state: DockState::new(Vec::new()), logs: Vec::new(), show_console: false,
        }
    }
}

impl ParquetApp {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let mut visuals = egui::Visuals::dark();
        visuals.selection.bg_fill = egui::Color32::from_rgb(0, 120, 215); 
        cc.egui_ctx.set_visuals(visuals);
        setup_fonts(&cc.egui_ctx);
        
        let mut app: Self = if let Some(storage) = cc.storage { eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default() } else { Default::default() };
        let (tx, rx) = mpsc::channel();
        app.tx_to_ui = tx; app.rx = rx; app.backend = Arc::new(Backend::new());

        for (path, tab) in app.tabs.iter_mut() {
            tab.last_error = None; tab.status = "Reloading...".to_string();
            let (tx_c, b_c, path_c, f_c, s_c, p, ps) = (app.tx_to_ui.clone(), app.backend.clone(), path.clone(), tab.filter.clone(), tab.sort.clone(), tab.current_page, tab.page_size);
            std::thread::spawn(move || {
                let _ = tx_c.send(BackendMessage::FileOpened { path: path_c.clone() });
                if let Ok(s_msg) = b_c.get_schema(path_c.clone()) { let _ = tx_c.send(s_msg); }
                let f = if f_c.trim().is_empty() { None } else { Some(f_c.clone()) };
                if let Ok(msg) = b_c.get_row_count(path_c.clone(), f.clone()) { let _ = tx_c.send(msg); }
                let s = if s_c.trim().is_empty() { None } else { Some(s_c) };
                let offset = (p - 1) * ps;
                if let Ok(q_msg) = b_c.run_query(path_c, f, s, Some(ps), Some(offset)) { let _ = tx_c.send(q_msg); }
            });
        }
        app
    }

    fn open_file_dialog(&mut self) {
        let (backend, tx) = (self.backend.clone(), self.tx_to_ui.clone());
        if let Some(paths) = rfd::FileDialog::new().add_filter("Data Files", &["parquet", "pqt", "csv", "json", "gz"]).pick_files() {
            for path_buf in paths {
                let path = path_buf.to_string_lossy().to_string();
                if !self.tabs.contains_key(&path) {
                    self.tabs.insert(path.clone(), Tab::new(path.clone()));
                    self.dock_state.push_to_focused_leaf(path.clone());
                    let (b_c, tx_c, p_c) = (backend.clone(), tx.clone(), path.clone());
                    std::thread::spawn(move || {
                        match b_c.open_file(p_c.clone()) {
                            Ok(msg) => {
                                let _ = tx_c.send(msg);
                                if let Ok(s_msg) = b_c.get_schema(p_c.clone()) { let _ = tx_c.send(s_msg); }
                                if let Ok(msg) = b_c.get_row_count(p_c.clone(), None) { let _ = tx_c.send(msg); }
                                if let Ok(q_msg) = b_c.run_query(p_c, None, None, Some(1000), Some(0)) { let _ = tx_c.send(q_msg); }
                            }
                            Err(e) => { let _ = tx_c.send(BackendMessage::Error { path: Some(p_c), message: e, sql: None }); }
                        }
                    });
                }
            }
        }
    }
}

struct ParquetTabViewer<'a> { tabs: &'a mut HashMap<String, Tab>, tx: mpsc::Sender<BackendMessage>, backend: Arc<Backend> }

impl<'a> ParquetTabViewer<'a> {
    fn load_page(tx: mpsc::Sender<BackendMessage>, backend: Arc<Backend>, path: String, page: usize, page_size: usize, filter: String, sort: String) {
        std::thread::spawn(move || {
            let offset = (page - 1) * page_size;
            let f = if filter.trim().is_empty() { None } else { Some(filter) };
            let s = if sort.trim().is_empty() { None } else { Some(sort) };
            match backend.run_query(path.clone(), f, s, Some(page_size), Some(offset)) {
                Ok(msg) => { let _ = tx.send(msg); }
                Err(e) => { let _ = tx.send(BackendMessage::Error { path: Some(path), message: e, sql: None }); }
            }
        });
    }

    fn refresh_data(tx: mpsc::Sender<BackendMessage>, backend: Arc<Backend>, path: String, filter: String, sort: String, page_size: usize) {
        let (tx_c, b_c, p_c, f_c) = (tx.clone(), backend.clone(), path.clone(), filter.clone());
        std::thread::spawn(move || {
            let f = if f_c.trim().is_empty() { None } else { Some(f_c) };
            match b_c.get_row_count(p_c.clone(), f) {
                Ok(msg) => { let _ = tx_c.send(msg); }
                Err(e) => { let _ = tx_c.send(BackendMessage::Error { path: Some(p_c), message: e, sql: None }); }
            }
        });
        Self::load_page(tx, backend, path, 1, page_size, filter, sort);
    }
}

impl<'a> TabViewer for ParquetTabViewer<'a> {
    type Tab = String;

    fn title(&mut self, tab_id: &mut Self::Tab) -> egui::WidgetText {
        if let Some(tab) = self.tabs.get(tab_id) {
            if tab.name.chars().count() > 20 { format!("{}...", tab.name.chars().take(17).collect::<String>()).into() } else { tab.name.clone().into() }
        } else { "Loading...".into() }
    }

    fn on_close(&mut self, tab_id: &mut Self::Tab) -> OnCloseResponse { self.tabs.remove(tab_id); OnCloseResponse::Close }

    fn ui(&mut self, ui: &mut egui::Ui, tab_id: &mut Self::Tab) {
        if let Some(tab) = self.tabs.get_mut(tab_id) {
            ui.vertical(|ui| {
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.add_space(8.0); ui.label("WHERE");
                    let f_in = ui.add(egui::TextEdit::singleline(&mut tab.filter).hint_text("filter").desired_width(200.0));
                    ui.add_space(8.0); ui.label("ORDER BY");
                    let s_in = ui.add(egui::TextEdit::singleline(&mut tab.sort).hint_text("sort").desired_width(150.0));
                    if ui.button("Apply").clicked() || (f_in.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))) || (s_in.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))) {
                        tab.current_page = 1; tab.jump_page_buffer = "1".to_string(); tab.status = "Applying...".to_string();
                        Self::refresh_data(self.tx.clone(), self.backend.clone(), tab.path.clone(), tab.filter.clone(), tab.sort.clone(), tab.page_size);
                    }
                });
                ui.add_space(4.0); ui.separator();

                if !tab.status.is_empty() && tab.last_error.is_none() && !tab.data.is_empty() {
                    ui.horizontal(|ui| { ui.add_space(8.0); ui.add(egui::Spinner::new().size(14.0)); ui.label(egui::RichText::new(&tab.status).color(ui.visuals().warn_fg_color).small()); });
                    ui.separator();
                }

                egui::TopBottomPanel::bottom(format!("footer_{}", tab.path)).frame(egui::Frame::NONE.inner_margin(egui::Margin::symmetric(8, 6))).show_inside(ui, |ui| {
                    ui.horizontal(|ui| {
                        let total_p = (tab.total_rows as f64 / tab.page_size as f64).ceil() as usize;
                        let total_p = if total_p == 0 { 1 } else { total_p };
                        ui.label(egui::RichText::new(format!("Showing {}-{} of {} | Page {}/{}", (tab.current_page-1)*tab.page_size+1, ((tab.current_page-1)*tab.page_size+tab.data.len()), tab.total_rows, tab.current_page, total_p)).weak());
                        
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                ui.add_space(4.0);
                                if ui.add_enabled(tab.current_page < total_p, egui::Button::new("Next ▶")).clicked() {
                                    tab.current_page += 1; tab.jump_page_buffer = tab.current_page.to_string(); tab.status = format!("Loading {}...", tab.current_page);
                                    Self::load_page(self.tx.clone(), self.backend.clone(), tab.path.clone(), tab.current_page, tab.page_size, tab.filter.clone(), tab.sort.clone());
                                }
                                if ui.add_enabled(tab.current_page > 1, egui::Button::new("◀ Prev")).clicked() {
                                    tab.current_page -= 1; tab.jump_page_buffer = tab.current_page.to_string(); tab.status = format!("Loading {}...", tab.current_page);
                                    Self::load_page(self.tx.clone(), self.backend.clone(), tab.path.clone(), tab.current_page, tab.page_size, tab.filter.clone(), tab.sort.clone());
                                }
                                
                                ui.separator();
                                
                                // Jump to Page Group
                                if ui.add(egui::TextEdit::singleline(&mut tab.jump_page_buffer).desired_width(40.0)).lost_focus() || ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                                    if let Ok(p) = tab.jump_page_buffer.parse::<usize>() {
                                        let p = p.clamp(1, total_p); tab.current_page = p; tab.jump_page_buffer = p.to_string();
                                        Self::load_page(self.tx.clone(), self.backend.clone(), tab.path.clone(), tab.current_page, tab.page_size, tab.filter.clone(), tab.sort.clone());
                                    }
                                }
                                ui.label(egui::RichText::new("Go to:").weak());
                                
                                ui.separator();
                                
                                // Page Size Group
                                let mut ts = tab.page_size;
                                if egui::ComboBox::from_id_salt(format!("ps_{}", tab.path)).selected_text(tab.page_size.to_string()).width(70.0).show_ui(ui, |ui| {
                                    let mut c = false;
                                    for s in [100, 500, 1000, 5000, 10000] { if ui.selectable_value(&mut ts, s, s.to_string()).clicked() { c = true; } }
                                    c
                                }).inner.unwrap_or(false) {
                                    tab.page_size = ts; tab.current_page = 1; tab.jump_page_buffer = "1".to_string();
                                    Self::refresh_data(self.tx.clone(), self.backend.clone(), tab.path.clone(), tab.filter.clone(), tab.sort.clone(), tab.page_size);
                                }
                                ui.label(egui::RichText::new("Page Size:").weak());
                            });
                    });
                });

                egui::CentralPanel::default().frame(egui::Frame::NONE).show_inside(ui, |ui| {
                    if tab.data.is_empty() && !tab.status.is_empty() && tab.last_error.is_none() {
                        ui.centered_and_justified(|ui| { ui.vertical_centered(|ui| { ui.add(egui::Spinner::new().size(32.0)); ui.heading(&tab.status); }); });
                    } else {
                        egui::ScrollArea::both().id_salt(format!("scroll_{}", tab.path)).show(ui, |ui| {
                            let mut table = egui_extras::TableBuilder::new(ui).striped(true).resizable(true).cell_layout(egui::Layout::left_to_right(egui::Align::Center));
                            table = table.column(Column::initial(40.0).at_least(40.0));
                            for _ in 0..tab.schema.len() { table = table.column(Column::initial(150.0).at_least(100.0)); }
                            table.header(28.0, |mut h| { h.col(|ui| { ui.strong("#"); }); for n in &tab.schema { h.col(|ui| { ui.strong(n); }); } }).body(|b| {
                                let start = (tab.current_page - 1) * tab.page_size;
                                b.rows(26.0, tab.data.len(), |mut r| {
                                    let i = r.index(); r.col(|ui| { ui.label((start + i + 1).to_string()); });
                                    if let Some(rd) = tab.data.get(i) { for c in rd { r.col(|ui| { if c == "(null)" { ui.label(egui::RichText::new(c).weak()); } else { ui.label(c); } }); } }
                                });
                            });
                        });
                    }
                });
            });
        }
    }
}

impl eframe::App for ParquetApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        while let Ok(msg) = self.rx.try_recv() {
            let ts = chrono::Local::now().format("%H:%M:%S").to_string();
            match msg {
                BackendMessage::FileOpened { path } => { if let Some(t) = self.tabs.get_mut(&path) { t.status = "Opening...".to_string(); } }
                BackendMessage::Schema { path, columns } => { if let Some(t) = self.tabs.get_mut(&path) { t.schema = columns; } }
                BackendMessage::RowCount { path, count, sql } => { self.logs.push(LogEntry { time: ts.clone(), path: path.clone(), sql, error: None }); if let Some(t) = self.tabs.get_mut(&path) { t.total_rows = count; } }
                BackendMessage::QueryData { path, rows, sql } => { self.logs.push(LogEntry { time: ts, path: path.clone(), sql, error: None }); if let Some(t) = self.tabs.get_mut(&path) { t.data = rows; t.row_count = t.data.len(); t.status.clear(); } }
                BackendMessage::SqlLog { path, sql } => { self.logs.push(LogEntry { time: ts, path, sql, error: None }); }
                BackendMessage::Error { path, message, sql } => { self.logs.push(LogEntry { time: ts, path: path.clone().unwrap_or_else(|| "Global".to_string()), sql: sql.unwrap_or_else(|| "N/A".to_string()), error: Some(message.clone()) }); self.show_console = true; if let Some(p) = path { if let Some(t) = self.tabs.get_mut(&p) { t.last_error = Some(message); t.status = "Error".to_string(); } } }
            }
            if self.logs.len() > 100 { self.logs.remove(0); }
        }

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::MenuBar::new().ui(ui, |ui| {
                ui.menu_button("File", |ui| { if ui.button("Open File...").clicked() { self.open_file_dialog(); ui.close(); } if ui.button("Quit").clicked() { std::process::exit(0); } });
                ui.separator(); if ui.selectable_label(self.show_console, "Console").clicked() { self.show_console = !self.show_console; }
            });
        });

        if self.show_console {
            egui::TopBottomPanel::bottom("global_console").resizable(true).default_height(150.0).show(ctx, |ui| {
                ui.vertical(|ui| {
                    ui.horizontal(|ui| { ui.heading("Console"); ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| { if ui.button("Clear").clicked() { self.logs.clear(); } if ui.button("X").clicked() { self.show_console = false; } }); });
                    ui.separator(); egui::ScrollArea::vertical().stick_to_bottom(true).show(ui, |ui| {
                        for log in &self.logs {
                            ui.horizontal_top(|ui| {
                                ui.label(egui::RichText::new(&log.time).weak().monospace());
                                ui.vertical(|ui| {
                                    let fname = std::path::Path::new(&log.path).file_name().and_then(|n| n.to_str()).unwrap_or(&log.path);
                                    ui.label(egui::RichText::new(format!("[{}]", fname)).strong().color(egui::Color32::from_rgb(100, 150, 255))); ui.label(egui::RichText::new(&log.sql).monospace());
                                    if let Some(e) = &log.error { ui.label(egui::RichText::new(format!("Error: {}", e)).color(ui.visuals().error_fg_color)); }
                                });
                            }); ui.add_space(4.0);
                        }
                    });
                });
            });
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            if self.tabs.is_empty() { ui.centered_and_justified(|ui| { ui.vertical_centered(|ui| { ui.heading("📊 ParquetGrip"); if ui.button("📁 Open File...").clicked() { self.open_file_dialog(); } }); }); }
            else { let mut tv = ParquetTabViewer { tx: self.tx_to_ui.clone(), backend: self.backend.clone(), tabs: &mut self.tabs }; let mut style = Style::from_egui(ctx.style().as_ref()); style.tab_bar.height = 32.0; DockArea::new(&mut self.dock_state).style(style).show_inside(ui, &mut tv); }
        });
    }
    fn save(&mut self, storage: &mut dyn eframe::Storage) { eframe::set_value(storage, eframe::APP_KEY, self); }
}

fn setup_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();

    // Check for common CJK fonts on different OS
    let font_paths = [
        // macOS
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        // Windows
        "C:\\Windows\\Fonts\\msyh.ttc",     // Microsoft YaHei
        "C:\\Windows\\Fonts\\msyh.ttf",
        "C:\\Windows\\Fonts\\simsun.ttc",   // SimSun
        // Linux
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/wenquanyi/wqy-microhei/wqy-microhei.ttc",
    ];
    
    let mut font_data: Option<egui::FontData> = None;
    for path in font_paths {
        if std::path::Path::new(path).exists() {
            if let Ok(data) = std::fs::read(path) {
                 font_data = Some(egui::FontData::from_owned(data).tweak(
                     egui::FontTweak {
                         scale: 1.2, // Scaling for high-dpi
                         ..Default::default()
                     }
                 ));
                 break;
            }
        }
    }
    
    if let Some(fd) = font_data {
        fonts.font_data.insert("my_cjk_font".to_owned(), fd.into());
        
        if let Some(vec) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
            vec.insert(0, "my_cjk_font".to_owned());
        }
        
        if let Some(vec) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
            vec.push("my_cjk_font".to_owned());
        }
    }

    ctx.set_fonts(fonts);
}
