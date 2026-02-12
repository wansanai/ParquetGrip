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

fn main() -> eframe::Result<()> {
    // Initialize logging if needed
    // env_logger::init(); 

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1024.0, 768.0])
            .with_drag_and_drop(true),
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
    // Pagination state
    current_page: usize,
    page_size: usize,
    #[serde(skip)]
    total_rows: usize,
    // Filter and Sort state
    filter: String,
    sort: String,
    // Error state
    #[serde(skip)]
    last_error: Option<String>,
}

impl Tab {
    fn new(path: String) -> Self {
        let name = std::path::Path::new(&path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&path)
            .to_string();
        Self {
            path,
            name,
            schema: Vec::new(),
            data: Vec::new(),
            row_count: 0,
            status: "Opening...".to_string(),
            current_page: 1,
            page_size: 1000,
            total_rows: 0,
            filter: String::new(),
            sort: String::new(),
            last_error: None,
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
    // Store tabs data by path
    tabs: HashMap<String, Tab>,
    // Manage UI layout State. Tab identifier is the file path (String).
    dock_state: DockState<String>,
}

impl Default for ParquetApp {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            backend: Arc::new(Backend::new()),
            rx,
            tx_to_ui: tx,
            tabs: HashMap::new(),
            dock_state: DockState::new(Vec::new()),
        }
    }
}

impl ParquetApp {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // Customize visuals for a more professional look
        let mut visuals = egui::Visuals::dark();
        visuals.selection.bg_fill = egui::Color32::from_rgb(0, 120, 215); // Professional blue
        cc.egui_ctx.set_visuals(visuals);

        // Customize fonts
        setup_fonts(&cc.egui_ctx);
        
        let mut app: Self = if let Some(storage) = cc.storage {
            eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default()
        } else {
            Default::default()
        };

        // Re-initialize transient fields
        let (tx, rx) = mpsc::channel();
        app.tx_to_ui = tx;
        app.rx = rx;
        app.backend = Arc::new(Backend::new());

        // Re-load data for all tabs found in restored session
        for (path, tab) in app.tabs.iter_mut() {
            tab.last_error = None;
            tab.status = "Reloading session...".to_string();
            let tx_c = app.tx_to_ui.clone();
            let backend_c = app.backend.clone();
            let path_c = path.clone();
            let filter_c = tab.filter.clone();
            let sort_c = tab.sort.clone();
            let page = tab.current_page;
            let page_size = tab.page_size;

            std::thread::spawn(move || {
                // 1. Path notice
                let _ = tx_c.send(BackendMessage::FileOpened { path: path_c.clone() });
                
                // 2. Load schema
                if let Ok(s_msg) = backend_c.get_schema(path_c.clone()) {
                    let _ = tx_c.send(s_msg);
                }

                // 3. Get row count with filter
                let f = if filter_c.trim().is_empty() { None } else { Some(filter_c.clone()) };
                if let Ok(count) = backend_c.get_row_count(path_c.clone(), f) {
                    let _ = tx_c.send(BackendMessage::RowCount { path: path_c.clone(), count });
                }

                // 4. Load page
                let f = if filter_c.trim().is_empty() { None } else { Some(filter_c) };
                let s = if sort_c.trim().is_empty() { None } else { Some(sort_c) };
                let offset = (page - 1) * page_size;
                if let Ok(q_msg) = backend_c.run_query(path_c, f, s, Some(page_size), Some(offset)) {
                    let _ = tx_c.send(q_msg);
                }
            });
        }

        app
    }

    fn open_file_dialog(&mut self) {
        let backend = self.backend.clone();
        let tx = self.tx_to_ui.clone();
        
        let files = rfd::FileDialog::new()
            .add_filter("Parquet", &["parquet", "pqt"])
            .pick_files();
            
        if let Some(paths) = files {
            for path_buf in paths {
                let path = path_buf.to_string_lossy().to_string();
                
                // Add tab if not already open
                if !self.tabs.contains_key(&path) {
                    self.tabs.insert(path.clone(), Tab::new(path.clone()));
                    self.dock_state.push_to_focused_leaf(path.clone());
                    
                    let backend_c = backend.clone();
                    let tx_c = tx.clone();
                    let path_c = path.clone();
                    
                    std::thread::spawn(move || {
                        match backend_c.open_file(path_c.clone()) {
                            Ok(msg) => {
                                let _ = tx_c.send(msg);
                                // Get schema automatically
                                if let Ok(s_msg) = backend_c.get_schema(path_c.clone()) {
                                    let _ = tx_c.send(s_msg);
                                }
                                // Get row count (no filter yet)
                                if let Ok(count) = backend_c.get_row_count(path_c.clone(), None) {
                                    let _ = tx_c.send(BackendMessage::RowCount { path: path_c.clone(), count });
                                }
                                
                                // Run initial query (Page 1, no filter/sort)
                                if let Ok(q_msg) = backend_c.run_query(path_c, None, None, Some(1000), Some(0)) {
                                    let _ = tx_c.send(q_msg);
                                }
                            }
                            Err(e) => {
                                let _ = tx_c.send(BackendMessage::Error { 
                                    path: Some(path_c), 
                                    message: e 
                                });
                            }
                        }
                    });
                } else {
                    // If already open, we could try to focus it, but DockState doesn't make it trivial to "find and focus" 
                    // without traversing. For now, we simple do nothing or maybe user will find it.
                    // Improving this would be a nice polish later.
                }
            }
        }
    }
}

struct ParquetTabViewer<'a> {
    tabs: &'a mut HashMap<String, Tab>,
    tx: mpsc::Sender<BackendMessage>,
    backend: Arc<Backend>,
}

impl<'a> ParquetTabViewer<'a> {
    fn load_page(tx: mpsc::Sender<BackendMessage>, backend: Arc<Backend>, path: String, page: usize, page_size: usize, filter: String, sort: String) {
        std::thread::spawn(move || {
            let limit = page_size;
            let offset = (page - 1) * page_size;
            let f = if filter.trim().is_empty() { None } else { Some(filter) };
            let s = if sort.trim().is_empty() { None } else { Some(sort) };
            match backend.run_query(path.clone(), f, s, Some(limit), Some(offset)) {
                Ok(msg) => { let _ = tx.send(msg); }
                Err(e) => {
                    let _ = tx.send(BackendMessage::Error { 
                        path: Some(path), 
                        message: e 
                    });
                }
            }
        });
    }

    fn refresh_data(tx: mpsc::Sender<BackendMessage>, backend: Arc<Backend>, path: String, filter: String, sort: String, page_size: usize) {
        let tx_c = tx.clone();
        let backend_c = backend.clone();
        let path_c = path.clone();
        let filter_c = filter.clone();
        
        // 1. Refresh row count
        std::thread::spawn(move || {
            let f = if filter_c.trim().is_empty() { None } else { Some(filter_c) };
            match backend_c.get_row_count(path_c.clone(), f) {
                Ok(count) => { let _ = tx_c.send(BackendMessage::RowCount { path: path_c, count }); }
                Err(e) => { let _ = tx_c.send(BackendMessage::Error { path: Some(path_c), message: e }); }
            }
        });

        // 2. Load first page
        Self::load_page(tx, backend, path, 1, page_size, filter, sort);
    }
}

impl<'a> TabViewer for ParquetTabViewer<'a> {
    type Tab = String;

    fn title(&mut self, tab_id: &mut Self::Tab) -> egui::WidgetText {
        if let Some(tab) = self.tabs.get(tab_id) {
            if tab.name.chars().count() > 20 {
                format!("{}...", tab.name.chars().take(17).collect::<String>()).into()
            } else {
                tab.name.clone().into()
            }
        } else {
            "Loading...".into()
        }
    }

    fn on_close(&mut self, tab_id: &mut Self::Tab) -> OnCloseResponse {
        self.tabs.remove(tab_id);
        OnCloseResponse::Close
    }

    fn on_tab_button(&mut self, tab_id: &mut Self::Tab, response: &egui::Response) {
        if let Some(tab) = self.tabs.get(tab_id) {
            response.clone().on_hover_text(format!("Full Name: {}\nPath: {}", tab.name, tab.path));
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, tab_id: &mut Self::Tab) {
        if let Some(tab) = self.tabs.get_mut(tab_id) {
            ui.vertical(|ui| {
                // Determine content size manually or let ScrollArea handle it.
                // Since TableBuilder likes to take available size, we put it in a central area minus status bar.
                
                // Filter and Sort toolbar
                egui::TopBottomPanel::top(format!("toolbar_{}", tab.path))
                    .frame(egui::Frame::NONE.inner_margin(egui::Margin::symmetric(8, 4)))
                    .show_inside(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.label("WHERE");
                            let filter_input = ui.add(egui::TextEdit::singleline(&mut tab.filter)
                                .hint_text("e.g. id > 100")
                                .desired_width(200.0));
                            
                            ui.add_space(8.0);
                            
                            ui.label("ORDER BY");
                            let sort_input = ui.add(egui::TextEdit::singleline(&mut tab.sort)
                                .hint_text("e.g. id DESC")
                                .desired_width(150.0));
                            
                            ui.add_space(8.0);
                            
                            if ui.button("Apply").clicked() 
                                || (filter_input.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                                || (sort_input.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                            {
                                tab.current_page = 1;
                                tab.status = "Applying filters...".to_string();
                                Self::refresh_data(
                                    self.tx.clone(), 
                                    self.backend.clone(), 
                                    tab.path.clone(), 
                                    tab.filter.clone(), 
                                    tab.sort.clone(),
                                    tab.page_size
                                );
                            }
                        });
                    });

                // Error Panel (Dedicated area for full error messages)
                let mut clear_error = false;
                if let Some(error_msg) = &tab.last_error {
                    let error_msg_cloned = error_msg.clone();
                    egui::TopBottomPanel::bottom(format!("error_panel_{}", tab.path))
                        .resizable(true)
                        .default_height(60.0)
                        .frame(egui::Frame::group(ui.style())
                            .fill(ui.visuals().error_fg_color.linear_multiply(0.1))
                            .stroke(egui::Stroke::new(1.0, ui.visuals().error_fg_color))
                            .inner_margin(egui::Margin::same(8)))
                        .show_inside(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(egui::RichText::new("⚠ Error").strong().color(ui.visuals().error_fg_color));
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui.button("X").on_hover_text("Clear Error").clicked() {
                                        clear_error = true;
                                    }
                                    if ui.button("📋").on_hover_text("Copy Error").clicked() {
                                        ui.ctx().copy_text(error_msg_cloned.clone());
                                    }
                                });
                            });
                            egui::ScrollArea::vertical().show(ui, |ui| {
                                ui.add(egui::Label::new(egui::RichText::new(error_msg_cloned).color(ui.visuals().error_fg_color)).wrap());
                            });
                        });
                }
                if clear_error {
                    tab.last_error = None;
                }

                // Combined Status and Pagination bar at bottom
                egui::TopBottomPanel::bottom(format!("bottom_bar_{}", tab.path))
                    .min_height(32.0)
                    .show_inside(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.spacing_mut().item_spacing.x = 12.0;

                            // Row and Page Info combined
                            let start_row = (tab.current_page - 1) * tab.page_size + 1;
                            let end_row = (start_row + tab.data.len()).saturating_sub(1);
                            
                            let total_pages = (tab.total_rows as f64 / tab.page_size as f64).ceil() as usize;
                            let total_pages = if total_pages == 0 { 1 } else { total_pages };

                            ui.label(egui::RichText::new(format!(
                                "Showing {}-{} of {} rows | Page {}/{}", 
                                start_row, end_row, tab.total_rows, tab.current_page, total_pages
                            )).weak());
                            
                            if tab.status.contains("Loading") {
                                ui.separator();
                                ui.label(egui::RichText::new(&tab.status).color(ui.visuals().warn_fg_color));
                            }
                            
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                ui.spacing_mut().item_spacing.x = 8.0;

                                // Next button
                                if ui.add_enabled(
                                    tab.current_page < total_pages,
                                    egui::Button::new("Next ▶").min_size(egui::vec2(80.0, 24.0))
                                ).on_hover_text("Next Page").clicked() 
                                {
                                    tab.current_page += 1;
                                    Self::load_page(
                                        self.tx.clone(), 
                                        self.backend.clone(), 
                                        tab.path.clone(), 
                                        tab.current_page, 
                                        tab.page_size,
                                        tab.filter.clone(),
                                        tab.sort.clone()
                                    );
                                    tab.status = format!("Loading page {}...", tab.current_page);
                                }

                                // Prev button
                                if ui.add_enabled(
                                    tab.current_page > 1,
                                    egui::Button::new("◀ Prev").min_size(egui::vec2(80.0, 24.0))
                                ).on_hover_text("Previous Page").clicked()
                                {
                                    tab.current_page -= 1;
                                    Self::load_page(
                                        self.tx.clone(), 
                                        self.backend.clone(), 
                                        tab.path.clone(), 
                                        tab.current_page, 
                                        tab.page_size,
                                        tab.filter.clone(),
                                        tab.sort.clone()
                                    );
                                    tab.status = format!("Loading page {}...", tab.current_page);
                                }
                            });
                        });
                    });

                // Main table area takes the rest of the space
                egui::CentralPanel::default()
                    .frame(egui::Frame::NONE)
                    .show_inside(ui, |ui| {
                        egui::ScrollArea::horizontal()
                            .id_salt(format!("scroll_{}", tab.path))
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                let mut table = egui_extras::TableBuilder::new(ui)
                                    .striped(true)
                                    .resizable(true)
                                    .vscroll(true)
                                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center));
                                
                                // Row number column
                                table = table.column(Column::initial(40.0).at_least(40.0).resizable(true));
                                
                                for _ in 0..tab.schema.len() {
                                    table = table.column(Column::initial(150.0).at_least(100.0).resizable(true));
                                }
                                
                                table.header(28.0, |mut header| {
                                        header.col(|ui| { ui.strong("#"); });
                                        for name in &tab.schema {
                                            header.col(|ui| { ui.strong(name); });
                                        }
                                    })
                                    .body(|body| {
                                        let start_row_index = (tab.current_page - 1) * tab.page_size;
                                        body.rows(26.0, tab.data.len(), |mut row| {
                                            let row_index = row.index();
                                            // Display global row number
                                            row.col(|ui| { ui.label((start_row_index + row_index + 1).to_string()); }); 
                                            
                                            if let Some(row_data) = tab.data.get(row_index) {
                                                for (col_idx, _col_name) in tab.schema.iter().enumerate() {
                                                    if let Some(cell) = row_data.get(col_idx) {
                                                        row.col(|ui| {
                                                            if cell == "(null)" {
                                                                ui.label(egui::RichText::new(cell).color(ui.visuals().weak_text_color()));
                                                            } else {
                                                                ui.label(cell);
                                                            }
                                                        });
                                                    }
                                                }
                                            }
                                        });
                                    });
                            });
                    });
            });
        } else {
            ui.centered_and_justified(|ui| {
                ui.label("Tab data missing or loading error.");
            });
        }
    }
}

impl eframe::App for ParquetApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Poll for backend messages
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                BackendMessage::FileOpened { path } => {
                    if let Some(tab) = self.tabs.get_mut(&path) {
                        tab.status = "File opened, loading schema...".to_string();
                    }
                }
                BackendMessage::Schema { path, columns } => {
                    if let Some(tab) = self.tabs.get_mut(&path) {
                        tab.schema = columns;
                        tab.status = "Schema loaded, running query...".to_string();
                    }
                }
                BackendMessage::RowCount { path, count } => {
                    if let Some(tab) = self.tabs.get_mut(&path) {
                        tab.total_rows = count;
                        if tab.status.contains("Loading") || tab.status.contains("Reloading") {
                             tab.status.clear();
                        }
                    }
                }
                BackendMessage::QueryData { path, rows } => {
                    if let Some(tab) = self.tabs.get_mut(&path) {
                        tab.data = rows;
                        tab.row_count = tab.data.len();
                        tab.status.clear(); // Clear loading/ready status
                    }
                }
                BackendMessage::Error { path, message } => {
                    if let Some(p) = path {
                        if let Some(tab) = self.tabs.get_mut(&p) {
                            tab.last_error = Some(message.clone());
                            tab.status = "Query failed".to_string();
                        }
                    } else {
                        println!("Global Error: {}", message);
                    }
                }
            }
        }

        // Menu Bar
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::MenuBar::new().ui(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Open Parquet...").clicked() {
                        self.open_file_dialog();
                        ui.close();
                    }
                    if ui.button("Quit").clicked() {
                        std::process::exit(0);
                    }
                });
            });
        });

        // Main Dock Area
        egui::CentralPanel::default().show(ctx, |ui| {
             if self.tabs.is_empty() {
                 ui.centered_and_justified(|ui| {
                     ui.vertical_centered(|ui| {
                         ui.add_space(20.0);
                         ui.heading("📊 ParquetGrip");
                         ui.add_space(10.0);
                         ui.label("No file loaded. Open a Parquet file to explore your data.");
                         if ui.button("📁 Open Parquet...").clicked() {
                             self.open_file_dialog();
                         }
                     });
                 });
             } else {
                let mut tab_viewer = ParquetTabViewer {
                    tx: self.tx_to_ui.clone(),
                    backend: self.backend.clone(),
                    tabs: &mut self.tabs,
                };
                let mut style = Style::from_egui(ctx.style().as_ref());

                // Customize style
                style.tab_bar.height = 32.0; 
                style.tab.minimum_width = Some(80.0);
                
                DockArea::new(&mut self.dock_state)
                    .style(style)
                    .show_inside(ui, &mut tab_viewer);
             }
        });
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, self);
    }
}

fn setup_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();

    // Check for common CJK fonts on different OS
    let _font_names = [
        "PingFang SC",    // macOS
        "Microsoft YaHei", // Windows
        "Noto Sans CJK SC", // Linux / Generic
        "WenQuanYi Micro Hei", // Linux fallback
    ];

    let mut font_data: Option<egui::FontData> = None;
    let mut _font_name_found = "";

    // In a real robust app, we should use `font-loader` or similar crate to find file path.
    // Egui requires loading binary data.
    
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
    
    for path in font_paths {
        if std::path::Path::new(path).exists() {
            if let Ok(data) = std::fs::read(path) {
                 font_data = Some(egui::FontData::from_owned(data).tweak(
                     egui::FontTweak {
                         scale: 1.2, // Scaling for high-dpi
                         ..Default::default()
                     }
                 ));
                 _font_name_found = path;
                 break;
            }
        }
    }
    
    // Fallback: system-ui font (San Francisco) is usually available on Mac via system default, 
    // but it might not include CJK in the same file. Mac uses composite fonts.
    // Egui's default font is limited (Hack/Ubuntu).
    
    if let Some(fd) = font_data {
        fonts.font_data.insert("my_cjk_font".to_owned(), fd.into());
        
        // Put my_cjk_font first in Proportional
        if let Some(vec) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
            vec.insert(0, "my_cjk_font".to_owned());
        }
        
        // Put my_cjk_font last in Monospace (as fallback)
        if let Some(vec) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
            vec.push("my_cjk_font".to_owned());
        }
    } else {
        println!("Warning: Could not load CJK font from fixed path. Chinese might not render.");
    }

    ctx.set_fonts(fonts);
}
