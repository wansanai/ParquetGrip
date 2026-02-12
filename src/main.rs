// ParquetGrip - A high-performance Parquet file viewer.
// Copyright (c) 2026 Edward (wansanai)
// SPDX-License-Identifier: MIT

use eframe::egui;
use egui_extras::Column;
use std::sync::{Arc, mpsc};

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

struct Tab {
    path: String,
    name: String,
    schema: Vec<String>,
    data: Vec<Vec<String>>,
    row_count: usize,
    status: String,
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
        }
    }
}

struct ParquetApp {
    backend: Arc<Backend>,
    rx: mpsc::Receiver<BackendMessage>,
    tx_to_ui: mpsc::Sender<BackendMessage>,
    tabs: Vec<Tab>,
    active_tab_index: Option<usize>,
}

impl ParquetApp {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // Customize visuals for a more professional look
        let mut visuals = egui::Visuals::dark();
        visuals.selection.bg_fill = egui::Color32::from_rgb(0, 120, 215); // Professional blue
        cc.egui_ctx.set_visuals(visuals);

        // Customize fonts
        setup_fonts(&cc.egui_ctx);
        
        let (tx, rx) = mpsc::channel();
        
        Self {
            backend: Arc::new(Backend::new()),
            rx,
            tx_to_ui: tx,
            tabs: Vec::new(),
            active_tab_index: None,
        }
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
                if !self.tabs.iter().any(|t| t.path == path) {
                    self.tabs.push(Tab::new(path.clone()));
                    self.active_tab_index = Some(self.tabs.len() - 1);
                    
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
                                // Run initial query
                                if let Ok(q_msg) = backend_c.run_query(path_c, "SELECT * FROM $TABLE".to_string()) {
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
                    // Switch to existing tab
                    if let Some(idx) = self.tabs.iter().position(|t| t.path == path) {
                        self.active_tab_index = Some(idx);
                    }
                }
            }
        }
    }
}

impl eframe::App for ParquetApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Poll for backend messages
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                BackendMessage::FileOpened { path } => {
                    if let Some(tab) = self.tabs.iter_mut().find(|t| t.path == path) {
                        tab.status = "File opened, loading schema...".to_string();
                    }
                }
                BackendMessage::Schema { path, columns } => {
                    if let Some(tab) = self.tabs.iter_mut().find(|t| t.path == path) {
                        tab.schema = columns;
                        tab.status = "Schema loaded, running query...".to_string();
                    }
                }
                BackendMessage::QueryData { path, rows } => {
                    if let Some(tab) = self.tabs.iter_mut().find(|t| t.path == path) {
                        tab.data = rows;
                        tab.row_count = tab.data.len();
                        tab.status = format!("Loaded {} rows", tab.row_count);
                    }
                }
                BackendMessage::Error { path, message } => {
                    if let Some(p) = path {
                        if let Some(tab) = self.tabs.iter_mut().find(|t| t.path == p) {
                             tab.status = format!("Error: {}", message);
                        }
                    } else {
                        // Global error or something?
                        println!("Global Error: {}", message);
                    }
                }
            }
        }

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
                // Tab Bar
                ui.horizontal_wrapped(|ui| {
                    let mut tab_to_close = None;
                    for (i, tab) in self.tabs.iter().enumerate() {
                        let is_active = self.active_tab_index == Some(i);
                        
                        let display_name = if tab.name.chars().count() > 25 {
                            let truncated: String = tab.name.chars().take(22).collect();
                            format!("{}...", truncated)
                        } else {
                            tab.name.clone()
                        };

                        ui.horizontal(|ui| {
                            ui.spacing_mut().item_spacing = egui::vec2(2.0, 0.0);
                            let response = ui.selectable_label(is_active, &display_name)
                                .on_hover_text(format!("Full Name: {}\nPath: {}", tab.name, tab.path));
                            
                            if response.clicked() {
                                self.active_tab_index = Some(i);
                            }
                            if ui.small_button("x").on_hover_text("Close Tab").clicked() {
                                tab_to_close = Some(i);
                            }
                        });
                        ui.add_space(5.0);
                        ui.separator();
                        ui.add_space(5.0);
                    }

                    if let Some(i) = tab_to_close {
                        self.tabs.remove(i);
                        if self.tabs.is_empty() {
                            self.active_tab_index = None;
                        } else {
                            let current_idx = self.active_tab_index.unwrap_or(0);
                            if i <= current_idx {
                                self.active_tab_index = Some(current_idx.saturating_sub(1).min(self.tabs.len() - 1));
                            }
                        }
                    }
                });

                ui.add_space(4.0);
                ui.separator();

                if let Some(idx) = self.active_tab_index {
                    if let Some(active_tab) = self.tabs.get(idx) {
                        egui::Frame::NONE
                            .inner_margin(8.0)
                            .show(ui, |ui| {
                                egui::ScrollArea::horizontal()
                                    .auto_shrink([false, false])
                                    .show(ui, |ui| {
                                        let mut table = egui_extras::TableBuilder::new(ui)
                                            .striped(true)
                                            .resizable(true)
                                            .vscroll(true)
                                            .cell_layout(egui::Layout::left_to_right(egui::Align::Center));
                                        
                                        table = table.column(Column::initial(40.0).at_least(40.0).resizable(true));
                                        for _ in 0..active_tab.schema.len() {
                                            table = table.column(Column::initial(150.0).at_least(100.0).resizable(true));
                                        }
                                        
                                        table.header(28.0, |mut header| {
                                                header.col(|ui| { ui.strong("#"); });
                                                for name in &active_tab.schema {
                                                    header.col(|ui| { ui.strong(name); });
                                                }
                                            })
                                            .body(|body| {
                                                body.rows(26.0, active_tab.data.len(), |mut row| {
                                                    let row_index = row.index();
                                                    row.col(|ui| { ui.label(row_index.to_string()); }); 
                                                    
                                                    if let Some(row_data) = active_tab.data.get(row_index) {
                                                        for i in 0..active_tab.schema.len() {
                                                            if let Some(cell) = row_data.get(i) {
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
                    }
                }
             }
        });
        
        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            if let Some(idx) = self.active_tab_index {
                if let Some(tab) = self.tabs.get(idx) {
                    ui.label(&tab.status);
                }
            } else {
                ui.label("Ready");
            }
        });
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
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/Supplemental/Songti.ttc",
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
