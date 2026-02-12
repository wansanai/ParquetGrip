# 📊 ParquetGrip

[English](./README.en.md) | 简体中文


**ParquetGrip** 是一款基于 Rust 开发的高性能 Parquet 文件跨平台浏览器。它利用 **DuckDB** 作为核心查询引擎，结合 **egui** 打造极速、流畅的数据预览体验。

![ParquetGrip Icon](./assets/icon.png)

## ✨ 特性

- **极速加载与分页**：支持加载数 GB 级的超大型 Parquet 文件，通过内置分页机制（LIMIT/OFFSET）确保海量数据下 UI 依然丝滑。
- **DataGrip 式搜索**：
  - **SQL 过滤**：支持直接输入 `WHERE` 子句进行复杂过滤。
  - **即时排序**：支持 `ORDER BY` 子句对数据进行动态排序。
- **Session 持久化**：自动记住上次打开的文件、窗口布局（Docking）、过滤条件以及阅读页码，实现无缝衔接。
- **多标签页支持 (Multi-Tab)**：支持同时打开多个文件，在顶部标签栏快速切换，支持拖拽拆分窗口。
- **专业级报错系统**：独立的、可折叠的错误面板，支持 SQL 错误信息的一键复制。
- **跨平台支持**：针对 macOS、Windows 和 Linux 进行了字体适配和打包优化。
- **中文字体支持**：自动适配各系统原生字体（苹方、微软雅黑、Noto Sans 等），完美显示 CJK 字符。
- **易读的数据格式**：
  - **日期/时间**：自动处理原生时间戳，转换为易读格式。
  - **空值区分**：显式显示 `(null)` 并通过颜色弱化处理。

## 🚀 快速开始

### 安装工具链
- **Rust 编译器** (推荐 1.81+)

### 运行
```bash
cargo run --release
```

### 打包 (Bundling)
本项目已预配置 `cargo-bundle`，可生成各平台原生安装包：
```bash
cargo install cargo-bundle
cargo bundle --release
```
生成的产物（如 macOS 的 `.app`）将位于 `target/release/bundle/`。

## 🛠 技术栈

- **Engine**: [DuckDB](https://duckdb.org/)
- **UI Framework**: [egui](https://github.com/emilk/egui) & [egui_dock](https://github.com/Adanos020/egui_dock)
- **Serialization**: [Serde](https://serde.rs/)
- **CI/CD**: GitHub Actions (自动化多平台发布)

## ⚠️ 免责声明与贡献

本项目目前主要由开发者在 **macOS** 环境下开发。虽然已完成了 Windows 和 Linux 的适配配置，但由于缺乏设备，**其他平台尚未经过充分测试**。

如果您在非 macOS 平台上使用时遇到问题，或者愿意帮助完善其他平台的兼容性，**非常欢迎提交 Issue 或 Pull Request (PR)**！

## 📝 许可证

本项目采用 [MIT License](LICENSE) 开源。
