# 📊 ParquetGrip

**ParquetGrip** 是一款基于 Rust 开发的高性能 Parquet 文件跨平台浏览器（目前针对 macOS 15 进行了深度优化）。它利用 **DuckDB** 作为核心查询引擎，结合 **egui** 打造极速、流畅的数据预览体验。

![Screenshot Placeholder](https://via.placeholder.com/800x450.png?text=ParquetGrip+UI+Preview)

## ✨ 特性

- **极速加载**：支持加载数 GB 级的超大型 Parquet 文件，通过后端限流（50,000 行）确保 UI 秒开且无卡顿。
- **多标签页支持 (Multi-Tab)**：支持同时打开多个文件，在顶部标签栏快速切换，方便数据对比。
- **完美的 macOS 兼容性**：针对 macOS 15+ 遇到的 `winit` 崩溃问题进行了底层修复，运行稳定。
- **中文字体支持**：自动发现并加载 macOS 系统中文字体，确保 CJK 字符在 UI 中完美显示。
- **易读的数据格式**：
  - **日期/时间**：自动将原生时间戳转换为易读的日期和时间格式（如 `2023-10-27 14:30:00`）。
  - **空值区分**：显式显示 `(null)`，并以弱化颜色区分。
- **专业级 UI**：
  - 现代深色模式风格。
  - 支持横向和纵向滚动的虚拟长表（Virtual Table）。
  - 标签名智能截断，悬停查看完整路径。

## 🚀 快速开始

### 预备条件
- **Rust 编译器** (推荐 1.81 或更高版本)

### 安装与运行
1. 克隆仓库：
   ```bash
   git clone https://github.com/yourusername/ParquetGrip.git
   cd ParquetGrip
   ```
2. 运行应用（推荐使用 release 模式以获得最佳性能）：
   ```bash
   cargo run --release
   ```

## 🛠 技术栈

- **Engine**: [DuckDB](https://duckdb.org/) (通过 `duckdb` crate)
- **UI Framework**: [egui](https://github.com/emilk/egui) & [eframe](https://github.com/emilk/egui/tree/master/crates/eframe)
- **Data Types**: [Chrono](https://github.com/chronotope/chrono) (用于时间格式化)
- **File Dialog**: [rfd](https://github.com/PolyhedralDev/rfd)

## 📝 许可证

本项目采用 [MIT License](LICENSE) 开源。
