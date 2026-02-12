#!/bin/bash

# ParquetGrip 图标生成脚本
# 需要安装 imagemagick (brew install imagemagick)

ICON_SOURCE="assets/icon.png"
ICONSET_DIR="assets/icon.iconset"

if [ ! -f "$ICON_SOURCE" ]; then
    echo "错误: 找不到 assets/icon.png"
    exit 1
fi

echo "正在生成多平台图标资源..."

# 1. 生成 Windows .ico (包含多种尺寸)
if command -v magick &> /dev/null || command -v convert &> /dev/null; then
    # 尝试使用 imagemagick
    CONVERT_CMD="magick"
    if ! command -v magick &> /dev/null; then CONVERT_CMD="convert"; fi
    
    $CONVERT_CMD "$ICON_SOURCE" -define icon:auto-resize=256,128,64,48,32,16 assets/icon.ico
    echo "✅ 已生成 assets/icon.ico"
else
    echo "⚠️ 警告: 未安装 ImageMagick，跳过 .ico 生成。请运行 'brew install imagemagick'"
fi

# 2. 生成 macOS .icns
if [[ "$OSTYPE" == "darwin"* ]]; then
    mkdir -p "$ICONSET_DIR"
    
    # 定义需要的尺寸
    s_list=(16 32 64 128 256 512 1024)
    for s in "${s_list[@]}"; do
        s2=$(( s * 2 ))
        # 如果原始图够大，生成各种尺寸
        if [ "$s" -le 1024 ]; then
            s_half=$(( s / 2 ))
            # 基础尺寸
            cp "$ICON_SOURCE" "$ICONSET_DIR/icon_${s}x${s}.png" 2>/dev/null
            # 高清尺寸 (@2x)
            cp "$ICON_SOURCE" "$ICONSET_DIR/icon_${s/2}x${s/2}@2x.png" 2>/dev/null
        fi
    done
    
    # 实际上为了简单，直接缩放（如果安装了 imagemagick）
    if command -v magick &> /dev/null || command -v convert &> /dev/null; then
        $CONVERT_CMD "$ICON_SOURCE" -resize 16x16     "$ICONSET_DIR/icon_16x16.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 32x32     "$ICONSET_DIR/icon_16x16@2x.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 32x32     "$ICONSET_DIR/icon_32x32.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 64x64     "$ICONSET_DIR/icon_32x32@2x.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 128x128   "$ICONSET_DIR/icon_128x128.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 256x256   "$ICONSET_DIR/icon_128x128@2x.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 256x256   "$ICONSET_DIR/icon_256x256.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 512x512   "$ICONSET_DIR/icon_256x256@2x.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 512x512   "$ICONSET_DIR/icon_512x512.png"
        $CONVERT_CMD "$ICON_SOURCE" -resize 1024x1024 "$ICONSET_DIR/icon_512x512@2x.png"
    fi

    iconutil -c icns "$ICONSET_DIR"
    mv assets/icon.icns assets/icon.icns 2>/dev/null # 默认生成在 assets
    rm -rf "$ICONSET_DIR"
    echo "✅ 已生成 assets/icon.icns"
else
    echo "⚠️ 消息: 非 macOS 环境，跳过 .icns 生成。"
fi

echo "完成！资源已保存在 assets/ 目录下。"
