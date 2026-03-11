#!/bin/bash

# 1. 设置颜色输出（方便查看进度）
GREEN='\033[0-32m'
RED='\033[0-31m'
NC='\033[0m' # No Color

echo -e "${GREEN}==== Starting GeminiFS Recompile ====${NC}"

# 2. 清理并重新创建 build 目录
echo -e "${GREEN}[1/4] Cleaning up build directory...${NC}"
rm -rf build
mkdir -p build
cd build

# 3. 运行 CMake (自动探测 CUDA 和环境)
echo -e "${GREEN}[2/4] Running CMake...${NC}"
# 如果你的机器需要手动指定驱动路径，取消下面一行的注释并修改版本号
# cmake .. -DNVIDIA=/usr/src/nvidia-550.54.15/
cmake ..
if [ $? -ne 0 ]; then
    echo -e "${RED}CMake failed! Check your environment.${NC}"
    exit 1
fi

# 4. 编译核心组件
echo -e "${GREEN}[3/4] Building libnvm and integrity...${NC}"
make libnvm -j$(nproc)
make integrity -j$(nproc)
if [ $? -ne 0 ]; then
    echo -e "${RED}Make failed!${NC}"
    exit 1
fi

# 5. 编译 libgeminiFs 并拷贝
echo -e "${GREEN}[4/4] Building GeminiFS Lib...${NC}"
cd ../lib
make clean && make -j$(nproc)
cp libgeminiFs.a ../build/lib/

echo -e "${GREEN}==== All Done! ====${NC}"
echo -e "You can now run: ${GREEN}sudo ./build/bin/nvm-integrity-util${NC}"