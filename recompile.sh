#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}==== Starting GeminiFS Recompile & Env Reset ====${NC}"

echo -e "${GREEN}[1/5] Cleaning up build directory...${NC}"
rm -rf build
mkdir -p build
cd build

echo -e "${GREEN}[2/5] Running CMake...${NC}"
cmake ..
if [ $? -ne 0 ]; then
    echo -e "${RED}CMake failed! Check your environment.${NC}"
    exit 1
fi

echo -e "${GREEN}[3/5] Building libnvm, integrity, and test_bench...${NC}"
make libnvm -j$(nproc)
make integrity -j$(nproc)
make test_bench -j$(nproc)
if [ $? -ne 0 ]; then
    echo -e "${RED}Make failed!${NC}"
    exit 1
fi

echo -e "${GREEN}[4/5] Building GeminiFS Lib...${NC}"
cd ../lib
make clean && make -j$(nproc)
cp libgeminiFs.a ../build/lib/

echo -e "${GREEN}[5/5] Resetting Kernel Environment (sudo password might be required)...${NC}"
cd ../module

sudo rmmod snvme 2>/dev/null || true
sudo rmmod snvme_core 2>/dev/null || true

sudo modprobe nvme
echo "Waiting for native NVMe driver to settle..."
sleep 2

sudo insmod snvme-core.ko
sudo insmod snvme.ko
if [ $? -ne 0 ]; then
    echo -e "${RED}Kernel module insertion failed! Please check dmesg.${NC}"
    exit 1
fi

cd ..

echo -e "${GREEN}==== All Done! Environment is perfectly clean. ====${NC}"
echo -e "You can now run your tests from the project root:"
echo -e "  Baseline:    ${GREEN}sudo ./build/bin/nvm-integrity-util${NC}"
echo -e "  Dual-Q Test: ${GREEN}sudo ./build/bin/nvm-test-bench${NC}"