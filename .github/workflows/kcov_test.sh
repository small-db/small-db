#!/bin/sh

# 若命令失败让脚本退出
set -o errexit 
# 若未设置的变量被使用让脚本退出
set -o nounset
# 打开调试选项，让 shell 在终端上显示所有执行的命令及其参数
set -x

wget https://github.com/SimonKagstrom/kcov/archive/master.tar.gz &&
tar xzf master.tar.gz &&
cd kcov-master &&
mkdir build &&
cd build &&
cmake .. &&
make &&
make install DESTDIR=../../kcov-build &&
cd ../.. &&
rm -rf kcov-master &&
for file in target/debug/examplerust-*; do [ -x "${file}" ] || continue; mkdir -p "target/cov/$(basename $file)"; ./kcov-build/usr/local/bin/kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"; done &&
bash <(curl -s https://codecov.io/bash) &&
echo "Uploaded code coverage"