# 若命令失败让脚本退出
set -o errexit 
# 若未设置的变量被使用让脚本退出
set -o nounset
# 打开调试选项，让 shell 在终端上显示所有执行的命令及其参数
set -x

# wget https://github.com/SimonKagstrom/kcov/archive/master.tar.gz &&
# tar xzf master.tar.gz &&
# cd kcov-master &&
# mkdir build &&
# cd build &&
# cmake .. &&
# make &&
# make install DESTDIR=../../kcov-build &&
# cd ../.. &&
# rm -rf kcov-master &&
# export CODECOV_TOKEN=5c004654-e67d-472f-88f6-e1f850819c87
# for file in target/debug/examplerust-*; do [ -x "${file}" ] || continue; mkdir -p "target/cov/$(basename $file)"; ./kcov-build/usr/local/bin/kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"; done
# curl -s https://codecov.io/bash
# echo "Uploaded code coverage"

apt-get install libcurl4-openssl-dev libelf-dev libdw-dev cmake gcc binutils-dev libiberty-dev zlib1g-dev
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
for file in target/debug/deps/simple_db_rust-*; do [ -x "${file}" ] || continue; mkdir -p "target/cov/$(basename $file)"; ./kcov-build/usr/local/bin/kcov --exclude-pattern=/.cargo,/usr/lib --verify "target/cov/$(basename $file)" "$file"; done &&
bash <(curl -s https://codecov.io/bash) &&
echo "Uploaded code coverage"