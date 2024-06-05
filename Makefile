run:
	# This command will start the "small-db" server at localhost:5433.
	# 
	# Connect to the server with
	# `psql -h localhost -p 5433 -d default_db -U xiaochen`
	# 
	# We use "info" log level since there are lots debug logs from the dependencies.
	RUST_LOG=info cargo run --features "tree_latch"

gen:
	python scripts/compilation_options/gen.py

# Standard test. Doesn't print debug logs.
# 
# The ouput (stdout & stderr) of the test will be redirected to the file "*.log".
# ===[COMPILATION OPTIONS START]===
test:
	test_tree_latch_aries_steal_aries_force_read_uncommitted
	test_tree_latch_aries_steal_aries_force_read_committed
	test_tree_latch_aries_steal_aries_force_repeatable_read
	test_tree_latch_aries_steal_aries_force_serializable
	test_tree_latch_aries_steal_aries_no_force_read_uncommitted
	test_tree_latch_aries_steal_aries_no_force_read_committed
	test_tree_latch_aries_steal_aries_no_force_repeatable_read
	test_tree_latch_aries_steal_aries_no_force_serializable
	test_tree_latch_aries_no_steal_aries_force_read_uncommitted
	test_tree_latch_aries_no_steal_aries_force_read_committed
	test_tree_latch_aries_no_steal_aries_force_repeatable_read
	test_tree_latch_aries_no_steal_aries_force_serializable
	test_tree_latch_aries_no_steal_aries_no_force_read_uncommitted
	test_tree_latch_aries_no_steal_aries_no_force_read_committed
	test_tree_latch_aries_no_steal_aries_no_force_repeatable_read
	test_tree_latch_aries_no_steal_aries_no_force_serializable
	test_page_latch_aries_steal_aries_force_read_uncommitted
	test_page_latch_aries_steal_aries_force_read_committed
	test_page_latch_aries_steal_aries_force_repeatable_read
	test_page_latch_aries_steal_aries_force_serializable
	test_page_latch_aries_steal_aries_no_force_read_uncommitted
	test_page_latch_aries_steal_aries_no_force_read_committed
	test_page_latch_aries_steal_aries_no_force_repeatable_read
	test_page_latch_aries_steal_aries_no_force_serializable
	test_page_latch_aries_no_steal_aries_force_read_uncommitted
	test_page_latch_aries_no_steal_aries_force_read_committed
	test_page_latch_aries_no_steal_aries_force_repeatable_read
	test_page_latch_aries_no_steal_aries_force_serializable
	test_page_latch_aries_no_steal_aries_no_force_read_uncommitted
	test_page_latch_aries_no_steal_aries_no_force_read_committed
	test_page_latch_aries_no_steal_aries_no_force_repeatable_read
	test_page_latch_aries_no_steal_aries_no_force_serializable


test_tree_latch_aries_steal_aries_force_read_uncommitted:
	echo "" > test_tree_latch_aries_steal_aries_force_read_uncommitted.log
	echo "Running tests with features: tree_latch, aries_steal, aries_force, read_uncommitted" | tee -a test_tree_latch_aries_steal_aries_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_force_read_uncommitted.log

test_tree_latch_aries_steal_aries_force_read_committed:
	echo "" > test_tree_latch_aries_steal_aries_force_read_committed.log
	echo "Running tests with features: tree_latch, aries_steal, aries_force, read_committed" | tee -a test_tree_latch_aries_steal_aries_force_read_committed.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_force_read_committed.log

test_tree_latch_aries_steal_aries_force_repeatable_read:
	echo "" > test_tree_latch_aries_steal_aries_force_repeatable_read.log
	echo "Running tests with features: tree_latch, aries_steal, aries_force, repeatable_read" | tee -a test_tree_latch_aries_steal_aries_force_repeatable_read.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_force_repeatable_read.log

test_tree_latch_aries_steal_aries_force_serializable:
	echo "" > test_tree_latch_aries_steal_aries_force_serializable.log
	echo "Running tests with features: tree_latch, aries_steal, aries_force, serializable" | tee -a test_tree_latch_aries_steal_aries_force_serializable.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_force, serializable" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_force_serializable.log

test_tree_latch_aries_steal_aries_no_force_read_uncommitted:
	echo "" > test_tree_latch_aries_steal_aries_no_force_read_uncommitted.log
	echo "Running tests with features: tree_latch, aries_steal, aries_no_force, read_uncommitted" | tee -a test_tree_latch_aries_steal_aries_no_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_no_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_no_force_read_uncommitted.log

test_tree_latch_aries_steal_aries_no_force_read_committed:
	echo "" > test_tree_latch_aries_steal_aries_no_force_read_committed.log
	echo "Running tests with features: tree_latch, aries_steal, aries_no_force, read_committed" | tee -a test_tree_latch_aries_steal_aries_no_force_read_committed.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_no_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_no_force_read_committed.log

test_tree_latch_aries_steal_aries_no_force_repeatable_read:
	echo "" > test_tree_latch_aries_steal_aries_no_force_repeatable_read.log
	echo "Running tests with features: tree_latch, aries_steal, aries_no_force, repeatable_read" | tee -a test_tree_latch_aries_steal_aries_no_force_repeatable_read.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_no_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_no_force_repeatable_read.log

test_tree_latch_aries_steal_aries_no_force_serializable:
	echo "" > test_tree_latch_aries_steal_aries_no_force_serializable.log
	echo "Running tests with features: tree_latch, aries_steal, aries_no_force, serializable" | tee -a test_tree_latch_aries_steal_aries_no_force_serializable.log
	RUST_LOG=info cargo test --features "tree_latch, aries_steal, aries_no_force, serializable" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_steal_aries_no_force_serializable.log

test_tree_latch_aries_no_steal_aries_force_read_uncommitted:
	echo "" > test_tree_latch_aries_no_steal_aries_force_read_uncommitted.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_force, read_uncommitted" | tee -a test_tree_latch_aries_no_steal_aries_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_force_read_uncommitted.log

test_tree_latch_aries_no_steal_aries_force_read_committed:
	echo "" > test_tree_latch_aries_no_steal_aries_force_read_committed.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_force, read_committed" | tee -a test_tree_latch_aries_no_steal_aries_force_read_committed.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_force_read_committed.log

test_tree_latch_aries_no_steal_aries_force_repeatable_read:
	echo "" > test_tree_latch_aries_no_steal_aries_force_repeatable_read.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_force, repeatable_read" | tee -a test_tree_latch_aries_no_steal_aries_force_repeatable_read.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_force_repeatable_read.log

test_tree_latch_aries_no_steal_aries_force_serializable:
	echo "" > test_tree_latch_aries_no_steal_aries_force_serializable.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_force, serializable" | tee -a test_tree_latch_aries_no_steal_aries_force_serializable.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_force, serializable" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_force_serializable.log

test_tree_latch_aries_no_steal_aries_no_force_read_uncommitted:
	echo "" > test_tree_latch_aries_no_steal_aries_no_force_read_uncommitted.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_no_force, read_uncommitted" | tee -a test_tree_latch_aries_no_steal_aries_no_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_no_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_no_force_read_uncommitted.log

test_tree_latch_aries_no_steal_aries_no_force_read_committed:
	echo "" > test_tree_latch_aries_no_steal_aries_no_force_read_committed.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_no_force, read_committed" | tee -a test_tree_latch_aries_no_steal_aries_no_force_read_committed.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_no_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_no_force_read_committed.log

test_tree_latch_aries_no_steal_aries_no_force_repeatable_read:
	echo "" > test_tree_latch_aries_no_steal_aries_no_force_repeatable_read.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_no_force, repeatable_read" | tee -a test_tree_latch_aries_no_steal_aries_no_force_repeatable_read.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_no_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_no_force_repeatable_read.log

test_tree_latch_aries_no_steal_aries_no_force_serializable:
	echo "" > test_tree_latch_aries_no_steal_aries_no_force_serializable.log
	echo "Running tests with features: tree_latch, aries_no_steal, aries_no_force, serializable" | tee -a test_tree_latch_aries_no_steal_aries_no_force_serializable.log
	RUST_LOG=info cargo test --features "tree_latch, aries_no_steal, aries_no_force, serializable" -- --test-threads=1 2>&1 | tee -a test_tree_latch_aries_no_steal_aries_no_force_serializable.log

test_page_latch_aries_steal_aries_force_read_uncommitted:
	echo "" > test_page_latch_aries_steal_aries_force_read_uncommitted.log
	echo "Running tests with features: page_latch, aries_steal, aries_force, read_uncommitted" | tee -a test_page_latch_aries_steal_aries_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_force_read_uncommitted.log

test_page_latch_aries_steal_aries_force_read_committed:
	echo "" > test_page_latch_aries_steal_aries_force_read_committed.log
	echo "Running tests with features: page_latch, aries_steal, aries_force, read_committed" | tee -a test_page_latch_aries_steal_aries_force_read_committed.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_force_read_committed.log

test_page_latch_aries_steal_aries_force_repeatable_read:
	echo "" > test_page_latch_aries_steal_aries_force_repeatable_read.log
	echo "Running tests with features: page_latch, aries_steal, aries_force, repeatable_read" | tee -a test_page_latch_aries_steal_aries_force_repeatable_read.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_force_repeatable_read.log

test_page_latch_aries_steal_aries_force_serializable:
	echo "" > test_page_latch_aries_steal_aries_force_serializable.log
	echo "Running tests with features: page_latch, aries_steal, aries_force, serializable" | tee -a test_page_latch_aries_steal_aries_force_serializable.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_force, serializable" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_force_serializable.log

test_page_latch_aries_steal_aries_no_force_read_uncommitted:
	echo "" > test_page_latch_aries_steal_aries_no_force_read_uncommitted.log
	echo "Running tests with features: page_latch, aries_steal, aries_no_force, read_uncommitted" | tee -a test_page_latch_aries_steal_aries_no_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_no_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_no_force_read_uncommitted.log

test_page_latch_aries_steal_aries_no_force_read_committed:
	echo "" > test_page_latch_aries_steal_aries_no_force_read_committed.log
	echo "Running tests with features: page_latch, aries_steal, aries_no_force, read_committed" | tee -a test_page_latch_aries_steal_aries_no_force_read_committed.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_no_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_no_force_read_committed.log

test_page_latch_aries_steal_aries_no_force_repeatable_read:
	echo "" > test_page_latch_aries_steal_aries_no_force_repeatable_read.log
	echo "Running tests with features: page_latch, aries_steal, aries_no_force, repeatable_read" | tee -a test_page_latch_aries_steal_aries_no_force_repeatable_read.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_no_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_no_force_repeatable_read.log

test_page_latch_aries_steal_aries_no_force_serializable:
	echo "" > test_page_latch_aries_steal_aries_no_force_serializable.log
	echo "Running tests with features: page_latch, aries_steal, aries_no_force, serializable" | tee -a test_page_latch_aries_steal_aries_no_force_serializable.log
	RUST_LOG=info cargo test --features "page_latch, aries_steal, aries_no_force, serializable" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_steal_aries_no_force_serializable.log

test_page_latch_aries_no_steal_aries_force_read_uncommitted:
	echo "" > test_page_latch_aries_no_steal_aries_force_read_uncommitted.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_force, read_uncommitted" | tee -a test_page_latch_aries_no_steal_aries_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_force_read_uncommitted.log

test_page_latch_aries_no_steal_aries_force_read_committed:
	echo "" > test_page_latch_aries_no_steal_aries_force_read_committed.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_force, read_committed" | tee -a test_page_latch_aries_no_steal_aries_force_read_committed.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_force_read_committed.log

test_page_latch_aries_no_steal_aries_force_repeatable_read:
	echo "" > test_page_latch_aries_no_steal_aries_force_repeatable_read.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_force, repeatable_read" | tee -a test_page_latch_aries_no_steal_aries_force_repeatable_read.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_force_repeatable_read.log

test_page_latch_aries_no_steal_aries_force_serializable:
	echo "" > test_page_latch_aries_no_steal_aries_force_serializable.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_force, serializable" | tee -a test_page_latch_aries_no_steal_aries_force_serializable.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_force, serializable" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_force_serializable.log

test_page_latch_aries_no_steal_aries_no_force_read_uncommitted:
	echo "" > test_page_latch_aries_no_steal_aries_no_force_read_uncommitted.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_no_force, read_uncommitted" | tee -a test_page_latch_aries_no_steal_aries_no_force_read_uncommitted.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_no_force, read_uncommitted" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_no_force_read_uncommitted.log

test_page_latch_aries_no_steal_aries_no_force_read_committed:
	echo "" > test_page_latch_aries_no_steal_aries_no_force_read_committed.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_no_force, read_committed" | tee -a test_page_latch_aries_no_steal_aries_no_force_read_committed.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_no_force, read_committed" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_no_force_read_committed.log

test_page_latch_aries_no_steal_aries_no_force_repeatable_read:
	echo "" > test_page_latch_aries_no_steal_aries_no_force_repeatable_read.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_no_force, repeatable_read" | tee -a test_page_latch_aries_no_steal_aries_no_force_repeatable_read.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_no_force, repeatable_read" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_no_force_repeatable_read.log

test_page_latch_aries_no_steal_aries_no_force_serializable:
	echo "" > test_page_latch_aries_no_steal_aries_no_force_serializable.log
	echo "Running tests with features: page_latch, aries_no_steal, aries_no_force, serializable" | tee -a test_page_latch_aries_no_steal_aries_no_force_serializable.log
	RUST_LOG=info cargo test --features "page_latch, aries_no_steal, aries_no_force, serializable" -- --test-threads=1 2>&1 | tee -a test_page_latch_aries_no_steal_aries_no_force_serializable.log

# ===[COMPILATION OPTIONS END]===

# Used when you need more detail.
# 
# The ouput (stdout & stderr) of the test will be redirected to the file "./out" as well.
test-verbose:
	# Run with `RUST_BACKTRACE=1` environment variable to display a backtrace.
	# 
	# The `tee out` will make test always exit with 0.
	# 
	# The `--test-threads=1` instructs there is only one thread is used when
	# running tests. We use this option to avoid the disk file been operated
	# by multiple threads at the same time. Note that this option can be removed
	# once the file used by tests is not conflict with each other.
	# 
	# "--nocapture" instructs the test to print all output to stdout.
	# 
	# `2>&1` is used since the log is printed to stderr.
	# 
	# `tee out` is used to redirect the output to stdout and a file.
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test  -- --test-threads=1 --nocapture 2>&1 | tee out

# Used to run a single test in verbose mode.
# 
# e.g: make test_redistribute_internal_pages
test_%:
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test --features "tree_latch, aries_steal, aries_force" -- --test-threads=1 --nocapture $* 2>&1 | tee out

gen_report:
	source ~/code/python_env_xiaochen/bin/activate
	# python ./scripts/benchmark/benchmark.py
	python ./scripts/benchmark/draw.py

clean:
	rm *.db; \
	rm *.txt; \
	rm -rf target; \
	rm out

fmt:
	cargo fix --allow-dirty --allow-staged

	# unstable features are only available in nightly channel
	# 
	# install nightly-aarch64-apple-darwin:
	# > rustup toolchain install nightly-aarch64-apple-darwin
	# 
	# check the version of rustfmt:
	# > rustup run nightly rustfmt --version
	# rustfmt 1.5.1-nightly (81f39193 2022-10-09)
	# 
	rustup run nightly cargo fmt

pub:
	git push
	cargo login
	cargo publish

clear:
	rm -rf data
