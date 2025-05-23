run:
	# This command will start the "small-db" server at localhost:5433.
	# 
	# Connect to the server with
	# `psql -h localhost -p 5433 -d default_db -U xiaochen`
	# 
	# We use "info" log level since there are lots debug logs from the dependencies.
	RUST_LOG=info cargo run

gen:
	# pip install pyyaml
	python scripts/compilation_options/gen.py

test:
	# Note: don't use fancy pipeline and redirection operators in the makefile, because
	# they don't work on github acitons.
	# 
	# Github actions will use "sh" for the makefile, which doesn't support bash options.
	RUST_LOG=info cargo test -- --test-threads=1

# Used when you need more detail.
# 
# The ouput (stdout & stderr) of the test will be redirected to the file "./out" as well.
test_verbose:
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
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test -- --test-threads=1 --nocapture 2>&1 | tee out

# Used to run a single test in verbose mode.
# 
# e.g: make test_redistribute_internal_pages
# 
# options:
test_%:
	# --no-capture is used to print the log to stdout.
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test --features "benchmark" -- --test-threads=1 --nocapture test_$* 2>&1 | tee out

debug:
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test -- --test-threads=1 integretions::concurrent_test::test_concurrent --exact 2>&1 | tee out

benchmark:
	./scripts/benchmark/run.sh

clean:
	rm *.db; \
	rm *.txt; \
	rm -rf target; \
	rm out

fmt:
	# "--allow-dirty" and "--allow-staged" makes "cargo fix" doesn't care about the
	# status of the git repository.
	#
	# "--all-features" makes "cargo fix" treat all features as enabled, so that all
	# the code will be formatted.
	cargo fix --allow-dirty --allow-staged --all-features

	# unstable features are only available in nightly channel
	# 
	# install nightly-aarch64-apple-darwin:
	# > rustup toolchain install nightly-x86_64-unknown-linux-gnu
	# 
	# check the version of rustfmt:
	# > rustup run nightly rustfmt --version
	# rustfmt 1.7.1-nightly (636d7ff 2024-08-19)
	# 
	rustup run nightly cargo fmt

pub:
	git push
	cargo login
	cargo publish

clear:
	rm -rf data

# Standard test. Doesn't print debug logs.
# 
# The ouput (stdout & stderr) of the test will be redirected to the file "*.log".
# ===[COMPILATION OPTIONS START]===
test_all_modes:
	test_aries_steal_aries_force_read_uncommitted
	test_aries_steal_aries_force_read_committed
	test_aries_steal_aries_force_repeatable_read
	test_aries_steal_aries_force_serializable
	test_aries_steal_aries_no_force_read_uncommitted
	test_aries_steal_aries_no_force_read_committed
	test_aries_steal_aries_no_force_repeatable_read
	test_aries_steal_aries_no_force_serializable
	test_aries_no_steal_aries_force_read_uncommitted
	test_aries_no_steal_aries_force_read_committed
	test_aries_no_steal_aries_force_repeatable_read
	test_aries_no_steal_aries_force_serializable
	test_aries_no_steal_aries_no_force_read_uncommitted
	test_aries_no_steal_aries_no_force_read_committed
	test_aries_no_steal_aries_no_force_repeatable_read
	test_aries_no_steal_aries_no_force_serializable


test_aries_steal_aries_force_read_uncommitted:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_force, read_uncommitted" --no-default-features -- --test-threads=1 --nocapture

test_aries_steal_aries_force_read_committed:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_force, read_committed" --no-default-features -- --test-threads=1 --nocapture

test_aries_steal_aries_force_repeatable_read:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_force, repeatable_read" --no-default-features -- --test-threads=1 --nocapture

test_aries_steal_aries_force_serializable:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_force, serializable" --no-default-features -- --test-threads=1 --nocapture

test_aries_steal_aries_no_force_read_uncommitted:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_no_force, read_uncommitted" --no-default-features -- --test-threads=1 --nocapture

test_aries_steal_aries_no_force_read_committed:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_no_force, read_committed" --no-default-features -- --test-threads=1 --nocapture

test_aries_steal_aries_no_force_repeatable_read:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_no_force, repeatable_read" --no-default-features -- --test-threads=1 --nocapture

test_aries_steal_aries_no_force_serializable:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_steal, aries_no_force, serializable" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_force_read_uncommitted:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_force, read_uncommitted" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_force_read_committed:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_force, read_committed" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_force_repeatable_read:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_force, repeatable_read" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_force_serializable:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_force, serializable" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_no_force_read_uncommitted:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_no_force, read_uncommitted" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_no_force_read_committed:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_no_force, read_committed" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_no_force_repeatable_read:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_no_force, repeatable_read" --no-default-features -- --test-threads=1 --nocapture

test_aries_no_steal_aries_no_force_serializable:
	# "--no-default-features" is used to disable default features
	# "--test-threads=1" is used to run tests in serial
	# "--no-capture" is used to print the output to stdout
	RUST_LOG=info cargo test --features "aries_no_steal, aries_no_force, serializable" --no-default-features -- --test-threads=1 --nocapture

# ===[COMPILATION OPTIONS END]===



