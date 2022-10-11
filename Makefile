# Used when you don't need too much detail.
test:
	RUST_LOG=info cargo test -- --test-threads=1

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
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test -- --test-threads=1 --nocapture 2>&1 | tee out

# Used to run a single test in verbose mode.
# 
# e.g: make test_redistribute_internal_pages
test_%:
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test -- --test-threads=1 --nocapture $* 2>&1 | tee out

clean:
	rm *.db; \
	rm *.txt; \
	rm -rf target; \
	rm out

fmt:
	# unstable features are only available in nightly channel
	# 
	# > rustup run nightly rustfmt --version
	# rustfmt 1.5.1-nightly (81f39193 2022-10-09)
	# 
	rustup run nightly cargo fmt

pub:
	git commit -v -a -m "update version and publish cargo"
	git push
	cargo login
	cargo publish