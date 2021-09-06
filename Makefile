test:
	RUST_LOG=error cargo test -- --test-threads=1

	# run ignored tests
	# https://doc.rust-lang.org/book/ch11-02-running-tests.html#ignoring-some-tests-unless-specifically-requested
	RUST_LOG=error cargo test -- --ignored --test-threads=1

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
	# "--exact" instructs the test to print the exact output of the test.
	# 
	# `2>&1` is used since the log is printed to stderr.
	# 
	# `tee out` is used to redirect the output to stdout and a file.
	RUST_LOG=info RUST_BACKTRACE=1 cargo test -- --test-threads=1 --nocapture --exact 2>&1 | tee out

clean:
	rm *.db; \
	rm *.txt; \
	rm -rf target; \
	rm out

fmt:
	rustup run nightly cargo fmt

pub:
	git commit -v -a -m "update version and publish cargo"
	git push
	cargo login
	cargo publish