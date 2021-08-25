test:
	# Run with `RUST_BACKTRACE=1` environment variable to display a backtrace.
	# 
	# The `tee out` will make test always exit with 0.
	# 
	# The `--test-threads=1` instructs there is only one thread is used when
	# running tests. We use this option to avoid the disk file been operated
	# by multiple threads at the same time. Note that this option can be removed
	# once the file used by tests is not conflict with each other.
	# 
	# `2>&1` is used since the log is printed to stderr.
	RUST_LOG=info RUST_BACKTRACE=1 cargo test -- --test-threads=1 2>&1

test-insert:
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test --test btree_insert_test -- --test-threads=1 2>&1 | tee out

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
