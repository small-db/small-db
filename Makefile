test:
	# run with `RUST_BACKTRACE=1` environment variable to display a backtrace
	RUST_LOG=debug RUST_TEST_TASKS=1 cargo test 2>&1 | tee out

clean:
	rm *.db
	rm *.txt

fmt:
	cargo fmt
	sed -i -E 's|(//)\s*(\S)|\1 \2|' **/*.rs

