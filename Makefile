test:
	# run with `RUST_BACKTRACE=1` environment variable to display a backtrace
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test

clean:
	rm *.db
	rm *.txt

fmt:
	cargo fmt
	sed -i -E 's|(//)\s*(\S)|\1 \2|' **/*.rs

