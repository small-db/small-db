test:
	# run with `RUST_BACKTRACE=1` environment variable to display a backtrace
	RUST_LOG=debug cargo test -- --test-threads=1 2>&1 | tee out

clean:
	rm *.db
	rm *.txt

fmt:
	cargo fmt
	sed -i -E 's|(//)\s*(\S)|\1 \2|' **/*.rs

