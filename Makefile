test:
	RUST_LOG=debug cargo test

clean:
	rm *.db
	rm *.txt

fmt:
	cargo fmt
	sed -i -E 's|(//)\s*(\S)|\1 \2|' **/*.rs

