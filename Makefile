test:
	RUST_LOG=debug RUST_BACKTRACE=1 cargo test

clean:
	rm *.db
	rm *.txt
