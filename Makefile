test:
	# run with `RUST_BACKTRACE=1` environment variable to display a backtrace
	RUST_LOG=info RUST_BACKTRACE=1 cargo test -- --test-threads=1 2>&1 | tee out

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
