if [ "$1" = "clean" ]; then
    cargo clean
else
    cargo check --all --all-features && \
    cargo clippy --workspace --all-features -- -D warnings && \
    cargo build
fi
