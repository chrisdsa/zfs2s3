set quiet

build-devcontainer tag="latest":
    docker buildx build -t zfs2s3:{{tag}} -f .devcontainer/Dockerfile .

devcontainer datadir="/tmp/zfs2s3":
    docker run -it --rm \
        --privileged \
        -v /dev:/dev \
        -v {{justfile_directory()}}:/workspace \
        -v {{datadir}}:{{datadir}} \
        -v ZFS2S3_RUST_VOLUME:/usr/local/cargo \
        --network host \
        -w /workspace \
        -e DATA_DIR={{datadir}} \
        zfs2s3:latest \
        bash


### Commands for development inside the devcontainer ###

build profile="release":
    cargo build --profile {{profile}}

test:
    cargo test --all -- --nocapture

test-integration:
    ./tests/integration-tests

lint profile="release":
    cargo clippy --all --profile {{profile}} -- -D warnings
    cargo fmt --all -- --check
