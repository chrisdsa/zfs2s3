use std::cmp::min;
use std::env;

fn main() {
    const SHA_SIZE: usize = 7;

    // If the environment variable is set from CI
    let sha;
    if let Ok(git_sha) = env::var("GIT_SHA") {
        sha = git_sha;
    } else {
        sha = "dev".to_string();
    }

    // Set the environment variable
    let end = min(SHA_SIZE, sha.len());
    println!("cargo:rustc-env=GIT_SHA={}", &sha.trim()[..end]);
}
