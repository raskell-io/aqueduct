//! Compiles `proto/aqueduct/v1` with protox, a protobuf compiler in Rust: building needs no `protoc`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files = ["aqueduct/v1/ingress.proto", "aqueduct/v1/worker.proto"];
    for file in files {
        println!("cargo:rerun-if-changed={}", root.join(file).display());
    }
    let descriptors = protox::compile(files, [&root])?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(descriptors)?;
    Ok(())
}
