fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compiled straight out of the Go tree, the way seaweed-volume already reads
    // filer.proto, so the contract cannot drift from a vendored copy.
    tonic_build::configure()
        // The server half is only for tests, which stand up a fake admin.
        .build_server(true)
        .build_client(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["../../../weed/pb/plugin.proto"], &["../../../weed/pb/"])?;
    println!("cargo:rerun-if-changed=../../../weed/pb/plugin.proto");
    Ok(())
}
