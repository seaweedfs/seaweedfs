fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the protoc that ships with protoc-bin-vendored rather than a system
    // one, so the build needs no package manager and always sees the same
    // version. An explicit PROTOC still wins, for packagers supplying their own
    // and for the lance crates, whose own build scripts read the same variable.
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

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
