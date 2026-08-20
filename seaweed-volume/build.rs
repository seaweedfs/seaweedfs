fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the protoc that ships with protoc-bin-vendored rather than a system
    // one, so the build needs no package manager and always sees the same
    // version. An explicit PROTOC still wins, for packagers supplying their own.
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // filer.proto uses proto3 optional, which protoc rejects without this
        // flag before 3.15. The vendored protoc is newer, but it still accepts
        // the flag, so this keeps a build against an older PROTOC working.
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("seaweed_descriptor.bin"))
        .compile_protos(
            &[
                "proto/volume_server.proto",
                "proto/master.proto",
                "proto/remote.proto",
                "../weed/pb/filer.proto",
            ],
            &["proto/", "../weed/pb/"],
        )?;
    Ok(())
}
