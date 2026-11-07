fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the protoc that ships with protoc-bin-vendored rather than a system
    // one, so the build needs no package manager and always sees the same
    // version. An explicit PROTOC still wins, for packagers supplying their own
    // and for the lance crates, whose own build scripts read the same variable.
    if std::env::var_os("PROTOC").is_none() {
        // SAFETY: a build script's main runs single-threaded before anything
        // else in this process, so no other thread can be reading the
        // environment concurrently.
        unsafe {
            std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
        }
    }
    // The master's ListClusterNodes, for the filers; master.proto brings the
    // two it imports.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                "../../../weed/pb/master.proto",
                "../../../weed/pb/volume_server.proto",
                "../../../weed/pb/remote.proto",
            ],
            &["../../../weed/pb/"],
        )?;
    println!("cargo:rerun-if-changed=../../../weed/pb/master.proto");
    Ok(())
}
