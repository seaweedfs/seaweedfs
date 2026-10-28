fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        // SAFETY: a build script's main runs single-threaded before anything
        // else in this process, so no other thread can be reading the
        // environment concurrently.
        unsafe {
            std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
        }
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../../weed/pb/vector.proto",
                "../../../weed/pb/master.proto",
                "../../../weed/pb/volume_server.proto",
                "../../../weed/pb/remote.proto",
            ],
            &["../../../weed/pb/"],
        )?;
    println!("cargo:rerun-if-changed=../../../weed/pb/vector.proto");
    Ok(())
}
