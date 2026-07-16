fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // filer.proto uses proto3 optional; older protoc (e.g. 3.12 from ubuntu-22.04 apt)
        // rejects it without this flag, and newer protoc still accepts the flag
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
