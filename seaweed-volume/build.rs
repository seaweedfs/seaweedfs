fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
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
