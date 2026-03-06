mod config;
mod storage;

fn main() {
    let cli = config::parse_cli();
    println!("SeaweedFS Volume Server (Rust)");
    println!("Configuration: {:#?}", cli);
}
