use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[cfg(feature = "simd-accel")]
extern crate cc;

const FIELD_SIZE: usize = 256;

const GENERATING_POLYNOMIAL: usize = 29;

fn gen_log_table(polynomial: usize) -> [u8; FIELD_SIZE] {
    let mut result: [u8; FIELD_SIZE] = [0; FIELD_SIZE];
    let mut b: usize = 1;

    for log in 0..FIELD_SIZE - 1 {
        result[b] = log as u8;

        b = b << 1;

        if FIELD_SIZE <= b {
            b = (b - FIELD_SIZE) ^ polynomial;
        }
    }

    result
}

const EXP_TABLE_SIZE: usize = FIELD_SIZE * 2 - 2;

fn gen_exp_table(log_table: &[u8; FIELD_SIZE]) -> [u8; EXP_TABLE_SIZE] {
    let mut result: [u8; EXP_TABLE_SIZE] = [0; EXP_TABLE_SIZE];

    for i in 1..FIELD_SIZE {
        let log = log_table[i] as usize;
        result[log] = i as u8;
        result[log + FIELD_SIZE - 1] = i as u8;
    }

    result
}

fn multiply(log_table: &[u8; FIELD_SIZE], exp_table: &[u8; EXP_TABLE_SIZE], a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        0
    } else {
        let log_a = log_table[a as usize];
        let log_b = log_table[b as usize];
        let log_result = log_a as usize + log_b as usize;
        exp_table[log_result]
    }
}

fn gen_mul_table(
    log_table: &[u8; FIELD_SIZE],
    exp_table: &[u8; EXP_TABLE_SIZE],
) -> [[u8; FIELD_SIZE]; FIELD_SIZE] {
    let mut result: [[u8; FIELD_SIZE]; FIELD_SIZE] = [[0; 256]; 256];

    for a in 0..FIELD_SIZE {
        for b in 0..FIELD_SIZE {
            result[a][b] = multiply(log_table, exp_table, a as u8, b as u8);
        }
    }

    result
}

fn gen_mul_table_half(
    log_table: &[u8; FIELD_SIZE],
    exp_table: &[u8; EXP_TABLE_SIZE],
) -> ([[u8; 16]; FIELD_SIZE], [[u8; 16]; FIELD_SIZE]) {
    let mut low: [[u8; 16]; FIELD_SIZE] = [[0; 16]; FIELD_SIZE];
    let mut high: [[u8; 16]; FIELD_SIZE] = [[0; 16]; FIELD_SIZE];

    for a in 0..low.len() {
        for b in 0..low.len() {
            let mut result = 0;
            if !(a == 0 || b == 0) {
                let log_a = log_table[a];
                let log_b = log_table[b];
                result = exp_table[log_a as usize + log_b as usize];
            }
            if (b & 0x0F) == b {
                low[a][b] = result;
            }
            if (b & 0xF0) == b {
                high[a][b >> 4] = result;
            }
        }
    }
    (low, high)
}

macro_rules! write_table {
    (1D => $file:ident, $table:ident, $name:expr, $type:expr) => {{
        let len = $table.len();
        let mut table_str = String::from(format!("pub static {}: [{}; {}] = [", $name, $type, len));

        for v in $table.iter() {
            let str = format!("{}, ", v);
            table_str.push_str(&str);
        }

        table_str.push_str("];\n");

        $file.write_all(table_str.as_bytes()).unwrap();
    }};
    (2D => $file:ident, $table:ident, $name:expr, $type:expr) => {{
        let rows = $table.len();
        let cols = $table[0].len();
        let mut table_str = String::from(format!(
            "pub static {}: [[{}; {}]; {}] = [",
            $name, $type, cols, rows
        ));

        for a in $table.iter() {
            table_str.push_str("[");
            for b in a.iter() {
                let str = format!("{}, ", b);
                table_str.push_str(&str);
            }
            table_str.push_str("],\n");
        }

        table_str.push_str("];\n");

        $file.write_all(table_str.as_bytes()).unwrap();
    }};
}

fn write_tables() {
    let log_table = gen_log_table(GENERATING_POLYNOMIAL);
    let exp_table = gen_exp_table(&log_table);
    let mul_table = gen_mul_table(&log_table, &exp_table);

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("table.rs");
    let mut f = File::create(&dest_path).unwrap();

    write_table!(1D => f, log_table,      "LOG_TABLE",      "u8");
    write_table!(1D => f, exp_table,      "EXP_TABLE",      "u8");
    write_table!(2D => f, mul_table,      "MUL_TABLE",      "u8");

    if cfg!(feature = "simd-accel") {
        let (mul_table_low, mul_table_high) = gen_mul_table_half(&log_table, &exp_table);

        write_table!(2D => f, mul_table_low,  "MUL_TABLE_LOW",  "u8");
        write_table!(2D => f, mul_table_high, "MUL_TABLE_HIGH", "u8");
    }
}

#[cfg(all(
    feature = "simd-accel",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_env = "msvc"),
    not(any(target_os = "android", target_os = "ios"))
))]
fn compile_simd_c() {
    let mut build = cc::Build::new();
    build.opt_level(3);

    match env::var("RUST_REED_SOLOMON_ERASURE_ARCH") {
        Ok(arch) => {
            // Use explicitly specified environment variable as architecture.
            build.flag(&format!("-march={}", arch));
        }
        Err(_error) => {
            // On x86-64 enabling Haswell architecture unlocks useful instructions and improves performance
            // dramatically while allowing it to run ony modern CPU.
            match env::var("CARGO_CFG_TARGET_ARCH").unwrap().as_str(){
                "x86_64"  => { build.flag(&"-march=haswell"); },
                _         => ()
            }
        }
    }

    build
        .flag("-std=c11")
        .file("simd_c/reedsolomon.c")
        .compile("reedsolomon");
}

#[cfg(not(all(
    feature = "simd-accel",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_env = "msvc"),
    not(any(target_os = "android", target_os = "ios"))
)))]
fn compile_simd_c() {}

fn main() {
    compile_simd_c();
    write_tables();
}
