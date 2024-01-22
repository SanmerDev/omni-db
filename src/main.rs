use std::{env, fs};

use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::FromRow;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Create a new table
    Create {
        /// Name of table
        table: String,
    },

    /// Query for database
    Query {
        /// Command of sql
        sql: String,
    },

    /// Add data to database
    Add {
        /// Name of table
        #[arg(short, long)]
        table: String,
        /// Path of file
        #[arg(short, long)]
        file: String,
    },
}

#[derive(FromRow, Deserialize, Serialize, Debug)]
struct Data {
    day: i32,
    hour: i32,
    minute: i32,
    imf_sc_id: i32,
    plasma_sc_id: i32,
    imf_sc_point: i32,
    plasma_sc_point: i32,
    interp_percent: i32,
    timeshift: i32,
    timeshift_rms: i32,
    phase_rms: f32,
    obs_time: i32,
    b_avg: f32,
    bx: f32,
    gse_by: f32,
    gse_bz: f32,
    gsm_by: f32,
    gsm_bz: f32,
    scalar_rms: f32,
    vector_rms: f32,
    flow_speed: f32,
    gse_vx: f32,
    gse_vy: f32,
    gse_vz: f32,
    proton_density: f32,
    temperature: f32,
    flow_pressure: f32,
    e: f32,
    plasma_beta: f32,
    alfven_mach_num: f32,
    px_sc: f32,
    py_sc: f32,
    pz_sc: f32,
    px_bsn: f32,
    py_bsn: f32,
    pz_bsn: f32,
    ae: i32,
    al: i32,
    au: i32,
    symd: i32,
    symh: i32,
    aysd: i32,
    asyh: i32,
    nanp_or_pc: f32,
    mag_mach_num: f32,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;

    let cli = Args::parse();
    match cli.command {
        Commands::Create { table } => create_not_exists(&pool, &table).await?,
        Commands::Query { sql } => query(&pool, &sql).await?,
        Commands::Add { table, file } => add(&pool, &table, &file).await?,
    };

    Ok(())
}

async fn create_not_exists(pool: &PgPool, table_name: &String) -> anyhow::Result<()> {
    let sql = format!(
        r#"
CREATE TABLE IF NOT EXISTS {} (
    day INT,
    hour INT,
    minute INT,
    imf_sc_id INT,
    plasma_sc_id INT,
    imf_sc_point INT,
    plasma_sc_point INT,
    interp_percent INT,
    timeshift INT,
    timeshift_rms INT,
    phase_rms REAL,
    obs_time INT,
    b_avg REAL,
    bx REAL,
    gse_by REAL,
    gse_bz REAL,
    gsm_by REAL,
    gsm_bz REAL,
    scalar_rms REAL,
    vector_rms REAL,
    flow_speed REAL,
    gse_vx REAL,
    gse_vy REAL,
    gse_vz REAL,
    proton_density REAL,
    temperature REAL,
    flow_pressure REAL,
    e REAL,
    plasma_beta REAL,
    alfven_mach_num REAL,
    px_sc REAL,
    py_sc REAL,
    pz_sc REAL,
    px_bsn REAL,
    py_bsn REAL,
    pz_bsn REAL,
    ae INT,
    al INT,
    au INT,
    symd INT,
    symh INT,
    aysd INT,
    asyh INT,
    nanp_or_pc REAL,
    mag_mach_num REAL,
    PRIMARY KEY (day, hour, minute)
)
        "#,
        table_name
    );

    let _rec = sqlx::query(sql.as_str()).execute(pool).await?;

    Ok(())
}

async fn query(pool: &PgPool, sql: &String) -> anyhow::Result<()> {
    let recs: Vec<Data> = sqlx::query_as(sql.as_str()).fetch_all(pool).await?;
    let json = serde_json::to_string_pretty(&recs)?;

    if !recs.is_empty() {
        println!("{json}");
    }

    Ok(())
}

async fn add(pool: &PgPool, table_name: &String, file_path: &String) -> anyhow::Result<()> {
    create_not_exists(pool, table_name).await?;

    let texts = fs::read_to_string(file_path)?;
    for line in texts.lines() {
        let mut values: Vec<&str> = line.split_whitespace().collect();

        let _year = values.remove(0);
        let fields_str = values.join(", ");

        let sql = format!(
            "INSERT INTO {} VALUES ({}) ON CONFLICT (day, hour, minute) DO NOTHING",
            table_name, fields_str
        );

        let _rec = sqlx::query(sql.as_str()).execute(pool).await?;
        println!("Added: [{}, {}, {}]", values[0], values[1], values[2]);
    }

    Ok(())
}
