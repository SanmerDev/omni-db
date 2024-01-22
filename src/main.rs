use std::{env, fs};

use clap::Parser;
use reqwest::Url;
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
        /// Path (or url) of file
        #[arg(short, long)]
        path: String,
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

struct App {
    pool: PgPool,
}

impl App {
    async fn new(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(&url).await?;
        let app = App { pool };

        Ok(app)
    }

    async fn create(&self, table: &str) -> Result<(), sqlx::Error> {
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
            table
        );

        sqlx::query(&sql).execute(&self.pool).await?;

        Ok(())
    }

    async fn query(&self, sql: &str) -> anyhow::Result<()> {
        let recs: Vec<Data> = sqlx::query_as(sql).fetch_all(&self.pool).await?;
        let json = serde_json::to_string_pretty(&recs)?;

        if !recs.is_empty() {
            println!("{json}");
        }

        Ok(())
    }

    async fn add(&self, table: &str, path: &str) -> anyhow::Result<()> {
        let is_url = Url::parse(path).is_ok();
        let content = if is_url {
            reqwest::get(path).await?.text().await?
        } else {
            fs::read_to_string(path)?
        };

        self.create(table).await?;
        for line in content.lines() {
            let mut values: Vec<&str> = line.split_whitespace().collect();

            let _year = values.remove(0);
            let fields = values.join(", ");

            let sql = format!(
                "INSERT INTO {} VALUES ({}) ON CONFLICT (day, hour, minute) DO NOTHING",
                table, fields
            );

            sqlx::query(&sql).execute(&self.pool).await?;
            println!("Added: [{}, {}, {}]", values[0], values[1], values[2]);
        }

        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let url = env::var("DATABASE_URL").expect("Env var DATABASE_URL is required");
    let app = App::new(&url).await?;

    let cli = Args::parse();
    match cli.command {
        Commands::Create { table } => app.create(&table).await?,
        Commands::Query { sql } => app.query(&sql).await?,
        Commands::Add { table, path } => app.add(&table, &path).await?,
    };

    Ok(())
}
