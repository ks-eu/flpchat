mod database;
mod users;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let pool = database::create_pool().await?;

    println!("Connected to PostgreSQL successfully.");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await?;

    println!("Users migration completed successfully.");

    Ok(())
}

