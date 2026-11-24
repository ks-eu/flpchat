mod database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let pool = database::create_pool().await?;

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await?;

    println!("Connected to PostgreSQL successfully.");

    Ok(())
}
