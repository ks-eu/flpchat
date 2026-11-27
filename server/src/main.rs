mod database;
mod connection;
mod orchestration;
mod onboarding;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let pool = database::create_pool().await?;

    println!("Connected to PostgreSQL successfully.");

     
    Ok(())
}

