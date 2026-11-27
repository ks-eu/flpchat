mod database;
mod connection;
mod messages;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let pool = database::create_pool().await?;

    println!("Connected to PostgreSQL successfully.");

     
    Ok(())
}

