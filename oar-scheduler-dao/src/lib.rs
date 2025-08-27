use dotenvy::dotenv;
use rand::Rng;
use sqlx::postgres::PgPoolOptions;
use std::env;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        dbtest().await.unwrap();
    }
}

pub async fn dbtest() -> Result<(), sqlx::Error> {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file or environment");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url.as_str()).await?;

    let column_name: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();
    println!("Generated column name: {}", column_name);

    // create table if not exists
    sqlx::query("CREATE TABLE IF NOT EXISTS users (id serial PRIMARY KEY, name varchar NOT NULL)").execute(&pool).await?;

    sqlx::query(format!("ALTER TABLE users ADD COLUMN IF NOT EXISTS {} varchar DEFAULT {}", column_name, "'value'").as_str())
        .execute(&pool).await?;


    sqlx::query(format!("INSERT INTO users (name, {}) VALUES ($1, '{}')", column_name, column_name).as_str())
        .bind("test")
        .execute(&pool).await?;

    let row: (i32, String, String) = sqlx::query_as(format!("SELECT id, name, {} FROM users WHERE {} = $1", column_name, column_name).as_str())
        .bind(column_name.as_str())
        .fetch_one(&pool).await?;

    assert_eq!(row.1, "test".to_string());
    assert_eq!(row.2, column_name);

    Ok(())
}
