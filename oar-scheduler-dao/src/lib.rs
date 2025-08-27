use dotenvy::dotenv;
use rand::Rng;
use sea_query::{Alias, ExprTrait, Iden, PostgresQueryBuilder, Query};
use sea_query_sqlx::SqlxBinder;
use sqlx::postgres::PgPoolOptions;
use sqlx::Execute;
use std::env;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        sqlx_sea_query_example().await
    }
}

#[derive(Iden)]
pub enum Users {
    Table,
    Id,
    Name,
}

pub async fn sqlx_sea_query_example() {
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file or environment");
    let pool = PgPoolOptions::new().max_connections(5).connect(db_url.as_str()).await.unwrap();

    let column_name: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(7)
        .map(char::from)
        .collect::<String>()
        .to_ascii_lowercase();
    println!("Generated column name: {}", column_name);

    // create table if not exists
    sqlx::query("CREATE TABLE IF NOT EXISTS users (id serial PRIMARY KEY, name varchar NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let sql = format!(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS \"{}\" varchar DEFAULT '{}'",
        column_name, "value"
    );
    sqlx::query(sql.as_str()).execute(&pool).await.unwrap();

    let tx = pool.begin().await.unwrap();

    let (sql, values) = Query::insert()
        .into_table(Users::Table)
        .columns([Alias::new(Users::Name.to_string()), Alias::new(&column_name)])
        .values_panic(vec!["test".into(), column_name.clone().into()])
        .build_sqlx(PostgresQueryBuilder);
    println!("Insert SQL: {}", sql);
    sqlx::query_with(sql.as_str(), values).execute(&pool).await.unwrap();

    tx.commit().await.unwrap();

    let (sql, values) = Query::select()
        .column(Users::Id)
        .column(Users::Name)
        .column(Alias::new(&column_name))
        .from(Users::Table)
        .and_where(sea_query::Expr::col(Alias::new(&column_name)).eq(&column_name))
        .build_sqlx(PostgresQueryBuilder);

    let row: (i32, String, String) = sqlx::query_as_with(sql.as_str(), values).fetch_one(&pool).await.unwrap();

    assert_eq!(row.1, "test".to_string());
    assert_eq!(row.2, column_name);
}
