//! sqlx (raw) scenarios — no ORM, just `sqlx::query`.

use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use super::model::{CREATE_TABLE_SQL, UserRow, make_rows};
use crate::runner::Scenario;
use crate::time_iters;

async fn fresh_pool() -> SqlitePool {
    let opts = SqliteConnectOptions::new()
        .in_memory(true)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .expect("connect");
    sqlx::query(CREATE_TABLE_SQL).execute(&pool).await.unwrap();
    pool
}

async fn seed(pool: &SqlitePool, rows: &[UserRow]) {
    let mut tx = pool.begin().await.unwrap();
    for r in rows {
        sqlx::query("INSERT INTO users (id, name, email, score, active) VALUES (?, ?, ?, ?, ?)")
            .bind(r.id)
            .bind(&r.name)
            .bind(&r.email)
            .bind(r.score)
            .bind(r.active)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}

pub async fn run(scn: Scenario, rows: usize, iters: usize) -> Duration {
    match scn {
        Scenario::Insert => bench_insert(rows, iters).await,
        Scenario::InsertBatch => bench_insert_batch(rows, iters).await,
        Scenario::SelectAll => bench_select_all(rows, iters).await,
        Scenario::SelectByPk => bench_select_by_pk(rows, iters).await,
        Scenario::Update => bench_update(rows, iters).await,
        Scenario::Delete => bench_delete(rows, iters).await,
    }
}

async fn bench_insert(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let pool = fresh_pool().await;
        for r in &data {
            sqlx::query(
                "INSERT INTO users (id, name, email, score, active) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(r.id)
            .bind(&r.name)
            .bind(&r.email)
            .bind(r.score)
            .bind(r.active)
            .execute(&pool)
            .await
            .unwrap();
        }
    })
    .await
}

async fn bench_insert_batch(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let pool = fresh_pool().await;
        let mut sql = String::from("INSERT INTO users (id, name, email, score, active) VALUES ");
        for i in 0..data.len() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str("(?, ?, ?, ?, ?)");
        }
        let mut q = sqlx::query(&sql);
        for r in &data {
            q = q.bind(r.id).bind(&r.name).bind(&r.email).bind(r.score).bind(r.active);
        }
        q.execute(&pool).await.unwrap();
    })
    .await
}

async fn bench_select_all(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let pool = fresh_pool().await;
    seed(&pool, &data).await;
    time_iters(iters, || async {
        let rows = sqlx::query("SELECT id, name, email, score, active FROM users")
            .fetch_all(&pool)
            .await
            .unwrap();
        let out: Vec<UserRow> = rows
            .into_iter()
            .map(|row| UserRow {
                id: row.get(0),
                name: row.get(1),
                email: row.get(2),
                score: row.get(3),
                active: row.get(4),
            })
            .collect();
        std::hint::black_box(out);
    })
    .await
}

async fn bench_select_by_pk(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let pool = fresh_pool().await;
    seed(&pool, &data).await;
    let pk = (rows / 2) as i64;
    time_iters(iters, || async {
        let rows = sqlx::query("SELECT id, name, email, score, active FROM users WHERE id = ?")
            .bind(pk)
            .fetch_all(&pool)
            .await
            .unwrap();
        let out: Vec<UserRow> = rows
            .into_iter()
            .map(|row| UserRow {
                id: row.get(0),
                name: row.get(1),
                email: row.get(2),
                score: row.get(3),
                active: row.get(4),
            })
            .collect();
        std::hint::black_box(out);
    })
    .await
}

async fn bench_update(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let pool = fresh_pool().await;
        seed(&pool, &data).await;
        sqlx::query("UPDATE users SET score = ? WHERE active = ?")
            .bind(999_i32)
            .bind(true)
            .execute(&pool)
            .await
            .unwrap();
    })
    .await
}

async fn bench_delete(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let pool = fresh_pool().await;
        seed(&pool, &data).await;
        sqlx::query("DELETE FROM users WHERE active = ?")
            .bind(false)
            .execute(&pool)
            .await
            .unwrap();
    })
    .await
}
