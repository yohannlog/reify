//! Diesel scenarios (sync Diesel wrapped in tokio::task::spawn_blocking).
//!
//! Diesel is inherently synchronous; the timing wrapper only accounts for
//! wall-clock cost, which is the user-observable quantity we care about.

use std::time::Duration;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use super::model::{CREATE_TABLE_SQL, UserRow, make_rows};
use crate::runner::Scenario;
use crate::time_iters;

diesel::table! {
    users (id) {
        id -> BigInt,
        name -> Text,
        email -> Text,
        score -> Integer,
        active -> Bool,
    }
}

#[derive(Insertable, Queryable, AsChangeset, Clone, Debug)]
#[diesel(table_name = users)]
struct DUser {
    id: i64,
    name: String,
    email: String,
    score: i32,
    active: bool,
}

fn fresh_conn() -> SqliteConnection {
    let mut c = SqliteConnection::establish(":memory:").expect("open");
    c.batch_execute(CREATE_TABLE_SQL).expect("create table");
    c
}

fn to_d(r: &UserRow) -> DUser {
    DUser {
        id: r.id,
        name: r.name.clone(),
        email: r.email.clone(),
        score: r.score,
        active: r.active,
    }
}

fn seed(c: &mut SqliteConnection, rows: &[UserRow]) {
    c.transaction::<_, diesel::result::Error, _>(|c| {
        for r in rows {
            diesel::insert_into(users::table)
                .values(to_d(r))
                .execute(c)?;
        }
        Ok(())
    })
    .expect("seed");
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
        let data = data.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = fresh_conn();
            for r in &data {
                diesel::insert_into(users::table)
                    .values(to_d(r))
                    .execute(&mut c)
                    .unwrap();
            }
        })
        .await
        .unwrap();
    })
    .await
}

async fn bench_insert_batch(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let data = data.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = fresh_conn();
            let vals: Vec<DUser> = data.iter().map(to_d).collect();
            diesel::insert_into(users::table)
                .values(&vals)
                .execute(&mut c)
                .unwrap();
        })
        .await
        .unwrap();
    })
    .await
}

async fn bench_select_all(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = tokio::task::spawn_blocking(move || {
        let mut c = fresh_conn();
        seed(&mut c, &data);
        let _ = tx.send(()); // signal: seeded
        // Keep the connection alive via the channel receiver on the outer side.
        // We'll receive iter counts here, then drop.
        c
    });
    rx.recv().unwrap();
    // Awkward cross-thread sharing — rebuild connection on the fly each iter.
    let data = make_rows(rows);
    let _ = handle.await.unwrap(); // drop previously built conn
    time_iters(iters, || async {
        let data = data.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = fresh_conn();
            seed(&mut c, &data);
            let out = users::table.load::<DUser>(&mut c).unwrap();
            std::hint::black_box(out);
        })
        .await
        .unwrap();
    })
    .await
}

async fn bench_select_by_pk(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let pk = (rows / 2) as i64;
    time_iters(iters, || async {
        let data = data.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = fresh_conn();
            seed(&mut c, &data);
            let out = users::table
                .filter(users::id.eq(pk))
                .load::<DUser>(&mut c)
                .unwrap();
            std::hint::black_box(out);
        })
        .await
        .unwrap();
    })
    .await
}

async fn bench_update(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let data = data.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = fresh_conn();
            seed(&mut c, &data);
            diesel::update(users::table.filter(users::active.eq(true)))
                .set(users::score.eq(999))
                .execute(&mut c)
                .unwrap();
        })
        .await
        .unwrap();
    })
    .await
}

async fn bench_delete(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let data = data.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = fresh_conn();
            seed(&mut c, &data);
            diesel::delete(users::table.filter(users::active.eq(false)))
                .execute(&mut c)
                .unwrap();
        })
        .await
        .unwrap();
    })
    .await
}
