//! Raw rusqlite baseline — no ORM, handwritten SQL, prepared statements.

use std::time::Duration;

use rusqlite::{Connection, params};

use super::model::{CREATE_TABLE_SQL, UserRow, make_rows};
use crate::runner::Scenario;
use crate::time_iters;

fn fresh_conn() -> Connection {
    let c = Connection::open_in_memory().expect("open");
    c.execute_batch(CREATE_TABLE_SQL).expect("create table");
    c
}

fn seed(c: &Connection, rows: &[UserRow]) {
    let tx = c.unchecked_transaction().expect("begin");
    {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO users (id, name, email, score, active) VALUES (?, ?, ?, ?, ?)",
            )
            .expect("prepare");
        for r in rows {
            stmt.execute(params![r.id, r.name, r.email, r.score, r.active as i64])
                .expect("insert");
        }
    }
    tx.commit().expect("commit");
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
        let c = fresh_conn();
        let mut stmt = c
            .prepare("INSERT INTO users (id, name, email, score, active) VALUES (?, ?, ?, ?, ?)")
            .unwrap();
        for r in &data {
            stmt.execute(params![r.id, r.name, r.email, r.score, r.active as i64])
                .unwrap();
        }
    })
    .await
}

async fn bench_insert_batch(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let c = fresh_conn();
        // Single multi-row INSERT — fair comparison against ORM batch builders.
        let mut sql = String::from("INSERT INTO users (id, name, email, score, active) VALUES ");
        let mut pvals: Vec<rusqlite::types::Value> = Vec::with_capacity(rows * 5);
        for (i, r) in data.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str("(?, ?, ?, ?, ?)");
            pvals.push(r.id.into());
            pvals.push(r.name.clone().into());
            pvals.push(r.email.clone().into());
            pvals.push(r.score.into());
            pvals.push((r.active as i64).into());
        }
        c.execute(&sql, rusqlite::params_from_iter(pvals.iter()))
            .unwrap();
    })
    .await
}

async fn bench_select_all(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let c = fresh_conn();
    seed(&c, &data);
    time_iters(iters, || async {
        let mut stmt = c
            .prepare_cached("SELECT id, name, email, score, active FROM users")
            .unwrap();
        let out: Vec<UserRow> = stmt
            .query_map([], |row| {
                Ok(UserRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    email: row.get(2)?,
                    score: row.get(3)?,
                    active: row.get::<_, i64>(4)? != 0,
                })
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        std::hint::black_box(out);
    })
    .await
}

async fn bench_select_by_pk(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let c = fresh_conn();
    seed(&c, &data);
    let pk = (rows / 2) as i64;
    time_iters(iters, || async {
        let mut stmt = c
            .prepare_cached("SELECT id, name, email, score, active FROM users WHERE id = ?")
            .unwrap();
        let out: Vec<UserRow> = stmt
            .query_map(params![pk], |row| {
                Ok(UserRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    email: row.get(2)?,
                    score: row.get(3)?,
                    active: row.get::<_, i64>(4)? != 0,
                })
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        std::hint::black_box(out);
    })
    .await
}

async fn bench_update(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let c = fresh_conn();
        seed(&c, &data);
        c.execute("UPDATE users SET score = ? WHERE active = ?", params![999, 1])
            .unwrap();
    })
    .await
}

async fn bench_delete(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let c = fresh_conn();
        seed(&c, &data);
        c.execute("DELETE FROM users WHERE active = ?", params![0])
            .unwrap();
    })
    .await
}
