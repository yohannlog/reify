//! Reify scenarios — use the public `reify::` API end-to-end.

use std::time::Duration;

use reify::{
    Database, InsertManyBuilder, SqliteDb, Table, delete, fetch, insert, raw_execute, update,
};

use super::model::{CREATE_TABLE_SQL, DROP_TABLE_SQL, UserRow, make_rows};
use crate::runner::Scenario;
use crate::time_iters;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    pub name: String,
    pub email: String,
    pub score: i32,
    pub active: bool,
}

impl From<&UserRow> for User {
    fn from(r: &UserRow) -> Self {
        User {
            id: r.id,
            name: r.name.clone(),
            email: r.email.clone(),
            score: r.score,
            active: r.active,
        }
    }
}

async fn fresh_db() -> SqliteDb {
    let db = SqliteDb::open_in_memory().expect("open in-memory");
    raw_execute(&db, CREATE_TABLE_SQL, &[])
        .await
        .expect("create table");
    db
}

async fn seed(db: &SqliteDb, rows: &[UserRow]) {
    // Bulk seed — used by scenarios that read/modify pre-existing rows.
    let users: Vec<User> = rows.iter().map(|r| r.into()).collect();
    db.transaction(Box::new(move |tx| {
        Box::pin(async move {
            for u in &users {
                let (sql, params) = User::insert(u).build();
                tx.execute(&sql, &params).await?;
            }
            Ok(())
        })
    }))
    .await
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
        let db = fresh_db().await;
        for r in &data {
            let u: User = r.into();
            insert(&db, &User::insert(&u)).await.unwrap();
        }
        raw_execute(&db, DROP_TABLE_SQL, &[]).await.unwrap();
    })
    .await
}

async fn bench_insert_batch(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let db = fresh_db().await;
        let models: Vec<User> = data.iter().map(User::from).collect();
        let builder = InsertManyBuilder::new(&models);
        let (sql, params) = builder.build();
        raw_execute(&db, &sql, &params).await.unwrap();
        raw_execute(&db, DROP_TABLE_SQL, &[]).await.unwrap();
    })
    .await
}

async fn bench_select_all(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let db = fresh_db().await;
    seed(&db, &data).await;
    time_iters(iters, || async {
        let result = fetch::<User>(&db, &User::find()).await.unwrap();
        std::hint::black_box(result);
    })
    .await
}

async fn bench_select_by_pk(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let db = fresh_db().await;
    seed(&db, &data).await;
    let pk = (rows / 2) as i64;
    time_iters(iters, || async {
        let result = fetch::<User>(&db, &User::find().filter(User::id.eq(pk)))
            .await
            .unwrap();
        std::hint::black_box(result);
    })
    .await
}

async fn bench_update(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let db = fresh_db().await;
        seed(&db, &data).await;
        update(
            &db,
            &User::update()
                .set(User::score, 999_i32)
                .filter(User::active.eq(true)),
        )
        .await
        .unwrap();
    })
    .await
}

async fn bench_delete(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let db = fresh_db().await;
        seed(&db, &data).await;
        delete(&db, &User::delete().filter(User::active.eq(false)))
            .await
            .unwrap();
    })
    .await
}
