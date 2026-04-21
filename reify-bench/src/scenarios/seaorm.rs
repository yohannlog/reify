//! SeaORM scenarios.

use std::time::Duration;

use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, Database, DatabaseConnection,
    DeriveEntityModel, DeriveRelation, EntityTrait, QueryFilter, Set, Statement,
};

use super::model::{CREATE_TABLE_SQL, UserRow, make_rows};
use crate::runner::Scenario;
use crate::time_iters;

mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "users")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub name: String,
        pub email: String,
        pub score: i32,
        pub active: bool,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

async fn fresh_db() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.expect("connect");
    db.execute(Statement::from_string(
        db.get_database_backend(),
        CREATE_TABLE_SQL.to_string(),
    ))
    .await
    .expect("create table");
    db
}

fn to_active(r: &UserRow) -> entity::ActiveModel {
    entity::ActiveModel {
        id: Set(r.id),
        name: Set(r.name.clone()),
        email: Set(r.email.clone()),
        score: Set(r.score),
        active: Set(r.active),
    }
}

async fn seed(db: &DatabaseConnection, rows: &[UserRow]) {
    let models: Vec<entity::ActiveModel> = rows.iter().map(to_active).collect();
    entity::Entity::insert_many(models)
        .exec(db)
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
            to_active(r).insert(&db).await.unwrap();
        }
    })
    .await
}

async fn bench_insert_batch(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let db = fresh_db().await;
        let models: Vec<entity::ActiveModel> = data.iter().map(to_active).collect();
        entity::Entity::insert_many(models).exec(&db).await.unwrap();
    })
    .await
}

async fn bench_select_all(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let db = fresh_db().await;
    seed(&db, &data).await;
    time_iters(iters, || async {
        let out = entity::Entity::find().all(&db).await.unwrap();
        std::hint::black_box(out);
    })
    .await
}

async fn bench_select_by_pk(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    let db = fresh_db().await;
    seed(&db, &data).await;
    let pk = (rows / 2) as i64;
    time_iters(iters, || async {
        let out = entity::Entity::find_by_id(pk).one(&db).await.unwrap();
        std::hint::black_box(out);
    })
    .await
}

async fn bench_update(rows: usize, iters: usize) -> Duration {
    let data = make_rows(rows);
    time_iters(iters, || async {
        let db = fresh_db().await;
        seed(&db, &data).await;
        entity::Entity::update_many()
            .col_expr(
                entity::Column::Score,
                sea_orm::sea_query::Expr::value(999_i32),
            )
            .filter(entity::Column::Active.eq(true))
            .exec(&db)
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
        entity::Entity::delete_many()
            .filter(entity::Column::Active.eq(false))
            .exec(&db)
            .await
            .unwrap();
    })
    .await
}

// Unused — silences warnings from star-imported items when only a subset is
// used by some scenarios.
#[allow(dead_code)]
fn _silence<T: ActiveModelTrait + ColumnTrait + ConnectionTrait>() {
    let _ = ActiveValue::<i32>::NotSet;
}
