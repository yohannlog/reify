#[cfg(feature = "postgres")]
use reify::Table;
use reify::Value;

// ── IntoValue for arrays ───────────────────────────────────────────

#[cfg(feature = "postgres")]
#[test]
fn array_i32_into_value() {
    use reify::value::IntoValue;
    let v = vec![1i32, 2, 3];
    assert_eq!(v.into_value(), Value::ArrayI32(vec![1, 2, 3]));
}

#[cfg(feature = "postgres")]
#[test]
fn array_string_into_value() {
    use reify::value::IntoValue;
    let v = vec!["rust".to_string(), "go".to_string()];
    assert_eq!(
        v.into_value(),
        Value::ArrayString(vec!["rust".into(), "go".into()])
    );
}

#[cfg(feature = "postgres")]
#[test]
fn array_i64_into_value() {
    use reify::value::IntoValue;
    let v = vec![100i64, 200];
    assert_eq!(v.into_value(), Value::ArrayI64(vec![100, 200]));
}

#[cfg(feature = "postgres")]
#[test]
fn array_bool_into_value() {
    use reify::value::IntoValue;
    let v = vec![true, false, true];
    assert_eq!(v.into_value(), Value::ArrayBool(vec![true, false, true]));
}

#[cfg(feature = "postgres")]
#[test]
fn array_f64_into_value() {
    use reify::value::IntoValue;
    let v = vec![1.5f64, 2.5];
    assert_eq!(v.into_value(), Value::ArrayF64(vec![1.5, 2.5]));
}

// ── Table with array columns + operators ──────────────────────────

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;

    #[derive(Table, Debug, Clone)]
    #[table(name = "posts")]
    pub struct Post {
        #[column(primary_key)]
        pub id: i64,
        pub title: String,
        pub tags: Vec<String>,
        pub scores: Vec<i32>,
    }

    #[test]
    fn insert_with_array() {
        let post = Post {
            id: 1,
            title: "Hello".into(),
            tags: vec!["rust".into(), "db".into()],
            scores: vec![10, 20],
        };
        let (sql, params) = Post::insert(&post).build();
        assert_eq!(
            sql,
            "INSERT INTO \"posts\" (\"id\", \"title\", \"tags\", \"scores\") VALUES (?, ?, ?, ?)"
        );
        assert_eq!(
            params,
            vec![
                Value::I64(1),
                Value::String("Hello".into()),
                Value::ArrayString(vec!["rust".into(), "db".into()]),
                Value::ArrayI32(vec![10, 20]),
            ]
        );
    }

    #[test]
    fn filter_array_contains() {
        let (sql, params) = Post::find()
            .filter(Post::tags.contains(vec!["rust".to_string()]))
            .build();
        assert_eq!(sql, "SELECT * FROM \"posts\" WHERE \"tags\" @> ?");
        assert_eq!(params, vec![Value::ArrayString(vec!["rust".into()])]);
    }

    #[test]
    fn filter_array_contained_by() {
        let (sql, params) = Post::find()
            .filter(Post::tags.contained_by(vec![
                "rust".to_string(),
                "go".to_string(),
                "python".to_string(),
            ]))
            .build();
        assert_eq!(sql, "SELECT * FROM \"posts\" WHERE \"tags\" <@ ?");
        assert_eq!(
            params,
            vec![Value::ArrayString(vec![
                "rust".into(),
                "go".into(),
                "python".into(),
            ])]
        );
    }

    #[test]
    fn filter_array_overlaps() {
        let (sql, params) = Post::find()
            .filter(Post::tags.overlaps(vec!["rust".to_string(), "go".to_string()]))
            .build();
        assert_eq!(sql, "SELECT * FROM \"posts\" WHERE \"tags\" && ?");
        assert_eq!(
            params,
            vec![Value::ArrayString(vec!["rust".into(), "go".into()])]
        );
    }

    #[test]
    fn filter_array_contains_i32() {
        let (sql, params) = Post::find()
            .filter(Post::scores.contains(vec![10i32]))
            .build();
        assert_eq!(sql, "SELECT * FROM \"posts\" WHERE \"scores\" @> ?");
        assert_eq!(params, vec![Value::ArrayI32(vec![10])]);
    }

    #[test]
    fn update_array_column() {
        let (sql, params) = Post::update()
            .set(Post::tags, vec!["updated".to_string()])
            .filter(Post::id.eq(1i64))
            .build();
        assert_eq!(sql, "UPDATE \"posts\" SET \"tags\" = ? WHERE \"id\" = ?");
        assert_eq!(
            params,
            vec![Value::ArrayString(vec!["updated".into()]), Value::I64(1),]
        );
    }

    #[test]
    fn combined_array_filters() {
        let (sql, params) = Post::find()
            .filter(Post::tags.contains(vec!["rust".to_string()]))
            .filter(Post::title.contains("hello"))
            .build();
        assert_eq!(
            sql,
            "SELECT * FROM \"posts\" WHERE \"tags\" @> ? AND \"title\" LIKE ? ESCAPE '\\'"
        );
        assert_eq!(
            params,
            vec![
                Value::ArrayString(vec!["rust".into()]),
                Value::String("%hello%".into()),
            ]
        );
    }

    // ── FromValue Vec<f32> / Vec<f64> ─────────────────────────────────

    #[test]
    fn from_value_vec_f32() {
        use reify::value::FromValue;
        let val = Value::ArrayF32(vec![1.0f32, 2.5, 3.15]);
        let result = Vec::<f32>::from_value(val).unwrap();
        assert_eq!(result, vec![1.0f32, 2.5, 3.15]);
    }

    #[test]
    fn from_value_vec_f64() {
        use reify::value::FromValue;
        let val = Value::ArrayF64(vec![1.0f64, 2.5]);
        let result = Vec::<f64>::from_value(val).unwrap();
        assert_eq!(result, vec![1.0f64, 2.5]);
    }

    #[test]
    fn from_value_vec_f32_wrong_type_errors() {
        use reify::value::FromValue;
        let val = Value::ArrayI32(vec![1, 2]);
        assert!(Vec::<f32>::from_value(val).is_err());
    }

    // ── SqlType::Array DDL rendering ──────────────────────────────────

    #[test]
    fn sql_type_array_integer_ddl() {
        use reify::query::Dialect;
        use reify::schema::SqlType;
        let t = SqlType::Array(Box::new(SqlType::Integer));
        assert_eq!(t.to_sql(Dialect::Postgres), "INTEGER[]");
    }

    #[test]
    fn sql_type_array_text_ddl() {
        use reify::query::Dialect;
        use reify::schema::SqlType;
        let t = SqlType::Array(Box::new(SqlType::Text));
        assert_eq!(t.to_sql(Dialect::Postgres), "TEXT[]");
    }

    #[test]
    fn sql_type_array_bigint_ddl() {
        use reify::query::Dialect;
        use reify::schema::SqlType;
        let t = SqlType::Array(Box::new(SqlType::BigInt));
        assert_eq!(t.to_sql(Dialect::Postgres), "BIGINT[]");
    }

    #[test]
    fn sql_type_array_non_pg_falls_back_to_text() {
        use reify::query::Dialect;
        use reify::schema::SqlType;
        let t = SqlType::Array(Box::new(SqlType::Integer));
        assert_eq!(t.to_sql(Dialect::Generic), "TEXT");
    }

    // ── ANY / ALL operators ───────────────────────────────────────────

    #[test]
    fn filter_array_any_eq() {
        let (sql, params) = Post::find()
            .filter(Post::scores.array_any_eq(10i32))
            .build();
        assert_eq!(sql, "SELECT * FROM \"posts\" WHERE ? = ANY(\"scores\")");
        assert_eq!(params, vec![Value::I32(10)]);
    }

    #[test]
    fn filter_array_all_eq() {
        let (sql, params) = Post::find().filter(Post::scores.array_all_eq(0i32)).build();
        assert_eq!(sql, "SELECT * FROM \"posts\" WHERE ? = ALL(\"scores\")");
        assert_eq!(params, vec![Value::I32(0)]);
    }

    #[test]
    fn filter_array_any_eq_string() {
        let (sql, params) = Post::find()
            .filter(Post::tags.array_any_eq("rust".to_string()))
            .build();
        assert_eq!(sql, "SELECT * FROM \"posts\" WHERE ? = ANY(\"tags\")");
        assert_eq!(params, vec![Value::String("rust".into())]);
    }

    // ── array_append / array_prepend ──────────────────────────────────

    #[test]
    fn update_array_append() {
        let (sql, params) = Post::update()
            .set_array_append(Post::tags, "new_tag".to_string())
            .filter(Post::id.eq(1i64))
            .build();
        assert_eq!(
            sql,
            "UPDATE \"posts\" SET \"tags\" = \"tags\" || ? WHERE \"id\" = ?"
        );
        assert_eq!(params, vec![Value::String("new_tag".into()), Value::I64(1)]);
    }

    #[test]
    fn update_array_prepend() {
        let (sql, params) = Post::update()
            .set_array_prepend(Post::tags, "first_tag".to_string())
            .filter(Post::id.eq(1i64))
            .build();
        assert_eq!(
            sql,
            "UPDATE \"posts\" SET \"tags\" = ? || \"tags\" WHERE \"id\" = ?"
        );
        assert_eq!(
            params,
            vec![Value::String("first_tag".into()), Value::I64(1)]
        );
    }

    #[test]
    fn update_array_append_i32() {
        let (sql, params) = Post::update()
            .set_array_append(Post::scores, 99i32)
            .filter(Post::id.eq(2i64))
            .build();
        assert_eq!(
            sql,
            "UPDATE \"posts\" SET \"scores\" = \"scores\" || ? WHERE \"id\" = ?"
        );
        assert_eq!(params, vec![Value::I32(99), Value::I64(2)]);
    }
}
