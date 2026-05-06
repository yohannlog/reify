use super::select::SelectBuilder;
use crate::ident::qi;
use crate::table::Table;
use crate::value::Value;

// ── Eager loading — WithBuilder ──────────────────────────────────────

/// Result of a `.with(relation)` eager-load: the parent rows paired with
/// their associated child rows, assembled in memory from two queries.
///
/// The two queries issued are:
/// 1. `SELECT * FROM from_table [WHERE …]`
/// 2. `SELECT * FROM to_table WHERE to_col IN (parent_key_values…)`
///
/// Then rows are grouped by the join key in memory — no N+1.
#[must_use = "WithBuilder is lazy; chain `.build_queries()` or an execution method to use it"]
pub struct WithBuilder<F: Table, T: Table> {
    pub(crate) parent: SelectBuilder<F>,
    pub(crate) rel: crate::relation::Relation<F, T>,
}

impl<F: Table, T: Table> WithBuilder<F, T> {
    /// Build the two SQL statements needed for eager loading.
    ///
    /// Returns `(parent_sql, parent_params, child_sql_template)`.
    /// The child SQL uses an `IN (?)` placeholder; the caller must
    /// substitute the actual parent key values at runtime.
    #[must_use]
    pub fn build_queries(&self) -> ((String, Vec<Value>), String) {
        let (parent_sql, parent_params) = self.parent.build();
        // Child query: SELECT * FROM to_table WHERE to_col IN (?)
        // The `?` is a placeholder for the IN list — expanded at runtime.
        let child_sql = format!(
            "SELECT * FROM {} WHERE {} IN (?)",
            qi(T::table_name()),
            qi(self.rel.to_col),
        );
        ((parent_sql, parent_params), child_sql)
    }

    /// The relation this builder was constructed from.
    pub fn relation(&self) -> &crate::relation::Relation<F, T> {
        &self.rel
    }

    /// The parent query builder.
    pub fn parent_builder(&self) -> &SelectBuilder<F> {
        &self.parent
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::{Relation, RelationType};
    use crate::table::Table;

    struct Author;
    impl Table for Author {
        fn table_name() -> &'static str {
            "authors"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "name"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![]
        }
    }

    struct Book;
    impl Table for Book {
        fn table_name() -> &'static str {
            "books"
        }
        fn column_names() -> &'static [&'static str] {
            &["id", "author_id", "title"]
        }
        fn as_values(&self) -> Vec<Value> {
            vec![]
        }
    }

    fn books_relation() -> Relation<Author, Book> {
        Relation::new("books", RelationType::HasMany, "id", "author_id")
    }

    #[test]
    fn build_queries_emits_parent_and_child_sql() {
        let parent = SelectBuilder::<Author>::new();
        let wb = WithBuilder {
            parent,
            rel: books_relation(),
        };
        let ((parent_sql, parent_params), child_sql) = wb.build_queries();
        assert!(parent_sql.contains("authors"), "parent SQL: {parent_sql}");
        assert!(parent_params.is_empty());
        assert!(
            child_sql.contains("books") && child_sql.contains("author_id"),
            "child SQL: {child_sql}"
        );
        assert!(child_sql.contains("IN (?)"));
    }

    #[test]
    fn relation_accessor_returns_underlying_relation() {
        let parent = SelectBuilder::<Author>::new();
        let wb = WithBuilder {
            parent,
            rel: books_relation(),
        };
        let r = wb.relation();
        assert_eq!(r.name, "books");
        assert_eq!(r.from_col, "id");
        assert_eq!(r.to_col, "author_id");
    }

    #[test]
    fn parent_builder_accessor_exposes_parent() {
        let parent = SelectBuilder::<Author>::new();
        let wb = WithBuilder {
            parent,
            rel: books_relation(),
        };
        // Build through accessor matches the standalone build.
        let (sql, _) = wb.parent_builder().build();
        assert!(sql.contains("authors"));
    }
}
