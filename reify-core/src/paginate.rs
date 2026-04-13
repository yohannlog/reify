use std::marker::PhantomData;

use crate::column::Column;
use crate::condition::Condition;
use crate::query::SelectBuilder;
use crate::sql::{OrderFragment, SqlFragment};
use crate::table::Table;
use crate::value::{IntoValue, Value};

// ── Page result ─────────────────────────────────────────────────────

/// A page of results with metadata for navigation.
#[derive(Debug, Clone)]
pub struct Page<M> {
    /// Current page number (1-indexed).
    pub page: u64,
    /// Items per page.
    pub per_page: u64,
    /// Total number of items (from COUNT query).
    pub total_items: u64,
    /// Total number of pages.
    pub total_pages: u64,
    /// Whether there is a next page.
    pub has_next: bool,
    /// Whether there is a previous page.
    pub has_prev: bool,
    _model: PhantomData<M>,
}

impl<M> Page<M> {
    pub fn new(page: u64, per_page: u64, total_items: u64) -> Self {
        let total_pages = if total_items == 0 {
            1
        } else {
            (total_items + per_page - 1) / per_page
        };
        Self {
            page,
            per_page,
            total_items,
            total_pages,
            has_next: page < total_pages,
            has_prev: page > 1,
            _model: PhantomData,
        }
    }
}

// ── Offset-based pagination ─────────────────────────────────────────

/// Builder for offset-based pagination (classic `LIMIT/OFFSET`).
///
/// ```ignore
/// let paginated = User::find()
///     .filter(User::role.is_not_null())
///     .paginate(3, 25);  // page 3, 25 per page
///
/// let (data_sql, count_sql, params) = paginated.build();
/// ```
pub struct Paginated<M: Table> {
    builder: SelectBuilder<M>,
    page: u64,
    per_page: u64,
}

impl<M: Table> Paginated<M> {
    pub fn new(builder: SelectBuilder<M>, page: u64, per_page: u64) -> Self {
        assert!(page >= 1, "Page number must be >= 1");
        assert!(per_page >= 1, "Per-page must be >= 1");
        Self {
            builder,
            page,
            per_page,
        }
    }

    /// Build both the data query and the count query.
    ///
    /// Returns `(data_sql, count_sql, params)`.
    /// - `data_sql`: SELECT with LIMIT/OFFSET applied
    /// - `count_sql`: SELECT COUNT(*) with the same WHERE clause
    /// - `params`: shared parameters (used by both queries)
    pub fn build(&self) -> (String, String, Vec<Value>) {
        let ast = self.builder.build_ast();
        let offset = (self.page - 1) * self.per_page;

        // Data query: set LIMIT/OFFSET on the AST
        let data_ast = match ast {
            SqlFragment::Select {
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                order_by,
                ..
            } => SqlFragment::Select {
                columns,
                from,
                joins,
                conditions,
                group_by,
                having,
                order_by,
                limit: Some(self.per_page),
                offset: Some(offset),
            },
            raw => raw,
        };

        let mut data_params = Vec::new();
        let data_sql = data_ast.render(&mut data_params);

        // Count query: from the AST
        let count_ast = self.builder.build_ast().to_count_query();
        let mut count_params = Vec::new();
        let count_sql = count_ast.render(&mut count_params);

        // Use data_params as the shared params (count params are identical)
        (data_sql, count_sql, data_params)
    }

    /// Create a `Page` metadata object from a known total count.
    pub fn page_info(&self, total_items: u64) -> Page<M> {
        Page::new(self.page, self.per_page, total_items)
    }
}

// ── Cursor-based pagination ─────────────────────────────────────────

/// Builder for cursor-based pagination (keyset pagination).
///
/// More performant than offset for large datasets — uses WHERE instead of OFFSET.
///
/// ```ignore
/// let page = User::find()
///     .filter(User::role.is_not_null())
///     .after(User::id, 150, 25);  // 25 items after id=150
///
/// let (sql, params) = page.build();
/// ```
pub struct CursorPaginated<M: Table> {
    builder: SelectBuilder<M>,
    cursor_column: &'static str,
    cursor_value: Option<Value>,
    direction: CursorDirection,
    limit: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    Forward,
    Backward,
}

impl<M: Table> CursorPaginated<M> {
    pub fn new(
        builder: SelectBuilder<M>,
        cursor_column: &'static str,
        cursor_value: Option<Value>,
        direction: CursorDirection,
        limit: u64,
    ) -> Self {
        assert!(limit >= 1, "Limit must be >= 1");
        Self {
            builder,
            cursor_column,
            cursor_value,
            direction,
            limit,
        }
    }

    /// Build the cursor-paginated query.
    ///
    /// Requests `limit + 1` rows to detect if there are more results.
    pub fn build(&self) -> (String, Vec<Value>) {
        let ast = self.builder.build_ast();

        // Operate on the AST: strip limit/offset, add cursor condition + ordering
        let ast = match ast {
            SqlFragment::Select {
                columns,
                from,
                joins,
                mut conditions,
                group_by,
                having,
                ..
            } => {
                // Add cursor condition
                if let Some(ref val) = self.cursor_value {
                    let cursor_cond = match self.direction {
                        CursorDirection::Forward => Condition::Gt(self.cursor_column, val.clone()),
                        CursorDirection::Backward => Condition::Lt(self.cursor_column, val.clone()),
                    };
                    conditions.push(cursor_cond);
                }

                // Replace ORDER BY with cursor ordering
                let descending = self.direction == CursorDirection::Backward;
                let order_by = vec![OrderFragment {
                    column: self.cursor_column.to_string(),
                    descending,
                }];

                SqlFragment::Select {
                    columns,
                    from,
                    joins,
                    conditions,
                    group_by,
                    having,
                    order_by,
                    limit: Some(self.limit + 1), // fetch one extra to detect has_next
                    offset: None,
                }
            }
            raw => raw,
        };

        let mut params = Vec::new();
        let sql = ast.render(&mut params);
        (sql, params)
    }

    /// Check if there are more results based on the number of rows returned.
    ///
    /// Pass the actual row count from your query result.
    /// If `row_count > limit`, there are more pages — trim the last row from your results.
    pub fn has_more(&self, row_count: u64) -> bool {
        row_count > self.limit
    }
}

// ── SelectBuilder integration ───────────────────────────────────────

impl<M: Table> SelectBuilder<M> {
    /// Offset-based pagination: page N with `per_page` items.
    ///
    /// ```ignore
    /// let paginated = User::find()
    ///     .filter(User::role.is_not_null())
    ///     .paginate(3, 25);
    /// let (data_sql, count_sql, params) = paginated.build();
    /// ```
    pub fn paginate(self, page: u64, per_page: u64) -> Paginated<M> {
        Paginated::new(self, page, per_page)
    }

    /// Cursor-based pagination: `limit` items after the given cursor value.
    ///
    /// ```ignore
    /// let page = User::find()
    ///     .after(User::id, 150i64, 25);
    /// let (sql, params) = page.build();
    /// ```
    pub fn after<T: IntoValue>(
        self,
        cursor: Column<M, T>,
        value: impl IntoValue,
        limit: u64,
    ) -> CursorPaginated<M> {
        CursorPaginated::new(
            self,
            cursor.name,
            Some(value.into_value()),
            CursorDirection::Forward,
            limit,
        )
    }

    /// Cursor-based pagination: `limit` items before the given cursor value.
    ///
    /// ```ignore
    /// let page = User::find()
    ///     .before(User::id, 100i64, 25);
    /// let (sql, params) = page.build();
    /// ```
    pub fn before<T: IntoValue>(
        self,
        cursor: Column<M, T>,
        value: impl IntoValue,
        limit: u64,
    ) -> CursorPaginated<M> {
        CursorPaginated::new(
            self,
            cursor.name,
            Some(value.into_value()),
            CursorDirection::Backward,
            limit,
        )
    }
}

// Text-based helpers removed — pagination now operates on SqlFragment AST.
// See SqlFragment::to_count_query(), without_limit_offset(), without_order_by().
