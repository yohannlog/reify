use std::marker::PhantomData;

use crate::column::Column;
use crate::query::SelectBuilder;
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
        let (base_sql, params) = self.builder.build();

        // Data query: inject LIMIT/OFFSET (replace existing if any)
        let data_sql = strip_limit_offset(&base_sql);
        let offset = (self.page - 1) * self.per_page;
        let data_sql = format!("{data_sql} LIMIT {} OFFSET {offset}", self.per_page);

        // Count query: replace SELECT ... FROM with SELECT COUNT(*) FROM
        let count_sql = to_count_query(&base_sql);

        (data_sql, count_sql, params)
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
        let (base_sql, mut params) = self.builder.build();

        let base_sql = strip_limit_offset(&base_sql);

        let has_where = base_sql.contains(" WHERE ");
        let connector = if has_where { " AND" } else { " WHERE" };

        let mut sql = base_sql;

        // Add cursor condition if we have a cursor value
        if let Some(ref val) = self.cursor_value {
            let op = match self.direction {
                CursorDirection::Forward => ">",
                CursorDirection::Backward => "<",
            };
            params.push(val.clone());
            sql = format!("{sql}{connector} {} {op} ?", self.cursor_column);
        }

        // Order by cursor column
        let order = match self.direction {
            CursorDirection::Forward => "ASC",
            CursorDirection::Backward => "DESC",
        };

        // Strip existing ORDER BY to replace with cursor ordering
        let sql = strip_order_by(&sql);
        let sql = format!(
            "{sql} ORDER BY {} {order} LIMIT {}",
            self.cursor_column,
            self.limit + 1 // fetch one extra to detect has_next
        );

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

// ── Helpers ─────────────────────────────────────────────────────────

fn strip_limit_offset(sql: &str) -> String {
    let sql = sql.trim_end();
    // Remove trailing OFFSET ...
    let sql = if let Some(idx) = sql.to_uppercase().rfind(" OFFSET ") {
        &sql[..idx]
    } else {
        sql
    };
    // Remove trailing LIMIT ...
    if let Some(idx) = sql.to_uppercase().rfind(" LIMIT ") {
        sql[..idx].to_string()
    } else {
        sql.to_string()
    }
}

fn strip_order_by(sql: &str) -> String {
    if let Some(idx) = sql.to_uppercase().rfind(" ORDER BY ") {
        sql[..idx].to_string()
    } else {
        sql.to_string()
    }
}

fn to_count_query(sql: &str) -> String {
    let upper = sql.to_uppercase();
    if let Some(from_idx) = upper.find(" FROM ") {
        let rest = &sql[from_idx..];
        // Strip ORDER BY, LIMIT, OFFSET from count query
        let rest = strip_order_by(rest);
        let rest = strip_limit_offset(&rest);
        format!("SELECT COUNT(*){rest}")
    } else {
        // Fallback — shouldn't happen with well-formed queries
        sql.to_string()
    }
}
