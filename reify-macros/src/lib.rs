mod db_enum;
mod helpers;
mod partial_model;
mod relations;
mod table;
mod view;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

/// Derive macro that implements `Table` and generates typed column constants + query builder helpers.
///
/// # Usage
/// ```ignore
/// #[derive(Table)]
/// #[table(name = "users")]
/// pub struct User {
///     #[column(primary_key, auto_increment)]
///     pub id: i64,
///     #[column(unique, index)]
///     pub email: String,
/// }
/// ```
///
/// ## Composite indexes
///
/// ```ignore
/// #[derive(Table)]
/// #[table(
///     name = "users",
///     index(columns("email", "role")),
///     index(columns("email", "role"), unique, name = "idx_email_role"),
/// )]
/// pub struct User { /* ... */ }
/// ```
///
/// ## Validation rules (`dto-validation` feature)
///
/// `#[column(validate(...))]` forwards its rules to `validator::Validate`
/// on the generated `{Model}Dto`. Every rule the `validator` crate
/// supports is available — rules are parsed at macro-expansion time so
/// typos are rejected with a span-anchored error instead of silently
/// accepted.
///
/// **Built-in rules:** `email`, `url`, `length(min = …, max = …)`,
/// `range(min = …, max = …)`, `regex(path = …)`, `contains(…)`,
/// `does_not_contain(…)`, `must_match(…)`, `phone`, `credit_card`, `ip`,
/// `ip_v4`, `ip_v6`, `non_control_character`, `required`,
/// `required_nested`, `nested`, `skip_on_field_errors`, and
/// `custom(function = "path::to::fn")` for arbitrary logic (including
/// async, with `validator`'s `async` feature).
///
/// ```ignore
/// use reify::Table;
///
/// #[derive(Table, Debug, Clone)]
/// #[table(name = "users")]
/// pub struct User {
///     #[column(primary_key, auto_increment)]
///     pub id: i64,
///
///     // multiple rules, comma-separated
///     #[column(unique, validate(email, length(max = 254)))]
///     pub email: String,
///
///     // custom validation function
///     #[column(validate(custom(function = "crate::validators::check_slug")))]
///     pub slug: String,
///
///     // Option<T> fields are automatically nullable — no `nullable` attribute needed.
///     // Value rules on Option<T> MUST include `required` to reject `None`,
///     // otherwise the macro refuses to compile (validator skips `None`
///     // silently by default, which is a common footgun).
///     #[column(validate(required, length(min = 1)))]
///     pub bio: Option<String>,
/// }
/// ```
///
/// Use `{Model}Dto::validated_insert(&dto)` /
/// `validated_insert_many(&[dto])` to run validation before the DB
/// round-trip — these return `Result<InsertBuilder<Dto>,
/// validator::ValidationErrors>` so forgetting to call `.validate()`
/// stops being possible.
#[proc_macro_derive(Table, attributes(table, column))]
pub fn derive_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match table::impl_table(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive macro that reads `#[relations(...)]` attributes on a struct and
/// generates typed relation factory methods.
///
/// # Syntax
///
/// ```ignore
/// #[derive(Table, Relations)]
/// #[table(name = "users")]
/// #[relations(
///     has_many(posts:   Post,    foreign_key = "user_id"),
///     has_one( profile: Profile, foreign_key = "user_id"),
///     belongs_to(team: Team,    foreign_key = "team_id"),
/// )]
/// pub struct User {
///     pub id:      i64,
///     pub team_id: i64,
/// }
/// ```
///
/// Each entry generates a `pub fn <name>() -> Relation<Self, Target>` method
/// on the struct.
///
/// - `has_many` / `has_one`: `from_col` defaults to `"id"`, `to_col` = `foreign_key`.
/// - `belongs_to`: `from_col` = `foreign_key`, `to_col` defaults to `"id"`.
#[proc_macro_derive(Relations, attributes(relations))]
pub fn derive_relations(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match relations::impl_relations(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive macro that implements `DbEnum` + `IntoValue` for a unit enum.
///
/// Variants are lowercased by default (`Admin` → `"admin"`).
/// Use `#[db_enum(rename = "custom_name")]` to override.
///
/// # Usage
/// ```ignore
/// #[derive(DbEnum, Debug, Clone, PartialEq)]
/// pub enum Role {
///     Admin,
///     Member,
///     Guest,
/// }
///
/// #[derive(DbEnum, Debug, Clone, PartialEq)]
/// pub enum Status {
///     Active,
///     #[db_enum(rename = "on_hold")]
///     OnHold,
///     Archived,
/// }
/// ```
#[proc_macro_derive(DbEnum, attributes(db_enum))]
pub fn derive_db_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match db_enum::impl_db_enum(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive macro that generates `FromRow` and a `select_columns()` helper
/// for a partial model.
#[proc_macro_derive(PartialModel, attributes(partial_model))]
pub fn derive_partial_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match partial_model::impl_partial_model(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive macro that implements `View` (and a minimal `Table`) for read-only
/// SQL views, plus typed column constants and a `find()` query builder.
///
/// # Usage
///
/// ```ignore
/// #[derive(View, Debug, Clone)]
/// #[view(name = "active_users", query = "SELECT id, email FROM users WHERE deleted_at IS NULL")]
/// pub struct ActiveUser {
///     pub id: i64,
///     pub email: String,
/// }
///
/// // Read-only query builder
/// let (sql, params) = ActiveUser::find()
///     .filter(ActiveUser::email.ends_with("@corp.io"))
///     .build();
/// ```
///
/// The `query` attribute is optional — you can define the query via the
/// `ViewSchemaDef` trait or `ViewSchema` builder instead.
#[proc_macro_derive(View, attributes(view, column))]
pub fn derive_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match view::impl_view(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
