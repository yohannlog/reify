# TODO1.md — Plan d'action détaillé Reify ORM

> Généré le 2025-01-XX — Revue de code complète sur ~10 000 lignes  
> Organisé par priorité décroissante. Chaque item est actionnable avec fichier(s) cible(s).

---

## Table des matières

- [Phase 1 — Sécurité & Bugs critiques](#phase-1--sécurité--bugs-critiques)
- [Phase 2 — Ergonomie & Adoption](#phase-2--ergonomie--adoption)
- [Phase 3 — Architecture & Refactoring](#phase-3--architecture--refactoring)
- [Phase 4 — Features & Complétude](#phase-4--features--complétude)
- [Phase 5 — Performance & Optimisation](#phase-5--performance--optimisation)
- [Phase 6 — Cohérence & Polish](#phase-6--cohérence--polish)
- [Phase 7 — Écosystème & Outillage](#phase-7--écosystème--outillage)
- [Annexe — Points forts à préserver](#annexe--points-forts-à-préserver)

---

## Phase 1 — Sécurité & Bugs critiques

> 🔴 **Bloquant pour toute utilisation en production.**  
> Estimation : 3-5 jours de travail.

### 1.1 — Quoting des identifiants SQL (SQL Injection)

**Problème :** Les noms de tables et colonnes sont interpolés directement dans le SQL sans quoting. Si un nom contient un mot réservé SQL ou un caractère spécial, le SQL est invalide ou exploitable. Le `MigrationContext` accepte des `&str` dynamiques, ce qui aggrave le risque.

**Fichiers concernés :**
- `reify-core/src/sql.rs` — `SqlFragment::render()` (lignes 109, 112-118)
- `reify-core/src/query.rs` — `InsertBuilder::build_with_dialect()` (ligne 505), `UpdateBuilder::build()` (ligne 1011), `DeleteBuilder::build()` (ligne 1113)
- `reify-core/src/migration.rs` — `MigrationContext::add_column()` (ligne 64-67), `create_table_sql()` (ligne 389+)
- `reify-core/src/view.rs` — `create_view_sql()` (ligne 152), `drop_view_sql()` (ligne 157)
- `reify-core/src/rls.rs` — `scoped_fetch_all()`, `scoped_update()`, `scoped_delete()`

**Plan d'action :**
1. Créer un module `reify-core/src/ident.rs` avec :
   ```rust
   /// Quote un identifiant SQL selon le dialecte.
   /// PostgreSQL : "nom" (double quotes, escape " → "")
   /// MySQL : `nom` (backticks, escape ` → ``)
   /// SQLite : "nom" (double quotes)
   /// Generic : "nom"
   pub fn quote_ident(name: &str, dialect: Dialect) -> String;
   
   /// Valide qu'un identifiant ne contient pas de caractères dangereux.
   /// Utilisé comme garde supplémentaire pour les noms venant de macros.
   pub fn validate_ident(name: &str) -> Result<(), String>;
   ```
2. Appliquer `quote_ident()` dans tous les points d'interpolation listés ci-dessus.
3. Ajouter des tests avec des noms réservés (`"order"`, `"group"`, `"user"`) et des caractères spéciaux.
4. Pour les noms venant de `&'static str` (macros), appliquer `validate_ident()` au compile-time dans la proc-macro.

**Tests à ajouter :**
- `tests/sql_injection.rs` — noms de tables/colonnes avec mots réservés, guillemets, backticks, points-virgules.

---

### 1.2 — Échappement des wildcards LIKE/ILIKE

**Problème :** `contains()`, `starts_with()`, `ends_with()` et leurs variantes `i*` interpolent la valeur utilisateur directement dans le pattern LIKE sans échapper `%` et `_`.

**Fichiers concernés :**
- `reify-core/src/column.rs` — lignes 133-167 (String operators), lignes 153-167 (ILIKE operators)

**Plan d'action :**
1. Créer une fonction helper :
   ```rust
   fn escape_like(input: &str) -> String {
       input.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
   }
   ```
2. L'appliquer dans `contains()`, `starts_with()`, `ends_with()`, `icontains()`, `istarts_with()`, `iends_with()`.
3. Modifier `Condition::Like` et `Condition::ILike` dans `sql.rs` pour ajouter `ESCAPE '\'` au SQL généré :
   ```sql
   col LIKE ? ESCAPE '\'
   ```
4. Conserver les méthodes `like()` et `ilike()` sans échappement pour les cas où l'utilisateur veut ses propres wildcards.

**Tests à ajouter :**
- `contains("50%")` → `LIKE '%50\%%' ESCAPE '\'`
- `starts_with("test_")` → `LIKE 'test\_%' ESCAPE '\'`

---

### 1.3 — RLS bypass dans les transactions

**Problème :** `Scoped::transaction()` délègue au `DynDatabase` interne. La closure reçoit `&dyn DynDatabase` (le pool brut), pas un `Scoped`. Toutes les requêtes dans la transaction contournent silencieusement le RLS.

**Fichier concerné :**
- `reify-core/src/rls.rs` — lignes 116-122

**Plan d'action :**
1. Modifier `Scoped::transaction()` pour wrapper la connexion de transaction :
   ```rust
   async fn transaction<'a>(&'a self, f: TransactionFn<'a>) -> Result<(), DbError> {
       // Créer un Scoped autour de la connexion de transaction
       let ctx = self.ctx.clone();
       self.inner.transaction(Box::new(move |tx_db| {
           let scoped_tx = Scoped::new(tx_db, ctx);
           f(&scoped_tx)
       })).await
   }
   ```
2. Ajouter un test qui vérifie que le RLS est appliqué à l'intérieur d'une transaction.
3. Documenter le comportement dans la doc de `Scoped`.

---

### 1.4 — Panics → Results pour les builders

**Problème :** `UpdateBuilder::build()`, `DeleteBuilder::build()`, et `InsertManyBuilder::new()` paniquent au runtime. En production, un panic = crash du serveur.

**Fichiers concernés :**
- `reify-core/src/query.rs` — lignes 546, 988-991, 1106-1109

**Plan d'action :**
1. Créer un type d'erreur dédié :
   ```rust
   #[derive(Debug, Clone)]
   pub enum BuildError {
       /// UPDATE/DELETE sans WHERE clause.
       MissingFilter { operation: &'static str },
       /// INSERT avec 0 rows.
       EmptyInsert,
   }
   ```
2. Changer les signatures :
   - `build()` → `build() -> Result<(String, Vec<Value>), BuildError>`
   - `InsertManyBuilder::new()` → `new() -> Result<Self, BuildError>`
3. **Alternative (moins breaking) :** Garder `build()` qui panic, ajouter `try_build()` qui retourne un `Result`. Documenter clairement.
4. Ajouter une méthode `.unfiltered()` / `.all_rows()` sur `UpdateBuilder` et `DeleteBuilder` pour les cas légitimes de UPDATE/DELETE sans WHERE :
   ```rust
   User::update().set(User::active, false).unfiltered().build()
   // → UPDATE users SET active = ? (sans WHERE, explicitement voulu)
   ```

---

### 1.5 — Transaction PostgreSQL : connexion isolée

**Problème :** (Identifié dans REVIEW_PART_2.md) La transaction doit utiliser une connexion dédiée, pas le pool. Vérifier que l'implémentation actuelle dans `reify-postgres` est correcte après les corrections précédentes.

**Fichier concerné :**
- `reify-postgres/src/lib.rs` — implémentation de `Database::transaction()`

**Plan d'action :**
1. Vérifier que `transaction()` dans chaque adapter :
   - Acquiert une connexion dédiée du pool
   - Exécute `BEGIN` sur cette connexion
   - Passe cette connexion (pas le pool) à la closure
   - Exécute `COMMIT` ou `ROLLBACK` sur la même connexion
2. Ajouter un test d'intégration qui vérifie l'isolation (deux transactions concurrentes ne se voient pas).

---

## Phase 2 — Ergonomie & Adoption

> 🟠 **Critique pour l'adoption par les développeurs.**  
> Estimation : 5-8 jours de travail.

### 2.1 — `#[derive(FromRow)]` automatique

**Problème :** L'utilisateur doit écrire manuellement l'implémentation `FromRow` pour chaque struct. C'est le point de friction #1 pour l'adoption.

**Fichiers concernés :**
- `reify-macros/src/lib.rs` — ajouter un nouveau derive
- `reify-core/src/db.rs` — trait `FromRow` (ligne 45-47)

**Plan d'action :**
1. **Option A (recommandée) :** Générer `FromRow` automatiquement dans `#[derive(Table)]`. Chaque champ est extrait par nom de colonne via `row.get("col_name")` et converti via un trait `FromValue`.
2. **Option B :** Créer un `#[derive(FromRow)]` séparé pour les cas où on veut un `FromRow` sans `Table` (projections, vues).
3. Créer un trait `FromValue` :
   ```rust
   pub trait FromValue: Sized {
       fn from_value(val: &Value) -> Result<Self, DbError>;
   }
   ```
   Avec des impls pour `i16`, `i32`, `i64`, `f32`, `f64`, `bool`, `String`, `Option<T>`, `Vec<u8>`, et les types PG/MySQL derrière features.
4. Le code généré par la macro :
   ```rust
   impl FromRow for User {
       fn from_row(row: &Row) -> Result<Self, DbError> {
           Ok(User {
               id: FromValue::from_value(row.get("id").ok_or(...)?)?,
               email: FromValue::from_value(row.get("email").ok_or(...)?)?,
               // ...
           })
       }
   }
   ```

**Tests à ajouter :**
- Conversion réussie avec tous les types supportés
- Erreur propre quand une colonne manque
- Erreur propre quand le type ne correspond pas
- Support de `Option<T>` (NULL → None)

---

### 2.2 — Méthodes `.fetch()` / `.execute()` directement sur les builders

**Problème :** L'API actuelle force l'utilisation de free functions :
```rust
let users = reify::fetch(&db, &User::find().filter(...)).await?;
```
L'API idéale :
```rust
let users = User::find().filter(...).fetch(&db).await?;
```

**Fichiers concernés :**
- `reify-core/src/query.rs` — `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`
- `reify-core/src/db.rs` — free functions `fetch()`, `insert()`, etc.

**Plan d'action :**
1. Ajouter des méthodes sur chaque builder :
   ```rust
   impl<M: Table + FromRow> SelectBuilder<M> {
       pub async fn fetch(&self, db: &impl Database) -> Result<Vec<M>, DbError> {
           crate::db::fetch(db, self).await
       }
       pub async fn fetch_one(&self, db: &impl Database) -> Result<M, DbError> { ... }
       pub async fn fetch_optional(&self, db: &impl Database) -> Result<Option<M>, DbError> { ... }
   }
   
   impl<M: Table> InsertBuilder<M> {
       pub async fn execute(&self, db: &impl Database) -> Result<u64, DbError> {
           crate::db::insert(db, self).await
       }
   }
   ```
2. Conserver les free functions comme alternative (backward compat).
3. Ajouter `fetch_one()` et `fetch_optional()` qui n'existent pas encore.

---

### 2.3 — Feature flag `sqlite` manquant

**Problème :** `reify-sqlite` existe comme crate mais n'est pas ré-exporté par le crate `reify` derrière un feature flag.

**Fichiers concernés :**
- `reify/Cargo.toml`
- `reify/src/lib.rs` — lignes 37-41

**Plan d'action :**
1. Ajouter dans `reify/Cargo.toml` :
   ```toml
   [features]
   sqlite = ["dep:reify-sqlite"]
   
   [dependencies]
   reify-sqlite = { path = "../reify-sqlite", optional = true }
   ```
2. Ajouter dans `reify/src/lib.rs` :
   ```rust
   #[cfg(feature = "sqlite")]
   pub use reify_sqlite::{self, SqliteDb};
   ```

---

### 2.4 — Type `Query` dédié au lieu de `(String, Vec<Value>)`

**Problème :** `build()` retourne un tuple anonyme `(String, Vec<Value>)` partout. Pas de méthodes, pas de Debug lisible, pas d'extension possible.

**Fichier concerné :**
- Nouveau fichier `reify-core/src/built_query.rs`

**Plan d'action :**
1. Créer :
   ```rust
   #[derive(Debug, Clone)]
   pub struct BuiltQuery {
       pub sql: String,
       pub params: Vec<Value>,
   }
   
   impl BuiltQuery {
       /// Rewrite `?` placeholders to PostgreSQL `$1, $2, ...`.
       pub fn rewrite_pg(&self) -> BuiltQuery { ... }
       
       /// Debug-friendly display with params inlined.
       pub fn display_debug(&self) -> String { ... }
       
       /// Destructure into tuple (backward compat).
       pub fn into_parts(self) -> (String, Vec<Value>) { (self.sql, self.params) }
   }
   
   impl From<BuiltQuery> for (String, Vec<Value>) { ... }
   ```
2. Migrer progressivement les `build()` pour retourner `BuiltQuery`.
3. Implémenter `From<BuiltQuery> for (String, Vec<Value>)` pour la backward compat.

---

### 2.5 — `fetch_one()` et `fetch_optional()`

**Problème :** Pas de méthode pour récupérer exactement 1 row ou 0-1 row. L'utilisateur doit faire `fetch().first()` manuellement.

**Fichier concerné :**
- `reify-core/src/db.rs`

**Plan d'action :**
```rust
/// Fetch exactly one row. Returns error if 0 or 2+ rows.
pub async fn fetch_one<M: Table + FromRow>(
    db: &impl Database,
    builder: &SelectBuilder<M>,
) -> Result<M, DbError> {
    let rows = fetch(db, builder).await?;
    match rows.len() {
        1 => Ok(rows.into_iter().next().unwrap()),
        0 => Err(DbError::Query("expected 1 row, got 0".into())),
        n => Err(DbError::Query(format!("expected 1 row, got {n}"))),
    }
}

/// Fetch 0 or 1 row. Returns error if 2+ rows.
pub async fn fetch_optional<M: Table + FromRow>(
    db: &impl Database,
    builder: &SelectBuilder<M>,
) -> Result<Option<M>, DbError> { ... }
```

---

## Phase 3 — Architecture & Refactoring

> 🟡 **Améliore la maintenabilité long-terme.**  
> Estimation : 5-7 jours de travail.

### 3.1 — Éclater les god files

**Problème :** Plusieurs fichiers dépassent 700 lignes avec des responsabilités multiples.

**Plan d'action :**

#### `migration.rs` (2 376L) → module `migration/`
```
reify-core/src/migration/
├── mod.rs          — re-exports publics
├── error.rs        — MigrationError
├── context.rs      — MigrationContext
├── traits.rs       — Migration trait
├── plan.rs         — MigrationPlan, MigrationStatus
├── diff.rs         — SchemaDiff, TableDiff, ColumnDiff, DbColumnInfo, normalize_sql_type
├── ddl.rs          — create_table_sql, create_table_sql_with_checks, create_table_sql_named
├── runner.rs       — MigrationRunner
└── codegen.rs      — generate_migration_file, generate_view_migration_file
```

#### `query.rs` (1 133L) → module `query/`
```
reify-core/src/query/
├── mod.rs          — re-exports, Dialect, OnConflict, Expr, Order, count_all()
├── select.rs       — SelectBuilder
├── insert.rs       — InsertBuilder, InsertManyBuilder
├── update.rs       — UpdateBuilder
├── delete.rs       — DeleteBuilder
├── join.rs         — JoinedSelectBuilder, JoinClause, JoinKind
└── with.rs         — WithBuilder (eager loading)
```

#### `paginate.rs` (941L) → module `paginate/`
```
reify-core/src/paginate/
├── mod.rs          — re-exports
├── offset.rs       — Page, Paginated
├── cursor.rs       — CursorPaginated, CursorDirection
└── cursor_multi.rs — CursorBuilder (multi-column cursor)
```

#### `reify-macros/src/lib.rs` (1 299L) → modules
```
reify-macros/src/
├── lib.rs          — proc_macro entry points only
├── table.rs        — impl_table, parse_table_attr, parse_column_attrs
├── relations.rs    — impl_relations, parse_relations_attr
├── db_enum.rs      — impl_db_enum
├── view.rs         — impl_view (si existant)
└── util.rs         — unwrap_option_type, rust_type_to_sql_type, parse_sql_type_string
```

---

### 3.2 — Unifier `SelectBuilder` et `JoinedSelectBuilder`

**Problème :** `JoinedSelectBuilder` duplique `filter()`, `order_by()`, `limit()`, `offset()` en déléguant à un `SelectBuilder` interne. Le code `build_ast()` est dupliqué.

**Fichier concerné :**
- `reify-core/src/query.rs` — lignes 677-806

**Plan d'action :**
1. Ajouter un champ `joins: Vec<JoinClause>` directement dans `SelectBuilder`.
2. Les méthodes `join()`, `left_join()`, `right_join()` retournent `Self` au lieu de `JoinedSelectBuilder`.
3. `build_ast()` gère les joins nativement.
4. Supprimer `JoinedSelectBuilder` entièrement.
5. Mettre à jour les tests et la doc.

---

### 3.3 — Restructurer `Condition` enum

**Problème :** 30+ variantes dans un seul enum, mélangeant opérateurs universels, PG-only, agrégats, et raw SQL.

**Fichier concerné :**
- `reify-core/src/condition.rs`

**Plan d'action :**
```rust
#[derive(Debug, Clone)]
pub enum Condition {
    // Opérateurs universels
    Eq(&'static str, Value),
    Neq(&'static str, Value),
    Gt(&'static str, Value),
    Lt(&'static str, Value),
    Gte(&'static str, Value),
    Lte(&'static str, Value),
    Between(&'static str, Value, Value),
    Like(&'static str, String),
    In(&'static str, Vec<Value>),
    IsNull(&'static str),
    IsNotNull(&'static str),
    InSubquery(&'static str, String, Vec<Value>),
    
    // Logique
    Logical(LogicalOp),
    
    // Agrégats (HAVING)
    Aggregate(AggregateCondition),
    
    // PostgreSQL-specific
    #[cfg(feature = "postgres")]
    Postgres(PgCondition),
    
    // Escape hatch
    Raw(String, Vec<Value>),
}

#[derive(Debug, Clone)]
pub enum AggregateCondition {
    Gt(Expr, Value),
    Lt(Expr, Value),
    Gte(Expr, Value),
    Lte(Expr, Value),
    Eq(Expr, Value),
}

#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub enum PgCondition {
    ILike(&'static str, String),
    RangeContains(&'static str, Value),
    RangeContainedBy(&'static str, Value),
    RangeOverlaps(&'static str, Value),
    RangeLeftOf(&'static str, Value),
    RangeRightOf(&'static str, Value),
    RangeAdjacent(&'static str, Value),
    RangeIsEmpty(&'static str),
    JsonContains(&'static str, Value),
    JsonHasKey(&'static str, String),
    ArrayContains(&'static str, Value),
    ArrayContainedBy(&'static str, Value),
    ArrayOverlaps(&'static str, Value),
}
```

**Impact :** Modifier `sql.rs` (ToSql impl) pour dispatcher sur les sous-enums.

---

### 3.4 — Corriger `JsonGet` — ce n'est pas une Condition

**Problème :** `Condition::JsonGet` représente `column->key`, qui retourne une valeur, pas un booléen. Ce n'est pas un filtre WHERE valide seul.

**Fichier concerné :**
- `reify-core/src/condition.rs` — ligne 43-44
- `reify-core/src/column.rs` — ligne 268-270

**Plan d'action :**
1. Supprimer `Condition::JsonGet`.
2. Ajouter dans `Expr` :
   ```rust
   /// JSONB field access: `column->key` or `column->>key`.
   JsonAccess(&'static str, String),
   JsonAccessText(&'static str, String),
   ```
3. Modifier `Column<M, serde_json::Value>::json_get()` pour retourner un `JsonExpr` qui expose `.eq()`, `.contains()`, etc.
4. Ou plus simplement, transformer en `json_field_eq(key, value)` qui génère `column->>'key' = ?`.

---

### 3.5 — Supprimer le code mort

**Fichiers concernés :**
- `reify-core/src/hooks.rs` — `NoHooks` struct (ligne 34) : déclaré, jamais utilisé
- `reify-core/src/query.rs` — `count_all()` (ligne 179) : dupliqué avec `func.rs:33`
- `reify-core/src/lib.rs` — `count_all` exporté deux fois (ligne 26 via query, et implicitement via func)

**Plan d'action :**
1. Supprimer `NoHooks` de `hooks.rs`.
2. Supprimer `count_all()` de `query.rs`, garder uniquement celui de `func.rs`.
3. Mettre à jour l'export dans `lib.rs`.

---

## Phase 4 — Features & Complétude

> 🟢 **Différenciateurs pour la v0.2+.**  
> Estimation : 10-15 jours de travail.

### 4.1 — Soft Delete automatique

**Concept :**
```rust
#[derive(Table)]
#[table(name = "users", soft_delete = "deleted_at")]
pub struct User {
    pub id: i64,
    pub email: String,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}
```

**Comportement :**
- `User::find()` ajoute automatiquement `WHERE deleted_at IS NULL`
- `User::find().with_deleted()` désactive le filtre
- `User::find().only_deleted()` filtre `WHERE deleted_at IS NOT NULL`
- `User::delete().filter(...)` génère `UPDATE users SET deleted_at = NOW() WHERE ...`
- `User::force_delete().filter(...)` génère un vrai `DELETE`

**Fichiers à modifier :**
- `reify-macros/src/lib.rs` — parser `soft_delete` dans `#[table(...)]`
- `reify-core/src/query.rs` — `SelectBuilder::new()` injecte le filtre si soft_delete
- `reify-core/src/query.rs` — `DeleteBuilder` redirige vers UPDATE si soft_delete
- `reify-core/src/table.rs` — ajouter `fn soft_delete_column() -> Option<&'static str>`

---

### 4.2 — `SELECT DISTINCT`

**Fichier concerné :**
- `reify-core/src/query.rs` — `SelectBuilder`

**Plan d'action :**
1. Ajouter un champ `distinct: bool` à `SelectBuilder`.
2. Ajouter `.distinct()` :
   ```rust
   pub fn distinct(mut self) -> Self {
       self.distinct = true;
       self
   }
   ```
3. Modifier `build_ast()` pour émettre `SELECT DISTINCT ...`.
4. Ajouter `distinct: bool` à `SqlFragment::Select`.
5. Modifier `SqlFragment::render()` pour émettre `SELECT DISTINCT` quand `true`.

---

### 4.3 — Query duration logging

**Fichier concerné :**
- `reify-core/src/db.rs` — free functions `fetch()`, `insert()`, `update()`, `delete()`

**Plan d'action :**
1. Wrapper chaque appel DB avec un timer :
   ```rust
   pub async fn fetch<M: Table + FromRow>(
       db: &impl Database,
       builder: &SelectBuilder<M>,
   ) -> Result<Vec<M>, DbError> {
       let (sql, params) = builder.build();
       let start = std::time::Instant::now();
       let rows = db.query(&sql, &params).await?;
       let duration = start.elapsed();
       tracing::debug!(
           target: "reify::query",
           table = M::table_name(),
           rows = rows.len(),
           duration_ms = duration.as_millis(),
           sql = %sql,
           "Query executed"
       );
       if duration.as_millis() > 100 {
           tracing::warn!(
               target: "reify::slow_query",
               table = M::table_name(),
               duration_ms = duration.as_millis(),
               sql = %sql,
               "Slow query detected"
           );
       }
       rows.iter().map(|r| M::from_row(r)).collect()
   }
   ```
2. Rendre le seuil configurable via une constante ou un `thread_local`.

---

### 4.4 — Batch INSERT chunking automatique

**Problème :** Un `INSERT ... VALUES (...), (...), ...` avec 10 000 rows et 10 colonnes = 100 000 paramètres. PostgreSQL limite à 65 535.

**Fichier concerné :**
- `reify-core/src/db.rs` — `insert_many()`

**Plan d'action :**
1. Ajouter une constante `MAX_PARAMS_PER_QUERY: usize = 60_000` (marge de sécurité).
2. Dans `insert_many()`, calculer `params_per_row = M::writable_column_names().len()`.
3. Si `total_params > MAX_PARAMS_PER_QUERY`, chunker automatiquement :
   ```rust
   let chunk_size = MAX_PARAMS_PER_QUERY / params_per_row;
   let mut total_affected = 0u64;
   for chunk in models.chunks(chunk_size) {
       let builder = InsertManyBuilder::new(chunk);
       let (sql, params) = builder.build();
       total_affected += db.execute(&sql, &params).await?;
   }
   Ok(total_affected)
   ```

---

### 4.5 — `after_update` et `after_delete` dans ModelHooks

**Fichier concerné :**
- `reify-core/src/hooks.rs`

**Plan d'action :**
```rust
pub trait ModelHooks {
    fn before_insert(&mut self) {}
    fn after_insert(&self) {}
    fn before_update(&mut self) {}
    fn after_update(&self) {}      // ← AJOUTER
    fn before_delete(&self) {}
    fn after_delete(&self) {}      // ← AJOUTER
}
```

Mettre à jour `update_with_hooks()` et `delete_with_hooks()` dans `db.rs` pour appeler les nouveaux hooks.

---

### 4.6 — Support des Enums PostgreSQL natifs

**Concept :**
```rust
#[derive(DbEnum)]
#[db_enum(postgres_type = "user_role")]  // CREATE TYPE user_role AS ENUM (...)
pub enum Role {
    Admin,
    Member,
    Guest,
}
```

**Plan d'action :**
1. Ajouter un attribut `postgres_type` dans `#[derive(DbEnum)]`.
2. Générer une méthode `fn create_type_sql() -> String` sur l'enum.
3. Intégrer dans le `MigrationRunner` : détecter les enums PG et générer `CREATE TYPE` avant `CREATE TABLE`.
4. Modifier `IntoValue` pour les enums PG : utiliser un `Value::PgEnum(type_name, variant)` au lieu de `Value::String`.

---

### 4.7 — Subqueries dans FROM

**Concept :**
```rust
let subquery = User::find()
    .filter(User::active.eq(true))
    .select(&["id", "email"]);

let (sql, params) = SelectBuilder::<()>::from_subquery(subquery, "active_users")
    .filter(...)
    .build();
// SELECT * FROM (SELECT id, email FROM users WHERE active = ?) AS active_users WHERE ...
```

---

### 4.8 — Raw SQL avec typage

**Concept :**
```rust
let users: Vec<User> = reify::raw_fetch::<User>(
    &db,
    "SELECT * FROM users WHERE id = ?",
    &[Value::I64(1)],
).await?;
```

Ceci existe déjà (`raw_fetch` dans `db.rs`), mais nécessite `FromRow` manuel. Avec `#[derive(FromRow)]` (§2.1), ça devient utilisable.

---

## Phase 5 — Performance & Optimisation

> ⚡ **Optimisations pour la production à grande échelle.**  
> Estimation : 3-5 jours de travail.

### 5.1 — Cache de réécriture des placeholders PostgreSQL

**Problème :** `rewrite_placeholders_pg()` est appelé à chaque exécution de requête dans l'adapter PG. Pour des requêtes identiques, c'est du travail redondant.

**Fichier concerné :**
- `reify-postgres/src/lib.rs`

**Plan d'action :**
1. **Option A :** Déplacer la réécriture dans `build()` quand le dialecte est PG. Le builder sait déjà le dialecte.
2. **Option B :** Cache LRU dans `PostgresDb` : `HashMap<u64, String>` avec hash du SQL source.
3. **Option C (recommandée) :** Faire la réécriture dans `build_with_dialect(Dialect::Postgres)` directement. Zéro coût à l'exécution.

---

### 5.2 — `Row::get()` avec index pré-calculé

**Problème :** `Row::get(column_name)` fait une recherche linéaire O(n) à chaque appel.

**Fichier concerné :**
- `reify-core/src/db.rs` — lignes 8-40

**Plan d'action :**
1. Ajouter un `HashMap<String, usize>` pré-calculé dans `Row` :
   ```rust
   pub struct Row {
       columns: Vec<String>,
       values: Vec<Value>,
       index: HashMap<String, usize>,  // pré-calculé dans new()
   }
   ```
2. Ou utiliser un `Arc<Vec<String>>` partagé entre toutes les rows d'un même résultat (les noms de colonnes sont identiques).

---

### 5.3 — Prepared statements

**Problème :** Chaque requête est parsée et planifiée par la DB à chaque exécution.

**Fichiers concernés :**
- `reify-postgres/src/lib.rs`
- `reify-mysql/src/lib.rs`

**Plan d'action :**
1. Pour PostgreSQL : `tokio-postgres` prépare automatiquement les statements via `query()` (pas besoin de `prepare()` explicite). Vérifier que c'est bien le cas.
2. Pour MySQL : `mysql_async` supporte les prepared statements. Utiliser `conn.prep()` + `conn.exec()` au lieu de `conn.query()`.
3. Documenter le comportement dans chaque adapter.

---

### 5.4 — Réduire les allocations dans les builders

**Problème :** `build()` clone les `Value` dans `params`. Pour des requêtes avec beaucoup de paramètres, c'est coûteux.

**Plan d'action :**
1. Utiliser `Cow<'_, Value>` ou des références quand possible.
2. Pour `InsertManyBuilder`, éviter le `flat_map(|r| r.iter().cloned())` — utiliser un itérateur qui référence directement les rows.
3. Profiler avec `cargo flamegraph` pour identifier les hot spots réels.

---

## Phase 6 — Cohérence & Polish

> 🔵 **Qualité de code et conventions.**  
> Estimation : 2-3 jours de travail.

### 6.1 — Renommer `between_times` → `between`

**Fichier :** `reify-core/src/column.rs` — ligne 195

Le suffixe `_times` est inutile — le type-system différencie déjà. Renommer en `between()` pour cohérence avec les opérateurs numériques.

**Note :** Ceci crée un conflit de nom avec `Numeric::between()`. Solution : les deux impls sont sur des types différents (`Column<M, T: Temporal>` vs `Column<M, T: Numeric>`), donc pas de conflit réel en Rust. Mais vérifier que le compilateur ne se plaint pas.

---

### 6.2 — Supprimer `count_all()` dupliqué

**Fichiers :**
- Supprimer `reify-core/src/query.rs:179` (`pub fn count_all()`)
- Garder `reify-core/src/func.rs:33` (`pub fn count_all()`)
- Mettre à jour `reify-core/src/lib.rs` : exporter depuis `func` uniquement

---

### 6.3 — Ajouter `.gitignore` pour `.idea/`

**Fichier :** `.gitignore`

```gitignore
# IDE
.idea/
*.iml
```

Puis `git rm -r --cached .idea/` pour supprimer du tracking.

---

### 6.4 — Ajouter `#![deny(missing_docs)]` sur `reify-core`

**Fichier :** `reify-core/src/lib.rs`

```rust
#![deny(missing_docs)]
```

Forcer la documentation de toutes les APIs publiques. Ajouter les doc comments manquants progressivement.

---

### 6.5 — Uniformiser `&str` / `&'static str` / `String`

**Règle :**
- Noms de colonnes/tables venant des macros : `&'static str`
- Noms dynamiques (migrations, raw SQL) : `&str` ou `Cow<'static, str>`
- SQL généré : `String`

**Fichiers à auditer :**
- `condition.rs` — OK (`&'static str`)
- `migration.rs` — `MigrationContext` utilise `&str` → OK (dynamique)
- `sql.rs` — `JoinFragment` utilise `String` → pourrait être `Cow<'static, str>`

---

### 6.6 — Ajouter `Debug` sur tous les builders

**Fichiers :**
- `reify-core/src/query.rs` — `InsertBuilder`, `InsertManyBuilder`, `JoinedSelectBuilder`, `WithBuilder`

Ajouter `#[derive(Debug)]` ou impl manuelle (les `PhantomData` et `Value` supportent déjà `Debug`).

---

### 6.7 — Documenter le comportement de `DynDatabase::transaction`

**Fichier :** `reify-core/src/db.rs` — ligne 219-235

Ajouter un `#[doc]` warning clair :
```rust
/// ⚠️ **Warning:** Transactions are NOT supported through `&dyn DynDatabase`.
/// This implementation always returns an error. Use the concrete database
/// type directly for transaction support.
```

---

## Phase 7 — Écosystème & Outillage

> 🛠️ **Pour la maturité du projet.**  
> Estimation : 5-8 jours de travail.

### 7.1 — CLI fonctionnelle

**Problème :** La CLI actuelle ne fait rien — elle affiche des instructions.

**Plan d'action :**
1. **Option A (simple) :** La CLI lit un fichier de config (`reify.toml`) avec la connection string et exécute les migrations.
2. **Option B (avancée) :** La CLI compile le projet utilisateur et exécute le `MigrationRunner` configuré.
3. **Minimum viable :** `reify new` fonctionne déjà. Ajouter `reify status` qui se connecte à la DB et lit la table `_reify_migrations`.

---

### 7.2 — Tests d'intégration avec vraie DB

**Problème :** Aucun test ne touche une vraie base de données. Tous les tests sont sur le SQL généré.

**Plan d'action :**
1. Ajouter un `docker-compose.yml` avec PostgreSQL + MySQL.
2. Créer `reify/tests/integration/` avec :
   - `pg_basic.rs` — CRUD complet sur PostgreSQL
   - `pg_migrations.rs` — migration runner sur PostgreSQL
   - `mysql_basic.rs` — CRUD complet sur MySQL
   - `sqlite_basic.rs` — CRUD complet sur SQLite (in-memory, pas besoin de Docker)
3. Gater derrière un feature flag `integration-tests` ou une variable d'env `DATABASE_URL`.

---

### 7.3 — Benchmarks

**Plan d'action :**
1. Ajouter `benches/` avec `criterion` :
   - `build_select.rs` — benchmark de `SelectBuilder::build()` avec 0, 5, 20 conditions
   - `build_insert_many.rs` — benchmark de `InsertManyBuilder::build()` avec 1, 100, 10000 rows
   - `rewrite_placeholders.rs` — benchmark de `rewrite_placeholders_pg()`
   - `from_row.rs` — benchmark de `FromRow` avec 5, 20, 50 colonnes

---

### 7.4 — CI/CD

**Plan d'action :**
1. GitHub Actions workflow :
   ```yaml
   - cargo check (all features)
   - cargo test (default features)
   - cargo test --features postgres
   - cargo test --features mysql
   - cargo clippy -- -D warnings
   - cargo doc --no-deps
   ```
2. Ajouter un badge dans le README.

---

### 7.5 — Documentation utilisateur

**Plan d'action :**
1. `README.md` avec :
   - Quick start (5 lignes de code)
   - Feature matrix (quel backend supporte quoi)
   - Comparison avec Diesel, SeaORM, SQLx
2. `docs/` avec :
   - `getting-started.md`
   - `query-builder.md`
   - `migrations.md`
   - `relations.md`
   - `pagination.md`
   - `rls.md`

---

## Annexe — Points forts à préserver

> Ces éléments sont excellents et ne doivent pas être dégradés par les refactorings.

| Aspect | Détail | Fichier(s) |
|--------|--------|------------|
| **Column type safety** | `Column<M, T>` avec méthodes type-gated est le cœur du projet. Brillant. | `column.rs` |
| **Panic-safe UPDATE/DELETE** | Interdire les UPDATE/DELETE sans WHERE est un excellent choix de design. | `query.rs` |
| **AST-based SQL generation** | `SqlFragment` permet la manipulation structurée (pagination, count). | `sql.rs` |
| **Cursor pagination multi-colonnes** | Support complet avec row-value comparisons. Rare dans les ORMs. | `paginate.rs` |
| **Zero-alloc `write_joined`** | Évite `collect::<Vec<String>>().join()`. Bon réflexe perf. | `sql.rs` |
| **`Cow<'static, str>` pour SqlType** | Zero-alloc pour les types fixes, owned pour les paramétrés. | `schema.rs` |
| **Tracing intégré** | `tracing::debug!` sur chaque query built. Prêt pour la prod. | `query.rs` |
| **Audit trail** | `#[table(audit)]` génère automatiquement une table d'audit. Unique. | `audit.rs`, `macros` |
| **RLS applicatif** | `Policy` trait + `Scoped` wrapper. Concept solide (fix le bug §1.3). | `rls.rs` |
| **DbEnum** | `#[derive(DbEnum)]` avec rename par variante. Propre et utile. | `enumeration.rs`, `macros` |

---

## Résumé des estimations

| Phase | Effort | Priorité | Impact |
|-------|--------|----------|--------|
| **Phase 1** — Sécurité | 3-5 jours | 🔴 Critique | Bloquant prod |
| **Phase 2** — Ergonomie | 5-8 jours | 🟠 Haute | Adoption |
| **Phase 3** — Architecture | 5-7 jours | 🟡 Moyenne | Maintenabilité |
| **Phase 4** — Features | 10-15 jours | 🟢 Normale | Différenciation |
| **Phase 5** — Performance | 3-5 jours | ⚡ Normale | Scalabilité |
| **Phase 6** — Cohérence | 2-3 jours | 🔵 Basse | Qualité |
| **Phase 7** — Écosystème | 5-8 jours | 🛠️ Basse | Maturité |
| **Total** | **33-51 jours** | | |

---

> **Prochaine étape recommandée :** Commencer par la Phase 1 (sécurité), puis enchaîner avec §2.1 (`#[derive(FromRow)]`) qui débloque toute l'ergonomie.
