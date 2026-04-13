[NEWS]

C. Drizzle Studio / Reify Studio Drizzle fournit une interface web générée à la volée pour explorer sa base.
L'idée pour Reify : Une commande reify studio. Puisque tes structs Rust sont le schéma, la CLI pourrait compiler un petit serveur web embarqué (via axum par ex) qui expose une interface d'admin CRUD complète basée sur tes #[derive(Table)]. C'est un argument de vente énorme.

D. Intégration de validation (TypeBox/Zod vs Serde/Validator) Drizzle permet de générer des schémas de validation directement depuis le schéma DB.
L'idée pour Reify : Générer automatiquement l'implémentation du crate validator (ou générer des types de requêtes) pour vérifier les longueurs de string, les nullabilités ou les regex avant même de taper la DB.

C. Soft Deletes "Invisibles" Hibernate gère les @Where(clause = "deleted = false").
L'idée pour Reify : Si un modèle a #[column(soft_delete)], tous les User::find() cachent automatiquement les enregistrements supprimés sans que le développeur n'ait à rajouter .filter(User::deleted_at.is_null()) partout. Il faudrait juste une méthode .with_deleted() pour contourner cette règle quand c'est nécessaire.

D. Génération de DTOs / Sérialisation sélective Souvent, on ne veut pas renvoyer le mot de passe hashé via l'API.
L'idée pour Reify : Des attributs comme #[column(hidden)] ou générer automatiquement des "Views" Rust (ex: User::select_public()) qui omettent les champs sensibles lors du select et de la sérialisation serde.

E. Feature pour générer des DTOs / Sérialisation sélective Souvent, on ne veut pas renvoyer le mot de passe hashé via l'API.
L'idée pour Reify : Des attributs comme #[column(hidden)] ou générer automatiquement des "Views" Rust (ex: User::select_public()) qui omettent les champs sensibles lors du select et de la sérialisation serde.

3. Valeurs par défaut (Default) et protection (Quoting)
   La méthode .default(impl Into<String>) accepte une chaîne. Cependant, si j'écris .default("member"), le générateur DDL va-t-il émettre DEFAULT member (SQL invalide) ou DEFAULT 'member' ? Si je veux mettre DEFAULT NOW(), comment faire la différence entre l'expression SQL et la chaîne de caractères ? Amélioration : Différencier les valeurs littérales des expressions SQL :``` rust
   pub enum DefaultValue {
   String(String), // Sera quoté : DEFAULT 'member'
   Integer(i64),   // DEFAULT 42
   Boolean(bool),  // DEFAULT true
   Expr(String),   // Ne sera pas quoté : DEFAULT NOW()
   }

// Sur le builder:
pub fn default_value(mut self, val: impl Into<DefaultValue>) -> Self
pub fn default_expr(mut self, expr: &str) -> Self
```

#### 4. Manque des Clés Étrangères (Foreign Keys)
Dans une base de données relationnelle, la définition des relations (FK) est primordiale pour générer les DDL. Il manque une façon de les définir proprement dans le builder actuel. **Amélioration** : Ajouter un `ForeignKeyDef` et une méthode sur le ou sur . `ColumnBuilder``TableSchema```` rust
// Sur ColumnBuilder
.column(Post::user_id, |c| c.references(User::id).on_delete_cascade())

// Ou sur TableSchema (pour les FK composites)
.foreign_key(|fk| fk.column(Post::user_id).references(User::id))
```



1. Redondance avec le typage Rust (Inférence)
   Actuellement, si le champ Rust est pub role: Option<String>, le développeur doit appeler .nullable() dans le builder :``` rust
   .column(User::role, |c| c.nullable())

Je pense qu'il serait intéressant d'avoir l'autocomplete ?
```

Amélioration : Puisque la macro #[derive(Table)] génère probablement les instances de Column<M, T>, vous pourriez enrichir la structure Column ou utiliser un trait pour que le nullability et le SqlType par défaut soient inférés à partir du type Rust (Option -> nullable = true, String -> SqlType::Text). Le builder de schéma ne servirait alors qu'à surcharger les comportements (index, contraintes, foreign key) et non à redéfinir la nullabilité.
## Noms possibles

Quelques propositions avec leur angle :

| Nom | Angle |
|---|---|
| **Forged** | "forgé" — solide, Rust, artisanal |
| **Bolted** | boulonné — robuste, structurel |
| **Moldé** | moulé depuis le code |
| **`crato`** | du grec *kratos* — force, contrôle |
| **`ormish`** | simple, mémorable |
| **`sculpt`** | tu sculptes ta DB depuis tes structs |
| **`reify`** | "rendre réel" — le code devient la DB |

Mon coup de cœur : **`reify`** — le nom dit exactement ce que la lib fait : *tes structs Rust deviennent ta base de données*. Court, mémorable, pas pris sur crates.io.

---

## Plan détaillé — `reify`

---

### Vision

> *"Définis ta base de données en Rust. Reify la rend réelle."*

Zéro CLI pour le schéma. Zéro string magique. Autocomplétion totale. Les migrations vivent dans ton code.

---

### Principes non négociables

**1. Autocomplétion 100% sur les macros**
Chaque colonne, chaque relation, chaque condition est un **type Rust réel** — jamais une string. Rust-analyzer autocompléte tout. Renommer un champ = erreur de compilation immédiate partout où il est utilisé.

**2. Le code est la source de vérité**
Pas de CLI pour générer les entités depuis la DB. Pas de `schema.rs` séparé. Ta struct *est* ta table.

**3. Migrations sans surprise**
Mode dry-run obligatoire en prod. Diff automatique pour les cas simples. Manuel typé pour les cas complexes. Toujours réversible ou explicitement marqué irréversible.

---

### Architecture des crates

```
reify/
├── reify-core/          # Traits, types, query builder, migration runner
├── reify-macros/        # Proc-macros : #[derive(Table)], #[relations(...)]
├── reify-sqlite/        # Adapter SQLite (rusqlite)
├── reify-postgres/      # Adapter PostgreSQL (tokio-postgres)
├── reify-mysql/         # Adapter MySQL (mysql_async)
└── reify-cli/           # CLI : reify migrate, reify new, reify status
```

L'utilisateur installe uniquement ce dont il a besoin :
```toml
[dependencies]
reify = { version = "0.1", features = ["sqlite"] }
# ou
reify = { version = "0.1", features = ["postgres", "async"] }
```

---

### Module 1 — `reify-macros` : autocomplétion totale

C'est le cœur du projet. **Toute la saisie utilisateur dans les attributs doit être du code Rust parseable par rust-analyzer**, jamais des strings littérales.

#### `#[derive(Table)]`

```rust
#[derive(Table, Debug, Clone)]
#[table(name = "users")]          // ← seule string tolérée : le nom SQL
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,

    #[column(unique, index)]
    pub email: String,

    #[column(nullable, default = "member")]
    pub role: Option<String>,

    #[column(nullable)]
    pub deleted_at: Option<chrono::DateTime<Utc>>,
}
```

**Ce que la macro génère — les colonnes comme constantes typées :**

```rust
impl User {
    // Chaque champ → une constante Column<Model, Type>
    // rust-analyzer les voit, les autocompléte, vérifie les types
    pub const id:         Column<User, i64>                       = Column::new("id");
    pub const email:      Column<User, String>                    = Column::new("email");
    pub const role:       Column<User, Option<String>>            = Column::new("role");
    pub const deleted_at: Column<User, Option<DateTime<Utc>>>     = Column::new("deleted_at");

    // Query builder typé
    pub fn find() -> SelectBuilder<User> { SelectBuilder::new() }
    pub fn insert(val: &User) -> InsertBuilder<User> { InsertBuilder::new(val) }
    pub fn update() -> UpdateBuilder<User> { UpdateBuilder::new() }
    pub fn delete() -> DeleteBuilder<User> { DeleteBuilder::new() }
}
```

**Règle absolue sur les attributs :**

| Attribut | ✅ Autocomplétion | ❌ Interdit |
|---|---|---|
| `foreign_key = Post::user_id` | Type Rust — autocomplété | `foreign_key = "user_id"` |
| `on = Post::user_id.eq(User::id)` | Expression Rust — vérifiée | `on = "post.user_id = user.id"` |
| `model = Post` | Type Rust | `model = "Post"` |
| `table(name = "users")` | String SQL tolérée | — |

#### `#[relations(...)]`

```rust
#[derive(Table)]
#[table(name = "users")]
#[relations(
    has_many(posts:    Post,    on = Post::user_id.eq(User::id)),
    has_one( profile:  Profile, on = Profile::user_id.eq(User::id)),
    belongs_to(team:  Team,    on = User::team_id.eq(Team::id)),
)]
pub struct User {
    pub id: i64,
    pub team_id: i64,
}
```

Tout ce qui est dans `#[relations(...)]` est parsé comme des **expressions Rust** via `syn::Expr` et `syn::Path`. Rust-analyzer résout `Post::user_id` comme le `const` généré par le `#[derive(Table)]` de `Post`.

**Ce que la macro génère :**

```rust
impl User {
    pub fn posts() -> RelationBuilder<User, Post> {
        RelationBuilder::new().on(Post::user_id.eq(User::id))
    }
    pub fn profile() -> RelationBuilder<User, Profile> {
        RelationBuilder::new().on(Profile::user_id.eq(User::id))
    }
    pub fn team() -> RelationBuilder<User, Team> {
        RelationBuilder::new().on(User::team_id.eq(Team::id))
    }
}
```

**Vérification à la compilation :**
```rust
// ✅ OK : Post::user_id est Column<Post, i64>, User::id est Column<User, i64>
on = Post::user_id.eq(User::id)

// ❌ Erreur de compilation : Column<Post, String> != Column<User, i64>
on = Post::title.eq(User::id)
```

---

### Module 2 — Query builder typé

```rust
// SELECT avec autocomplétion totale
let users = User::find()
    .select([User::id, User::email])           // ✅ colonnes autocomplétées
    .filter(User::email.ends_with("@corp.io")) // ✅ méthode String uniquement
    .filter(User::deleted_at.is_null())        // ✅ méthode Option uniquement
    .order_by(User::id.desc())
    .limit(10)
    .offset(20)
    .fetch(&db)?;

// JOIN typé
let results = User::find()
    .join(User::posts())                        // ✅ relation autocomplétée
    .filter(Post::title.contains("Rust"))       // ✅ Post accessible après join
    .fetch::<(User, Vec<Post>)>(&db)?;

// INSERT
User::insert(&User { id: 0, email: "alice@example.com".into(), .. })
    .execute(&db)?;

// UPDATE ciblé — jamais de UPDATE sans WHERE par défaut
User::update()
    .set(User::role, "admin")                  // ✅ type vérifié : role est String
    .filter(User::id.eq(42))
    .execute(&db)?;

// DELETE avec soft-delete automatique si deleted_at existe
User::delete()
    .filter(User::id.eq(42))
    .soft()                                    // → UPDATE SET deleted_at = NOW()
    .execute(&db)?;

// Transactions
db.transaction(|tx| {
    User::insert(&alice).execute(tx)?;
    Post::insert(&post).execute(tx)?;
    Ok(())
})?;
```

**Types des opérateurs par type de colonne :**

```rust
// Column<M, String> expose :
.eq("val")  .neq()  .like()  .contains()  .starts_with()  .ends_with()  .in_list()

// Column<M, i32/i64/f64> expose :
.eq(n)  .gt(n)  .lt(n)  .gte(n)  .lte(n)  .between(a, b)

// Column<M, Option<T>> expose en plus :
.is_null()  .is_not_null()

// Column<M, DateTime> expose :
.before(dt)  .after(dt)  .between(dt1, dt2)
```

---

### Module 3 — Migrations

#### 3a. Migration automatique par diff (cas simples)

```rust
MigrationRunner::new()
    .add_table::<User>()   // CREATE TABLE si n'existe pas
    .add_table::<Post>()   // ALTER TABLE si nouvelles colonnes détectées
    .run(&db)?;
```

Détecte automatiquement :
- Nouvelle table → `CREATE TABLE`
- Nouveau champ → `ALTER TABLE ADD COLUMN`

Ne touche **jamais** à : rename, drop, type change → délégué aux migrations manuelles.

#### 3b. Migration manuelle typée (cas complexes)

```rust
// Structure d'une migration manuelle
pub struct SplitAddress;

impl Migration for SplitAddress {
    fn version(&self) -> &'static str { "20240320_000001_split_address" }
    fn description(&self) -> &'static str { "Split address into city + country" }
    fn is_reversible(&self) -> bool { true }

    fn up(&self, ctx: &mut MigrationContext) -> Result<()> {
        ctx.add_column("users", col!(city: String))?;
        ctx.add_column("users", col!(country: String))?;
        ctx.each_row::<(i64, String)>(
            "SELECT id, address FROM users",
            |ctx, (id, address)| {
                let (city, country) = split_address(&address);
                ctx.execute_with(
                    "UPDATE users SET city=?, country=? WHERE id=?",
                    params![city, country, id],
                )
            },
        )?;
        ctx.drop_column("users", "address")
    }

    fn down(&self, ctx: &mut MigrationContext) -> Result<()> {
        ctx.add_column("users", col!(address: String))?;
        ctx.each_row::<(i64, String, String)>(
            "SELECT id, city, country FROM users",
            |ctx, (id, city, country)| {
                ctx.execute_with(
                    "UPDATE users SET address=? WHERE id=?",
                    params![format!("{}, {}", city, country), id],
                )
            },
        )?;
        ctx.drop_column("users", "city")?;
        ctx.drop_column("users", "country")
    }
}
```

#### 3c. Mode dry-run

```rust
MigrationRunner::new()
    .add_table::<User>()
    .add(SplitAddress)
    .dry_run(&db)?;

// Output :
// ┌─ DRY RUN — nothing will be written ──────────────────────┐
// │ ✓ Already applied : 20240101_000001_create_posts          │
// │ ~ Would apply     : 20240101_000002_create_users          │
// │     CREATE TABLE IF NOT EXISTS users (                    │
// │         id INTEGER PRIMARY KEY AUTOINCREMENT,             │
// │         email TEXT NOT NULL UNIQUE                        │
// │     );                                                    │
// │ ~ Would apply     : 20240320_000001_split_address         │
// │     ALTER TABLE users ADD COLUMN city TEXT NOT NULL       │
// │     ALTER TABLE users ADD COLUMN country TEXT NOT NULL    │
// │     <data migration — estimated 1 337 rows>               │
// │     ALTER TABLE users DROP COLUMN address                 │
// └───────────────────────────────────────────────────────────┘
```

---

### Module 4 — CLI `reify`

**Philosophie : beaucoup plus simple que `sea-orm-cli`.** La CLI ne génère pas le schéma (c'est le code qui le fait), elle gère uniquement le cycle de vie des migrations.

```bash
# Installer
cargo install reify-cli

# Initialiser un projet
reify init
# → Crée reify.toml + migrations/ + src/schema.rs

# Créer un fichier de migration manuelle pré-rempli
reify new rename_email_to_contact_email
# → Crée migrations/20240315_000001_rename_email_to_contact_email.rs
# → Avec le trait Migration pré-implémenté, prêt à remplir up() et down()

# Voir l'état des migrations
reify status
# ✓ Applied   20240101_000001_create_users
# ✓ Applied   20240101_000002_create_posts
# ~ Pending   20240315_000001_rename_email_to_contact_email

# Dry-run avant d'appliquer
reify migrate --dry-run

# Appliquer
reify migrate

# Rollback de la dernière
reify rollback

# Rollback jusqu'à une version précise
reify rollback --to 20240101_000001_create_users
```

**Contenu généré par `reify new` :**
```rust
// migrations/20240315_000001_rename_email_to_contact_email.rs
// Généré par : reify new rename_email_to_contact_email

use reify::Migration;

pub struct RenamEmailToContactEmail;

impl Migration for RenamEmailToContactEmail {
    fn version(&self) -> &'static str {
        "20240315_000001_rename_email_to_contact_email"
    }

    fn description(&self) -> &'static str {
        "Rename email to contact_email"
    }

    fn up(&self, ctx: &mut MigrationContext) -> Result<()> {
        todo!("implement up migration")
    }

    fn down(&self, ctx: &mut MigrationContext) -> Result<()> {
        todo!("implement down migration")
    }
}
```

Zéro magie, zéro config YAML, zéro DSL externe — juste du Rust.

---

### Module 5 — Support RLS (Row Level Security)

Conçu pour PostgreSQL en priorité, avec une API portable.

```rust
// Définir une politique RLS sur un modèle
#[derive(Table)]
#[table(name = "posts")]
#[rls(
    policy = "tenant_isolation",
    using  = Post::tenant_id.eq(CurrentUser::tenant_id),   // ← filtre SELECT/UPDATE/DELETE
    check  = Post::tenant_id.eq(CurrentUser::tenant_id),   // ← filtre INSERT
)]
pub struct Post {
    pub id:        i64,
    pub tenant_id: i64,
    pub title:     String,
}
```

**Activation dans le runner de migrations :**
```rust
MigrationRunner::new()
    .add_table::<Post>()           // CREATE TABLE
    .enable_rls::<Post>()          // ALTER TABLE posts ENABLE ROW LEVEL SECURITY
    .add_rls_policy::<Post>()      // CREATE POLICY tenant_isolation ON posts ...
    .run(&db)?;
```

**Utilisation avec contexte utilisateur :**
```rust
// Chaque connexion reçoit le contexte de l'utilisateur courant
let db = db.with_rls_context(RlsContext {
    user_id:   current_user.id,
    tenant_id: current_user.tenant_id,
    role:      current_user.role.clone(),
})?;

// Toutes les requêtes sont automatiquement filtrées par la politique
// Un user du tenant 42 ne peut jamais voir les posts du tenant 99
let posts = Post::find().fetch(&db)?;
//          ↑ WHERE tenant_id = 42 appliqué automatiquement par PostgreSQL
```

**Mode bypass pour les admins :**
```rust
// Passer outre le RLS pour les opérations admin (avec traçabilité)
db.bypass_rls(|db| {
    Post::find().fetch(db)   // Voit tous les tenants
})?;
```

---

### Roadmap

| Phase | Contenu | Priorité |
|---|---|---|
| **v0.1** | `#[derive(Table)]`, colonnes typées, SELECT/INSERT/UPDATE/DELETE, SQLite | Fondation |
| **v0.2** | `#[relations(...)]`, JOIN typés, transactions | Essentiel |
| **v0.3** | Migration runner, diff auto, dry-run, CLI `reify new/status/migrate` | Différenciateur |
| **v0.4** | Support PostgreSQL, RLS complet | Multi-db |
| **v0.5** | Support async (tokio), mode sync+async unifiés | Confort |
| **v0.6** | MySQL, soft-delete, pagination cursor, audit log | Complétude |

---

### Ce qui rend `reify` unique en une phrase par feature

- **Colonnes typées** → *le premier ORM Rust où renommer un champ casse la compilation partout*
- **Relations via expressions Rust** → *`Post::user_id` pas `"user_id"` — rust-analyzer le voit*
- **Migrations dans le code** → *tes structs génèrent le DDL, la CLI gère le cycle de vie*
- **Dry-run** → *vois exactement ce qui va changer avant de toucher la prod*
- **RLS intégré** → *le seul ORM Rust avec Row Level Security dans l'API*