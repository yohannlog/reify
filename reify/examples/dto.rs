use reify::Table;

#[derive(Table, Debug, Clone)]
#[table(name = "users")]
pub struct User {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    #[column(unique)]
    pub email: String,
    pub name: String,
    pub age: Option<i32>,
}

fn main() {
    // UserDto is auto-generated: it excludes `id` (primary_key + auto_increment)
    let dto = UserDto {
        email: "alice@example.com".into(),
        name: "Alice".into(),
        age: Some(30),
    };

    println!("DTO: {dto:?}");
    println!("Columns: {:?}", UserDto::column_names());
    println!("Values: {:?}", dto.into_values());
}
