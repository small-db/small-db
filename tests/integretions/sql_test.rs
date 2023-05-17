use small_db::sql::sql_handler::handle_sql;

use crate::test_utils::setup;

#[test]
fn test_sql() {
    setup();

    let sql = "
        CREATE TABLE foo (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255)
        );
    ";

    // handle_sql(sql);
}
