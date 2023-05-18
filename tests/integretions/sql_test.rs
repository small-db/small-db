use crate::test_utils::setup;

#[test]
fn test_sql() {
    setup();

    let _sql = "
        CREATE TABLE foo (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255)
        );
    ";

    // handle_sql(sql);
}
