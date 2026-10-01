use super::*;
use std::path::Path;

#[test]
fn sql_vector_distance_orders_json_vectors() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    storage
        .sql(
            "CREATE TABLE vectors(id TEXT PRIMARY KEY, embedding TEXT NOT NULL)",
            &[],
        )
        .unwrap();
    storage
        .sql(
            "INSERT INTO vectors(id, embedding) VALUES ('far', '[10,0]'), ('near', '[1,0]')",
            &[],
        )
        .unwrap();
    let result = storage
        .sql("SELECT id, vector_distance(embedding, '[0,0]') AS distance FROM vectors ORDER BY distance LIMIT 1", &[])
        .unwrap()
        .value;

    assert_eq!(result.columns, vec!["id", "distance"]);
    assert_eq!(result.rows[0][0], SqlValue::Text("near".into()));
    assert_eq!(result.rows[0][1], SqlValue::Real(1.0));
}

#[test]
fn sql_vector_distance_rejects_dimension_mismatch() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    let result = storage.sql("SELECT vector_distance('[1,2]', '[1]')", &[]);

    assert!(matches!(result, Err(StorageError::Sql { .. })));
}
