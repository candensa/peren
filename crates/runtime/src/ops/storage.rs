use std::{cell::RefCell, rc::Rc};

use deno_core::{OpState, op2};

use super::context::{host_error, storage};
use crate::wire::{ListOptions, ListPage, SqlQuery, SqlResult};

#[op2(async(lazy), fast)]
pub async fn op_storage_begin(state: Rc<RefCell<OpState>>) -> Result<(), deno_error::JsErrorBox> {
    storage(&state).host().begin().await.map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_storage_get(
    state: Rc<RefCell<OpState>>,
    #[string] scope: String,
    #[serde] key: Vec<u8>,
) -> Result<Option<Vec<u8>>, deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .load(&scope, &key)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_storage_put(
    state: Rc<RefCell<OpState>>,
    #[string] scope: String,
    #[serde] key: Vec<u8>,
    #[serde] value: Vec<u8>,
) -> Result<(), deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .put(&scope, &key, &value)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_storage_delete(
    state: Rc<RefCell<OpState>>,
    #[string] scope: String,
    #[serde] key: Vec<u8>,
) -> Result<(), deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .delete(&scope, &key)
        .await
        .map(|_| ())
        .map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_storage_list(
    state: Rc<RefCell<OpState>>,
    #[string] scope: String,
    #[serde] options: ListOptions,
) -> Result<ListPage, deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .list(&scope, options)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_storage_sql(
    state: Rc<RefCell<OpState>>,
    #[serde] query: SqlQuery,
) -> Result<SqlResult, deno_error::JsErrorBox> {
    storage(&state).host().sql(query).await.map_err(host_error)
}

#[op2(async(lazy), fast)]
#[serde]
pub async fn op_storage_mutation_outcome_get(
    state: Rc<RefCell<OpState>>,
    #[string] id: String,
) -> Result<Option<Vec<u8>>, deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .mutation_outcome(&id)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_storage_mutation_outcome_record(
    state: Rc<RefCell<OpState>>,
    #[string] id: String,
    #[serde] outcome: Vec<u8>,
) -> Result<(), deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .record_mutation_outcome(&id, &outcome)
        .await
        .map_err(host_error)
}

#[op2(async(lazy), fast)]
#[serde]
pub async fn op_storage_commit(state: Rc<RefCell<OpState>>) -> Result<u64, deno_error::JsErrorBox> {
    let storage = storage(&state);
    let revision = storage.host().commit().await.map_err(host_error)?;
    storage.record(revision);
    Ok(revision.get())
}

#[op2(async(lazy), fast)]
pub async fn op_storage_rollback(
    state: Rc<RefCell<OpState>>,
) -> Result<(), deno_error::JsErrorBox> {
    storage(&state).host().rollback().await.map_err(host_error)
}

#[op2(async(lazy), fast)]
#[serde]
pub async fn op_ws_attachment_get(
    state: Rc<RefCell<OpState>>,
    #[string] id: String,
) -> Result<Option<Vec<u8>>, deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .attachment(&id)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_ws_attachment_set(
    state: Rc<RefCell<OpState>>,
    #[string] id: String,
    #[serde] bytes: Vec<u8>,
) -> Result<(), deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .set_attachment(&id, &bytes)
        .await
        .map_err(host_error)
}

#[op2(async(lazy), fast)]
pub async fn op_ws_attachment_delete(
    state: Rc<RefCell<OpState>>,
    #[string] id: String,
) -> Result<(), deno_error::JsErrorBox> {
    storage(&state)
        .host()
        .delete_attachment(&id)
        .await
        .map(|_| ())
        .map_err(host_error)
}
