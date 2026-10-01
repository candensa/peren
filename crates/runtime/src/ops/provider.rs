use std::{cell::RefCell, rc::Rc};

use deno_core::{OpState, op2};

use super::context::{ai, cache, host_error, kv, queue, r2};
use crate::wire::{
    AiRun, CacheEntry, CacheGet, CachePut, KvGet, KvList, KvPut, ListPage, QueueSend, R2Delete,
    R2Get, R2List, R2ListPage, R2Object, R2Put,
};

#[op2(async(lazy))]
pub async fn op_r2_put(
    state: Rc<RefCell<OpState>>,
    #[serde] object: R2Put,
) -> Result<(), deno_error::JsErrorBox> {
    r2(&state).host().put(object).await.map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_r2_get(
    state: Rc<RefCell<OpState>>,
    #[serde] object: R2Get,
) -> Result<Option<R2Object>, deno_error::JsErrorBox> {
    r2(&state).host().get(object).await.map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_r2_delete(
    state: Rc<RefCell<OpState>>,
    #[serde] object: R2Delete,
) -> Result<(), deno_error::JsErrorBox> {
    r2(&state).host().delete(object).await.map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_r2_list(
    state: Rc<RefCell<OpState>>,
    #[serde] request: R2List,
) -> Result<R2ListPage, deno_error::JsErrorBox> {
    r2(&state).host().list(request).await.map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_cache_match(
    state: Rc<RefCell<OpState>>,
    #[serde] request: CacheGet,
) -> Result<Option<CacheEntry>, deno_error::JsErrorBox> {
    cache(&state)
        .host()
        .match_entry(request)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_cache_put(
    state: Rc<RefCell<OpState>>,
    #[serde] entry: CachePut,
) -> Result<(), deno_error::JsErrorBox> {
    cache(&state)
        .host()
        .put_entry(entry)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_cache_delete(
    state: Rc<RefCell<OpState>>,
    #[serde] request: CacheGet,
) -> Result<bool, deno_error::JsErrorBox> {
    cache(&state)
        .host()
        .delete_entry(request)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_kv_get(
    state: Rc<RefCell<OpState>>,
    #[serde] request: KvGet,
) -> Result<Option<Vec<u8>>, deno_error::JsErrorBox> {
    kv(&state).host().get(request).await.map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_kv_put(
    state: Rc<RefCell<OpState>>,
    #[serde] request: KvPut,
) -> Result<(), deno_error::JsErrorBox> {
    kv(&state).host().put(request).await.map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_kv_delete(
    state: Rc<RefCell<OpState>>,
    #[serde] request: KvGet,
) -> Result<bool, deno_error::JsErrorBox> {
    kv(&state).host().delete(request).await.map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_kv_list(
    state: Rc<RefCell<OpState>>,
    #[serde] request: KvList,
) -> Result<ListPage, deno_error::JsErrorBox> {
    kv(&state).host().list(request).await.map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_ai_run(
    state: Rc<RefCell<OpState>>,
    #[serde] request: AiRun,
) -> Result<serde_json::Value, deno_error::JsErrorBox> {
    ai(&state).host().run(request).await.map_err(host_error)
}

#[op2(async(lazy))]
pub async fn op_queue_send(
    state: Rc<RefCell<OpState>>,
    #[serde] message: QueueSend,
) -> Result<(), deno_error::JsErrorBox> {
    queue(&state).host().send(message).await.map_err(host_error)
}
