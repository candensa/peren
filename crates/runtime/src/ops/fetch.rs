use std::{cell::RefCell, rc::Rc};

use deno_core::{OpState, op2};

use super::context::{durable, fetch, host_error, service};
use crate::{
    HttpRequest, HttpResponse,
    wire::{AwsSigv4Fetch, DurableObjectFetch, ServiceFetch},
};

#[op2(async(lazy))]
#[serde]
pub async fn op_outbound_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] request: HttpRequest,
) -> Result<HttpResponse, deno_error::JsErrorBox> {
    fetch(&state)
        .host()
        .fetch(request)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_aws_sigv4_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] request: AwsSigv4Fetch,
) -> Result<HttpResponse, deno_error::JsErrorBox> {
    fetch(&state)
        .host()
        .fetch_aws(request)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_service_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] request: ServiceFetch,
) -> Result<HttpResponse, deno_error::JsErrorBox> {
    service(&state)
        .host()
        .fetch(request)
        .await
        .map_err(host_error)
}

#[op2(async(lazy))]
#[serde]
pub async fn op_durable_object_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DurableObjectFetch,
) -> Result<HttpResponse, deno_error::JsErrorBox> {
    durable(&state)
        .host()
        .fetch(request)
        .await
        .map_err(host_error)
}
