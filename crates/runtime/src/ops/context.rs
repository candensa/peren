use std::{cell::RefCell, rc::Rc};

use deno_core::OpState;

use crate::{
    InvocationAi, InvocationCache, InvocationDurableObject, InvocationFetch, InvocationKv,
    InvocationQueue, InvocationR2, InvocationService, InvocationStorage,
};

pub(super) fn storage(state: &Rc<RefCell<OpState>>) -> InvocationStorage {
    state.borrow().borrow::<InvocationStorage>().clone()
}

pub(super) fn fetch(state: &Rc<RefCell<OpState>>) -> InvocationFetch {
    state.borrow().borrow::<InvocationFetch>().clone()
}

pub(super) fn queue(state: &Rc<RefCell<OpState>>) -> InvocationQueue {
    state.borrow().borrow::<InvocationQueue>().clone()
}

pub(super) fn service(state: &Rc<RefCell<OpState>>) -> InvocationService {
    state.borrow().borrow::<InvocationService>().clone()
}

pub(super) fn durable(state: &Rc<RefCell<OpState>>) -> InvocationDurableObject {
    state.borrow().borrow::<InvocationDurableObject>().clone()
}

pub(super) fn r2(state: &Rc<RefCell<OpState>>) -> InvocationR2 {
    state.borrow().borrow::<InvocationR2>().clone()
}

pub(super) fn cache(state: &Rc<RefCell<OpState>>) -> InvocationCache {
    state.borrow().borrow::<InvocationCache>().clone()
}

pub(super) fn kv(state: &Rc<RefCell<OpState>>) -> InvocationKv {
    state.borrow().borrow::<InvocationKv>().clone()
}

pub(super) fn ai(state: &Rc<RefCell<OpState>>) -> InvocationAi {
    state.borrow().borrow::<InvocationAi>().clone()
}

pub(super) fn host_error(error: crate::HostError) -> deno_error::JsErrorBox {
    deno_error::JsErrorBox::generic(error.to_string())
}
