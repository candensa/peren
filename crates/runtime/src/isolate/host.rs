use super::{Hosts, peren_storage, peren_web};
use crate::{
    InvocationAi, InvocationCache, InvocationDurableObject, InvocationFetch, InvocationKv,
    InvocationQueue, InvocationR2, InvocationService, InvocationStorage,
    empty::{NoAi, NoCache, NoDurableObject, NoFetch, NoKv, NoQueue, NoR2, NoService, NoStorage},
};
use deno_core::Extension;
use std::sync::Arc;

mod embedded_deno_sources {
    include!(concat!(env!("OUT_DIR"), "/embedded_deno_sources.rs"));
}

pub(crate) struct InvocationHosts {
    pub(super) storage: InvocationStorage,
    pub(super) fetch: InvocationFetch,
    pub(super) queue: InvocationQueue,
    pub(super) r2: InvocationR2,
    pub(super) service: InvocationService,
    pub(super) durable: InvocationDurableObject,
    pub(super) cache: InvocationCache,
    pub(super) kv: InvocationKv,
    pub(super) ai: InvocationAi,
}

impl InvocationHosts {
    pub(crate) fn empty() -> Self {
        Self::from_hosts(Hosts::default())
    }

    pub(crate) fn from_capabilities(capabilities: crate::Capabilities) -> Self {
        Self {
            storage: InvocationStorage::new(capabilities.storage),
            fetch: capabilities.fetch.map_or_else(
                || InvocationFetch::new(Arc::new(NoFetch)),
                InvocationFetch::new,
            ),
            queue: capabilities.queue.map_or_else(
                || InvocationQueue::new(Arc::new(NoQueue)),
                InvocationQueue::new,
            ),
            r2: capabilities
                .r2
                .map_or_else(|| InvocationR2::new(Arc::new(NoR2)), InvocationR2::new),
            service: capabilities.service.map_or_else(
                || InvocationService::new(Arc::new(NoService)),
                InvocationService::new,
            ),
            durable: capabilities.durable.map_or_else(
                || InvocationDurableObject::new(Arc::new(NoDurableObject)),
                InvocationDurableObject::new,
            ),
            cache: capabilities.cache.map_or_else(
                || InvocationCache::new(Arc::new(NoCache)),
                InvocationCache::new,
            ),
            kv: capabilities
                .kv
                .map_or_else(|| InvocationKv::new(Arc::new(NoKv)), InvocationKv::new),
            ai: capabilities
                .ai
                .map_or_else(|| InvocationAi::new(Arc::new(NoAi)), InvocationAi::new),
        }
    }

    pub(super) fn from_hosts(hosts: Hosts) -> Self {
        Self {
            storage: hosts.storage.map_or_else(
                || InvocationStorage::new(Arc::new(NoStorage)),
                InvocationStorage::new,
            ),
            fetch: hosts.fetch.map_or_else(
                || InvocationFetch::new(Arc::new(NoFetch)),
                InvocationFetch::new,
            ),
            queue: hosts.queue.map_or_else(
                || InvocationQueue::new(Arc::new(NoQueue)),
                InvocationQueue::new,
            ),
            r2: hosts
                .r2
                .map_or_else(|| InvocationR2::new(Arc::new(NoR2)), InvocationR2::new),
            service: hosts.service.map_or_else(
                || InvocationService::new(Arc::new(NoService)),
                InvocationService::new,
            ),
            durable: hosts.durable.map_or_else(
                || InvocationDurableObject::new(Arc::new(NoDurableObject)),
                InvocationDurableObject::new,
            ),
            cache: hosts.cache.map_or_else(
                || InvocationCache::new(Arc::new(NoCache)),
                InvocationCache::new,
            ),
            kv: hosts
                .kv
                .map_or_else(|| InvocationKv::new(Arc::new(NoKv)), InvocationKv::new),
            ai: hosts
                .ai
                .map_or_else(|| InvocationAi::new(Arc::new(NoAi)), InvocationAi::new),
        }
    }
}

pub(crate) fn web_extensions(hosts: InvocationHosts) -> Vec<Extension> {
    let mut extensions = vec![
        peren_storage::init(hosts),
        deno_webidl::deno_webidl::init(),
        deno_web::deno_web::init(
            Arc::new(deno_web::BlobStore::default()),
            None,
            false,
            deno_web::InMemoryBroadcastChannel::default(),
        ),
        deno_net::deno_net::init(None, None),
        deno_fetch::deno_fetch::init(deno_fetch::Options::default()),
        peren_web::init(),
    ];
    for extension in &mut extensions {
        embedded_deno_sources::embed(extension);
    }
    extensions
}
