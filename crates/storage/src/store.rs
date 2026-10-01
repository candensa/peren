use crate::{Alarm, Committed, ListOptions, ListPage, MutationRecord, SqlResult, SqlValue};

pub trait CellStore: Send {
    type Error;
    fn begin(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn load(
        &mut self,
        scope: &str,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;
    fn put(
        &mut self,
        scope: &str,
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete(
        &mut self,
        scope: &str,
        key: &[u8],
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
    fn commit(&mut self) -> impl Future<Output = Result<Committed<()>, Self::Error>> + Send;
    fn rollback(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait ListStore: CellStore {
    fn list(
        &mut self,
        scope: &str,
        options: &ListOptions<'_>,
    ) -> impl Future<Output = Result<ListPage, Self::Error>> + Send;
}

pub trait SqlStore: CellStore {
    fn sql(
        &mut self,
        query: &str,
        parameters: &[SqlValue],
    ) -> impl Future<Output = Result<SqlResult, Self::Error>> + Send;
}

pub trait AlarmStore: CellStore {
    fn alarm(
        &mut self,
        scope: &str,
    ) -> impl Future<Output = Result<Option<Alarm>, Self::Error>> + Send;
    fn set_alarm(
        &mut self,
        scope: &str,
        at_ms: i64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn retry_alarm(
        &mut self,
        scope: &str,
        generation: i64,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
    fn delete_alarm(
        &mut self,
        scope: &str,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

pub trait MutationStore: CellStore {
    fn mutation_outcome(
        &mut self,
        id: uuid::Uuid,
    ) -> impl Future<Output = Result<Option<MutationRecord>, Self::Error>> + Send;
    fn record_mutation_outcome(
        &mut self,
        id: uuid::Uuid,
        outcome: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait AttachmentStore: CellStore {
    fn attachment(
        &mut self,
        id: &str,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;
    fn set_attachment(
        &mut self,
        id: &str,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete_attachment(
        &mut self,
        id: &str,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}
