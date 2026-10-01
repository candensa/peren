#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Alarm {
    pub at_ms: i64,
    pub retry: i64,
    pub counted_retry: i64,
    pub generation: i64,
}
