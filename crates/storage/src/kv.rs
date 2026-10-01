use crate::StorageError;
use rusqlite::{Connection, OptionalExtension, params};

pub const MAX_ENTRY_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_LIST_ENTRIES: usize = 1_000;
pub type KvEntries = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Clone, Debug)]
pub struct ListOptions<'a> {
    pub prefix: Option<&'a [u8]>,
    pub cursor: Option<&'a [u8]>,
    pub start: Option<&'a [u8]>,
    pub start_after: Option<&'a [u8]>,
    pub end: Option<&'a [u8]>,
    pub limit: usize,
    pub reverse: bool,
}

impl Default for ListOptions<'_> {
    fn default() -> Self {
        Self {
            prefix: None,
            cursor: None,
            start: None,
            start_after: None,
            end: None,
            limit: MAX_LIST_ENTRIES,
            reverse: false,
        }
    }
}

impl ListOptions<'_> {
    pub fn page_size(&self) -> Result<usize, InvalidListLimit> {
        (1..=MAX_LIST_ENTRIES)
            .contains(&self.limit)
            .then_some(self.limit)
            .ok_or(InvalidListLimit(self.limit))
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("list limit {0} is outside 1..={MAX_LIST_ENTRIES}")]
pub struct InvalidListLimit(pub usize);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPage {
    pub entries: KvEntries,
    pub next_cursor: Option<Vec<u8>>,
}

pub(crate) fn validate(key: &[u8], value: &[u8]) -> Result<(), StorageError> {
    let actual = key.len().saturating_add(value.len());
    (actual <= MAX_ENTRY_BYTES)
        .then_some(())
        .ok_or(StorageError::EntryTooLarge {
            actual,
            limit: MAX_ENTRY_BYTES,
        })
}

pub(crate) fn get(
    conn: &Connection,
    scope: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StorageError> {
    conn.query_row(
        "SELECT v FROM kv WHERE scope=?1 AND k=?2",
        params![scope, key],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn list(
    conn: &Connection,
    scope: &str,
    options: &ListOptions<'_>,
) -> Result<ListPage, StorageError> {
    let limit = options.page_size()?;
    let lower = options.prefix.unwrap_or_default().to_vec();
    let upper = options.prefix.and_then(prefix_end);
    let sql = if options.reverse {
        "SELECT k,v FROM kv WHERE scope=?1 AND k>=?2 AND (?3 IS NULL OR k<?3) AND (?4 IS NULL OR k<?4) AND (?5 IS NULL OR k>=?5) AND (?6 IS NULL OR k>?6) AND (?7 IS NULL OR k<?7) ORDER BY k DESC LIMIT ?8"
    } else {
        "SELECT k,v FROM kv WHERE scope=?1 AND k>=?2 AND (?3 IS NULL OR k<?3) AND (?4 IS NULL OR k>?4) AND (?5 IS NULL OR k>=?5) AND (?6 IS NULL OR k>?6) AND (?7 IS NULL OR k<?7) ORDER BY k ASC LIMIT ?8"
    };
    let fetch = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
    let mut statement = conn.prepare_cached(sql)?;
    let rows = statement.query_map(
        params![
            scope,
            lower,
            upper,
            options.cursor,
            options.start,
            options.start_after,
            options.end,
            fetch
        ],
        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
    )?;
    let mut entries = rows.collect::<Result<Vec<_>, _>>()?;
    let next_cursor = if entries.len() > limit {
        entries.truncate(limit);
        entries.last().map(|entry| entry.0.clone())
    } else {
        None
    };
    Ok(ListPage {
        entries,
        next_cursor,
    })
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.last_mut() {
        if *byte != u8::MAX {
            *byte += 1;
            return Some(end);
        }
        end.pop();
    }
    None
}
