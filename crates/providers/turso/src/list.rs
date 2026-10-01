use peren_storage::{ListOptions, ListPage, ListStore};
use turso::params;

use crate::TursoStore;

impl ListStore for TursoStore {
    async fn list(
        &mut self,
        scope: &str,
        options: &ListOptions<'_>,
    ) -> Result<ListPage, Self::Error> {
        self.ensure_reconciled()?;
        let limit = options.page_size()?;
        let lower = options.prefix.unwrap_or_default().to_vec();
        let upper = options.prefix.and_then(prefix_end);
        let query = if options.reverse {
            "SELECT k,v FROM kv WHERE scope=?1 AND k>=?2 AND (?3 IS NULL OR k<?3) AND (?4 IS NULL OR k<?4) AND (?5 IS NULL OR k>=?5) AND (?6 IS NULL OR k>?6) AND (?7 IS NULL OR k<?7) ORDER BY k DESC LIMIT ?8"
        } else {
            "SELECT k,v FROM kv WHERE scope=?1 AND k>=?2 AND (?3 IS NULL OR k<?3) AND (?4 IS NULL OR k>?4) AND (?5 IS NULL OR k>=?5) AND (?6 IS NULL OR k>?6) AND (?7 IS NULL OR k<?7) ORDER BY k ASC LIMIT ?8"
        };
        let fetch = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let mut rows = self
            .connection
            .query(
                query,
                params![
                    scope,
                    lower,
                    upper,
                    options.cursor.map(<[u8]>::to_vec),
                    options.start.map(<[u8]>::to_vec),
                    options.start_after.map(<[u8]>::to_vec),
                    options.end.map(<[u8]>::to_vec),
                    fetch
                ],
            )
            .await?;
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(limit);
        while let Some(row) = rows.next().await? {
            entries.push((row.get(0)?, row.get(1)?));
        }
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
