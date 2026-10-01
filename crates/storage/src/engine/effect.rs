use peren_primitives::StorageRevision;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

use crate::{EffectRecord, EffectStatus, StorageError};

use super::CellStorage;

impl CellStorage {
    pub fn claim_effects(
        &mut self,
        now_ms: i64,
        lease_token: &str,
        lease_until_ms: i64,
        limit: usize,
    ) -> Result<Vec<EffectRecord>, StorageError> {
        let sqlite = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ids = {
            let mut statement = sqlite.prepare(
                "SELECT id FROM effects WHERE (status='pending' AND due_at_ms<=?1) OR (status='leased' AND leased_until_ms<=?1) ORDER BY due_at_ms,id LIMIT ?2",
            )?;
            statement
                .query_map(
                    params![
                        now_ms,
                        i64::try_from(limit).map_err(|_| StorageError::RevisionOverflow)?
                    ],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        for id in &ids {
            sqlite.execute(
                "UPDATE effects SET status='leased', attempts=attempts+1, lease_token=?1, leased_until_ms=?2 WHERE id=?3",
                params![lease_token, lease_until_ms, id],
            )?;
        }
        let effects = ids
            .iter()
            .map(|id| effect_by_id(&sqlite, id))
            .collect::<Result<Vec<_>, _>>()?;
        sqlite.commit()?;
        Ok(effects)
    }

    pub fn acknowledge_effect(
        &mut self,
        id: Uuid,
        lease_token: &str,
    ) -> Result<bool, StorageError> {
        let changed = self.connection.execute(
            "UPDATE effects SET status='acknowledged', lease_token=NULL, leased_until_ms=NULL WHERE id=?1 AND status='leased' AND lease_token=?2",
            params![id.to_string(), lease_token],
        )?;
        Ok(changed == 1)
    }

    pub fn retry_effect(
        &mut self,
        id: Uuid,
        lease_token: &str,
        due_at_ms: i64,
    ) -> Result<bool, StorageError> {
        let changed = self.connection.execute(
            "UPDATE effects SET status='pending', due_at_ms=?1, lease_token=NULL, leased_until_ms=NULL WHERE id=?2 AND status='leased' AND lease_token=?3",
            params![due_at_ms, id.to_string(), lease_token],
        )?;
        Ok(changed == 1)
    }

    pub fn effect(&self, id: Uuid) -> Result<Option<EffectRecord>, StorageError> {
        effect_by_id(&self.connection, &id.to_string())
            .optional()
            .map_err(Into::into)
    }
}

fn effect_by_id(connection: &Connection, id: &str) -> Result<EffectRecord, rusqlite::Error> {
    connection.query_row(
        "SELECT id,destination,inbox_key,payload,status,attempts,due_at_ms,lease_token,leased_until_ms,revision FROM effects WHERE id=?1",
        [id],
        |row| {
            let id_text: String = row.get(0)?;
            let revision = u64::try_from(row.get::<_, i64>(9)?)
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(9, 0))?;
            let attempts = u32::try_from(row.get::<_, i64>(5)?)
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, 0))?;
            let status_text: String = row.get(4)?;
            let status = EffectStatus::parse(&status_text).map_err(|_| rusqlite::Error::InvalidQuery)?;
            Ok(EffectRecord {
                id: Uuid::parse_str(&id_text).map_err(|_| rusqlite::Error::InvalidQuery)?,
                destination: row.get(1)?,
                inbox_key: row.get(2)?,
                payload: row.get(3)?,
                status,
                attempts,
                due_at_ms: row.get(6)?,
                lease_token: row.get(7)?,
                leased_until_ms: row.get(8)?,
                revision: StorageRevision::new(revision),
            })
        },
    )
}
