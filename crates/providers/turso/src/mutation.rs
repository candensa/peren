use peren_storage::{MutationRecord, MutationStore};
use turso::params;
use uuid::Uuid;

use crate::{SessionState, TursoError, TursoStore};

impl MutationStore for TursoStore {
    async fn mutation_outcome(&mut self, id: Uuid) -> Result<Option<MutationRecord>, Self::Error> {
        self.ensure_reconciled()?;
        let mut rows = self
            .connection
            .query(
                "SELECT outcome,revision FROM mutation_outcomes WHERE id=?1",
                params![id.to_string()],
            )
            .await?;
        Ok(match rows.next().await? {
            Some(row) => {
                let revision =
                    u64::try_from(row.get::<i64>(1)?).map_err(|_| TursoError::InvalidRevision)?;
                Some(MutationRecord {
                    id,
                    outcome: row.get(0)?,
                    revision: peren_primitives::StorageRevision::new(revision),
                })
            }
            None => None,
        })
    }

    async fn record_mutation_outcome(
        &mut self,
        id: Uuid,
        outcome: &[u8],
    ) -> Result<(), Self::Error> {
        self.ensure_reconciled()?;
        self.require_session()?;
        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO mutation_outcomes(id,outcome,revision) VALUES(?1,?2,?3)",
                params![
                    id.to_string(),
                    outcome,
                    i64::try_from(self.revision.get()).map_err(|_| TursoError::RevisionOverflow)?
                ],
            )
            .await?;
        if inserted == 0 {
            return Err(TursoError::DuplicateMutation(id));
        }
        self.session = SessionState::Open { dirty: true };
        Ok(())
    }
}
