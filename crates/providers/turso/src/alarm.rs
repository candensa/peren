use peren_storage::{Alarm, AlarmStore};
use turso::params;

use crate::{SessionState, TursoStore};

impl AlarmStore for TursoStore {
    async fn alarm(&mut self, scope: &str) -> Result<Option<Alarm>, Self::Error> {
        self.ensure_reconciled()?;
        let mut rows = self
            .connection
            .query(
                "SELECT at_ms,retry,counted_retry,generation FROM alarms WHERE scope=?1",
                params![scope],
            )
            .await?;
        Ok(match rows.next().await? {
            Some(row) => Some(Alarm {
                at_ms: row.get(0)?,
                retry: row.get(1)?,
                counted_retry: row.get(2)?,
                generation: row.get(3)?,
            }),
            None => None,
        })
    }

    async fn set_alarm(&mut self, scope: &str, at_ms: i64) -> Result<(), Self::Error> {
        self.ensure_reconciled()?;
        self.require_session()?;
        self.connection.execute("INSERT INTO alarms(scope,at_ms,retry,counted_retry,generation) VALUES(?1,?2,0,0,0) ON CONFLICT(scope) DO UPDATE SET at_ms=excluded.at_ms,retry=0,counted_retry=0,generation=alarms.generation+1",params![scope,at_ms]).await?;
        self.session = SessionState::Open { dirty: true };
        Ok(())
    }

    async fn retry_alarm(&mut self, scope: &str, generation: i64) -> Result<bool, Self::Error> {
        self.ensure_reconciled()?;
        let dirty = self.require_session()?;
        let changed = self.connection.execute("UPDATE alarms SET retry=retry+1,counted_retry=counted_retry+1 WHERE scope=?1 AND generation=?2",params![scope,generation]).await? > 0;
        self.session = SessionState::Open {
            dirty: dirty || changed,
        };
        Ok(changed)
    }

    async fn delete_alarm(&mut self, scope: &str) -> Result<bool, Self::Error> {
        self.ensure_reconciled()?;
        let dirty = self.require_session()?;
        let changed = self
            .connection
            .execute("DELETE FROM alarms WHERE scope=?1", params![scope])
            .await?
            > 0;
        self.session = SessionState::Open {
            dirty: dirty || changed,
        };
        Ok(changed)
    }
}
