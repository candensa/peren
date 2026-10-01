use peren_storage::{AttachmentStore, MAX_ATTACHMENT_BYTES};
use turso::params;

use crate::{SessionState, TursoError, TursoStore};

impl AttachmentStore for TursoStore {
    async fn attachment(&mut self, id: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        self.ensure_reconciled()?;
        let mut rows = self
            .connection
            .query(
                "SELECT bytes FROM ws_attachments WHERE connection_id=?1",
                params![id],
            )
            .await?;
        Ok(match rows.next().await? {
            Some(row) => Some(row.get(0)?),
            None => None,
        })
    }

    async fn set_attachment(&mut self, id: &str, bytes: &[u8]) -> Result<(), Self::Error> {
        self.ensure_reconciled()?;
        self.require_session()?;
        if bytes.len() > MAX_ATTACHMENT_BYTES {
            return Err(TursoError::AttachmentTooLarge {
                actual: bytes.len(),
                limit: MAX_ATTACHMENT_BYTES,
            });
        }
        self.connection.execute("INSERT INTO ws_attachments(connection_id,bytes) VALUES(?1,?2) ON CONFLICT(connection_id) DO UPDATE SET bytes=excluded.bytes",params![id,bytes]).await?;
        self.session = SessionState::Open { dirty: true };
        Ok(())
    }

    async fn delete_attachment(&mut self, id: &str) -> Result<bool, Self::Error> {
        self.ensure_reconciled()?;
        let dirty = self.require_session()?;
        let changed = self
            .connection
            .execute(
                "DELETE FROM ws_attachments WHERE connection_id=?1",
                params![id],
            )
            .await?
            > 0;
        self.session = SessionState::Open {
            dirty: dirty || changed,
        };
        Ok(changed)
    }
}
