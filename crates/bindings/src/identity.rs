use hmac::{Hmac, Mac};
use peren_primitives::CellId;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const PAYLOAD_BYTES: usize = 24;
const TAG_BYTES: usize = 8;
const PAYLOAD_DOMAIN: &[u8] = b"peren-do-id-payload-v1";
const MEMBERSHIP_DOMAIN: &[u8] = b"peren-do-id-membership-v1";
const KV_CLASS: &str = "__peren_kv";
const D1_CLASS: &str = "__peren_d1";
const FACET_CLASS: &str = "__peren_facet";

#[must_use]
pub fn derive_cell_id(unique_key: &str, class_name: &str, object_name: &str) -> CellId {
    derive(unique_key.as_bytes(), class_name, object_name)
}

#[must_use]
pub fn new_unique_cell_id(unique_key: &str, class_name: &str) -> CellId {
    let mut payload = [0; PAYLOAD_BYTES];
    payload[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    payload[16..].copy_from_slice(&uuid::Uuid::new_v4().as_bytes()[..8]);
    assemble(
        payload,
        membership_tag(unique_key.as_bytes(), class_name, &payload),
    )
}

#[must_use]
pub fn verify_cell_id_membership(unique_key: &str, class_name: &str, id: &CellId) -> bool {
    let bytes = id.as_bytes();
    membership_mac(unique_key.as_bytes(), class_name, &bytes[..PAYLOAD_BYTES])
        .verify_truncated_left(&bytes[PAYLOAD_BYTES..])
        .is_ok()
}

#[must_use]
pub fn kv_cell_id(unique_key: &str, namespace: &str) -> CellId {
    derive_cell_id(unique_key, KV_CLASS, namespace)
}

#[must_use]
pub fn d1_cell_id(unique_key: &str, database_name: &str) -> CellId {
    derive_cell_id(unique_key, D1_CLASS, database_name)
}

#[must_use]
pub fn facet_cell_id(parent: &CellId, facet_name: &str) -> CellId {
    derive(parent.as_bytes(), FACET_CLASS, facet_name)
}

fn derive(key: &[u8], class_name: &str, object_name: &str) -> CellId {
    let mut mac = mac(key);
    mac.update(PAYLOAD_DOMAIN);
    update_field(&mut mac, class_name.as_bytes());
    update_field(&mut mac, object_name.as_bytes());
    let digest = mac.finalize().into_bytes();
    let mut payload = [0; PAYLOAD_BYTES];
    payload.copy_from_slice(&digest[..PAYLOAD_BYTES]);
    assemble(payload, membership_tag(key, class_name, &payload))
}

fn membership_tag(key: &[u8], class_name: &str, payload: &[u8]) -> [u8; TAG_BYTES] {
    let digest = membership_mac(key, class_name, payload)
        .finalize()
        .into_bytes();
    let mut tag = [0; TAG_BYTES];
    tag.copy_from_slice(&digest[..TAG_BYTES]);
    tag
}

fn membership_mac(key: &[u8], class_name: &str, payload: &[u8]) -> HmacSha256 {
    let mut mac = mac(key);
    mac.update(MEMBERSHIP_DOMAIN);
    update_field(&mut mac, class_name.as_bytes());
    mac.update(payload);
    mac
}

fn mac(key: &[u8]) -> HmacSha256 {
    HmacSha256::new_from_slice(key).unwrap_or_else(|_| unreachable!("HMAC accepts any key length"))
}

fn update_field(mac: &mut HmacSha256, value: &[u8]) {
    let length = u32::try_from(value.len()).expect("identity field length exceeds u32");
    mac.update(&length.to_be_bytes());
    mac.update(value);
}

fn assemble(payload: [u8; PAYLOAD_BYTES], tag: [u8; TAG_BYTES]) -> CellId {
    let mut bytes = [0; 32];
    bytes[..PAYLOAD_BYTES].copy_from_slice(&payload);
    bytes[PAYLOAD_BYTES..].copy_from_slice(&tag);
    CellId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_deterministic_and_namespace_bound() {
        let id = derive_cell_id("application-key", "Room", "lobby");
        assert_eq!(
            id.to_string(),
            "f6a99f09a2e65b1c6b3a0831bc32aec12e08fe73deecc7282089fa3861c24b5e"
        );
        assert_eq!(id, derive_cell_id("application-key", "Room", "lobby"));
        assert!(verify_cell_id_membership("application-key", "Room", &id));
        assert!(!verify_cell_id_membership("other-key", "Room", &id));
        assert!(!verify_cell_id_membership("application-key", "Other", &id));
    }

    #[test]
    fn field_boundaries_and_reserved_namespaces_do_not_alias() {
        assert_ne!(
            derive_cell_id("key", "class", "AB"),
            derive_cell_id("key", "classA", "B")
        );
        assert_ne!(kv_cell_id("key", "data"), d1_cell_id("key", "data"));
    }

    #[test]
    fn random_and_facet_ids_keep_membership_tags() {
        let parent = new_unique_cell_id("key", "Room");
        assert!(verify_cell_id_membership("key", "Room", &parent));
        assert_ne!(
            facet_cell_id(&parent, "presence"),
            facet_cell_id(&parent, "metadata")
        );
    }
}
