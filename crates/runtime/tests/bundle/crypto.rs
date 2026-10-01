use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn node_crypto_covers_hash_hmac_random_and_safe_compare() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import crypto, { KeyObject, createHash, createHmac, createSecretKey, getHashes, randomBytes, randomUUID, timingSafeEqual, webcrypto } from "node:crypto";
            export default {
              fetch() {
                const hash = createHash('sha256').update('hel').update('lo').digest('hex');
                const hmac = createHmac('sha256', 'secret').update('hello').digest('hex');
                const key = createSecretKey(new TextEncoder().encode('secret'));
                const objectHmac = createHmac('sha256', key).update('hello').digest('hex');
                const exported = key.export().toString('utf8');
                const refused = (() => { try { key.export({ format: 'jwk' }); return false; } catch { return true; } })();
                const random = randomBytes(8);
                const uuid = randomUUID();
                let lengthRefused = false;
                try { timingSafeEqual(new Uint8Array([1]), new Uint8Array([1, 2])); } catch { lengthRefused = true; }
                return new Response(JSON.stringify({
                  defaultHash: typeof crypto.createHash,
                  defaultSecretKey: typeof crypto.createSecretKey,
                  hash,
                  hmac,
                  objectHmac,
                  keyObject: key instanceof KeyObject,
                  keyType: key.type,
                  keySize: key.symmetricKeySize,
                  exported,
                  refused,
                  hashes: getHashes(),
                  random: random.length,
                  uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(uuid),
                  equal: timingSafeEqual(new Uint8Array([1, 2]), new Uint8Array([1, 2])),
                  different: timingSafeEqual(new Uint8Array([1, 2]), new Uint8Array([1, 3])),
                  lengthRefused,
                  webcrypto: webcrypto === globalThis.crypto,
                }));
              }
            };
        "#,
            ),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/node-crypto".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "defaultHash": "function",
            "defaultSecretKey": "function",
            "hash": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "hmac": "88aab3ede8d3adf94d26ab90d3bafd4a2083070c3bcce9c014ee04a443847c0b",
            "objectHmac": "88aab3ede8d3adf94d26ab90d3bafd4a2083070c3bcce9c014ee04a443847c0b",
            "keyObject": true,
            "keyType": "secret",
            "keySize": 6,
            "exported": "secret",
            "refused": true,
            "hashes": ["sha256", "sha384", "sha512"],
            "random": 8,
            "uuid": true,
            "equal": true,
            "different": false,
            "lengthRefused": true,
            "webcrypto": true,
        })
    );
}
