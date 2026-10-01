use super::*;

#[tokio::test(flavor = "current_thread")]
async fn web_crypto_random_uuid_and_digest_are_available() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const random = new Uint8Array(16);
                const returned = crypto.getRandomValues(random);
                const encoder = new TextEncoder();
                const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode('hello')));
                const hex = [...digest].map((byte) => byte.toString(16).padStart(2, '0')).join('');
                const sha1 = new Uint8Array(await crypto.subtle.digest('SHA-1', encoder.encode('hello')));
                const sha1Hex = [...sha1].map((byte) => byte.toString(16).padStart(2, '0')).join('');
                const key = await crypto.subtle.importKey('raw', encoder.encode('secret'), { name: 'HMAC', hash: 'SHA-256' }, true, ['sign', 'verify']);
                const signature = new Uint8Array(await crypto.subtle.sign('HMAC', key, encoder.encode('hello')));
                const signatureHex = [...signature].map((byte) => byte.toString(16).padStart(2, '0')).join('');
                const verified = await crypto.subtle.verify('HMAC', key, signature, encoder.encode('hello'));
                const rejected = await crypto.subtle.verify('HMAC', key, signature, encoder.encode('hellO'));
                const exported = new Uint8Array(await crypto.subtle.exportKey('raw', key));
                const sha1Key = await crypto.subtle.importKey('raw', encoder.encode('secret'), { name: 'HMAC', hash: 'SHA-1' }, true, ['sign', 'verify']);
                const sha1Signature = new Uint8Array(await crypto.subtle.sign('HMAC', sha1Key, encoder.encode('hello')));
                const sha1SignatureHex = [...sha1Signature].map((byte) => byte.toString(16).padStart(2, '0')).join('');
                const sha1Verified = await crypto.subtle.verify('HMAC', sha1Key, sha1Signature, encoder.encode('hello'));
                const locked = await crypto.subtle.importKey('raw', encoder.encode('secret'), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
                let exportRefused = false;
                try { await crypto.subtle.exportKey('raw', locked); } catch { exportRefused = true; }
                const pair = await crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify']);
                const publicKey = new Uint8Array(await crypto.subtle.exportKey('raw', pair.publicKey));
                const imported = await crypto.subtle.importKey('raw', publicKey, 'Ed25519', true, ['verify']);
                const edSignature = new Uint8Array(await crypto.subtle.sign('Ed25519', pair.privateKey, encoder.encode('hello')));
                const edVerified = await crypto.subtle.verify('Ed25519', imported, edSignature, encoder.encode('hello'));
                const edRejected = await crypto.subtle.verify('Ed25519', imported, edSignature, encoder.encode('hellO'));
                let privateExportRefused = false;
                try { await crypto.subtle.exportKey('raw', pair.privateKey); } catch { privateExportRefused = true; }
                let unsupported = false;
                try { await crypto.subtle.digest('MD5', new Uint8Array()); } catch { unsupported = true; }
                return new Response(JSON.stringify({
                  same: returned === random,
                  randomLength: random.length,
                  uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(crypto.randomUUID()),
                  hex,
                  sha1Hex,
                  unsupported,
                  key: typeof CryptoKey,
                  keyType: key.type,
                  keyAlgorithm: key.algorithm.name,
                  keyHash: key.algorithm.hash.name,
                  signatureHex,
                  verified,
                  rejected,
                  sha1SignatureHex,
                  sha1Verified,
                  exported: new TextDecoder().decode(exported),
                  exportRefused,
                  edPublicLength: publicKey.length,
                  edPrivateType: pair.privateKey.type,
                  edPublicType: imported.type,
                  edSignatureLength: edSignature.length,
                  edVerified,
                  edRejected,
                  privateExportRefused,
                }));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/crypto"), InvocationLimits::new(4096, 10))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "same": true,
            "randomLength": 16,
            "uuid": true,
            "hex": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "sha1Hex": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
            "unsupported": true,
            "key": "function",
            "keyType": "secret",
            "keyAlgorithm": "HMAC",
            "keyHash": "SHA-256",
            "signatureHex": "88aab3ede8d3adf94d26ab90d3bafd4a2083070c3bcce9c014ee04a443847c0b",
            "verified": true,
            "rejected": false,
            "sha1SignatureHex": "5112055c05f944f85755efc5cd8970e194e9f45b",
            "sha1Verified": true,
            "exported": "secret",
            "exportRefused": true,
            "edPublicLength": 32,
            "edPrivateType": "private",
            "edPublicType": "public",
            "edSignatureLength": 64,
            "edVerified": true,
            "edRejected": false,
            "privateExportRefused": true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn web_crypto_ecdsa_p256_raw_public_keys_are_available() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const encoder = new TextEncoder();
                const pair = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign', 'verify']);
                const publicKey = new Uint8Array(await crypto.subtle.exportKey('raw', pair.publicKey));
                const imported = await crypto.subtle.importKey('raw', publicKey, { name: 'ECDSA', namedCurve: 'P-256' }, true, ['verify']);
                const signature = new Uint8Array(await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, pair.privateKey, encoder.encode('hello')));
                const verified = await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, imported, signature, encoder.encode('hello'));
                const rejected = await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, imported, signature, encoder.encode('hellO'));
                let privateExportRefused = false;
                try { await crypto.subtle.exportKey('raw', pair.privateKey); } catch { privateExportRefused = true; }
                return new Response(JSON.stringify({
                  publicLength: publicKey.length,
                  privateType: pair.privateKey.type,
                  publicType: imported.type,
                  signatureLength: signature.length,
                  verified,
                  rejected,
                  privateExportRefused,
                }));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/crypto-ecdsa"), InvocationLimits::new(1024, 10))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "publicLength": 65,
            "privateType": "private",
            "publicType": "public",
            "signatureLength": 64,
            "verified": true,
            "rejected": false,
            "privateExportRefused": true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn web_crypto_aes_gcm_encrypts_decrypts_and_authenticates_data() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const encoder = new TextEncoder();
                const decoder = new TextDecoder();
                const iv = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
                const aad = encoder.encode('tenant:acme');
                const generated = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, true, ['encrypt', 'decrypt']);
                const raw = new Uint8Array(await crypto.subtle.exportKey('raw', generated));
                const imported = await crypto.subtle.importKey('raw', raw, 'AES-GCM', false, ['encrypt', 'decrypt']);
                const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: aad }, imported, encoder.encode('secret')));
                const plaintext = new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-GCM', iv, additionalData: aad }, imported, ciphertext));
                let wrongAadRejected = false;
                try {
                  await crypto.subtle.decrypt({ name: 'AES-GCM', iv, additionalData: encoder.encode('tenant:other') }, imported, ciphertext);
                } catch { wrongAadRejected = true; }
                let exportRefused = false;
                try { await crypto.subtle.exportKey('raw', imported); } catch { exportRefused = true; }
                return new Response(JSON.stringify({
                  key: generated instanceof CryptoKey,
                  algorithm: generated.algorithm.name,
                  length: generated.algorithm.length,
                  rawLength: raw.length,
                  ciphertextLonger: ciphertext.length > 'secret'.length,
                  plaintext: decoder.decode(plaintext),
                  wrongAadRejected,
                  exportRefused,
                }));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/crypto-aes-gcm"), InvocationLimits::new(2048, 10))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "key": true,
            "algorithm": "AES-GCM",
            "length": 256,
            "rawLength": 32,
            "ciphertextLonger": true,
            "plaintext": "secret",
            "wrongAadRejected": true,
            "exportRefused": true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn web_crypto_aes_cbc_encrypts_decrypts_and_pads_data() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const encoder = new TextEncoder();
                const decoder = new TextDecoder();
                const iv = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
                const generated = await crypto.subtle.generateKey({ name: 'AES-CBC', length: 128 }, true, ['encrypt', 'decrypt']);
                const raw = new Uint8Array(await crypto.subtle.exportKey('raw', generated));
                const imported = await crypto.subtle.importKey('raw', raw, 'AES-CBC', false, ['encrypt', 'decrypt']);
                const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-CBC', iv }, imported, encoder.encode('secret')));
                const plaintext = new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-CBC', iv }, imported, ciphertext));
                let wrongIvRejected = false;
                try {
                  await crypto.subtle.decrypt({ name: 'AES-CBC', iv: iv.slice(0, 8) }, imported, ciphertext);
                } catch { wrongIvRejected = true; }
                let tamperRejected = false;
                const tampered = new Uint8Array(ciphertext);
                tampered[tampered.length - 1] ^= 1;
                try { await crypto.subtle.decrypt({ name: 'AES-CBC', iv }, imported, tampered); } catch { tamperRejected = true; }
                let exportRefused = false;
                try { await crypto.subtle.exportKey('raw', imported); } catch { exportRefused = true; }
                return new Response(JSON.stringify({
                  key: generated instanceof CryptoKey,
                  algorithm: generated.algorithm.name,
                  length: generated.algorithm.length,
                  rawLength: raw.length,
                  blockAligned: ciphertext.length % 16 === 0,
                  plaintext: decoder.decode(plaintext),
                  wrongIvRejected,
                  tamperRejected,
                  exportRefused,
                }));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/crypto-aes-cbc"), InvocationLimits::new(2048, 10))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "key": true,
            "algorithm": "AES-CBC",
            "length": 128,
            "rawLength": 16,
            "blockAligned": true,
            "plaintext": "secret",
            "wrongIvRejected": true,
            "tamperRejected": true,
            "exportRefused": true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn web_crypto_pbkdf2_derives_bits_and_keys() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const encoder = new TextEncoder();
                const decoder = new TextDecoder();
                const base = await crypto.subtle.importKey('raw', encoder.encode('password'), 'PBKDF2', false, ['deriveBits', 'deriveKey']);
                const params = { name: 'PBKDF2', hash: 'SHA-256', salt: encoder.encode('salt'), iterations: 1 };
                const bits = new Uint8Array(await crypto.subtle.deriveBits(params, base, 256));
                const hex = [...bits].map((byte) => byte.toString(16).padStart(2, '0')).join('');
                const key = await crypto.subtle.deriveKey(params, base, { name: 'AES-GCM', length: 128 }, false, ['encrypt', 'decrypt']);
                const iv = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
                const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, encoder.encode('secret')));
                const plaintext = new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ciphertext));
                let badLengthRejected = false;
                try { await crypto.subtle.deriveBits(params, base, 7); } catch { badLengthRejected = true; }
                let badUsageRejected = false;
                const locked = await crypto.subtle.importKey('raw', encoder.encode('password'), 'PBKDF2', false, ['deriveBits']);
                try { await crypto.subtle.deriveKey(params, locked, { name: 'AES-GCM', length: 128 }, false, ['encrypt']); } catch { badUsageRejected = true; }
                return new Response(JSON.stringify({
                  base: base.algorithm.name,
                  hex,
                  key: key instanceof CryptoKey,
                  algorithm: key.algorithm.name,
                  length: key.algorithm.length,
                  plaintext: decoder.decode(plaintext),
                  badLengthRejected,
                  badUsageRejected,
                }));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/crypto-pbkdf2"), InvocationLimits::new(4096, 10))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "base": "PBKDF2",
            "hex": "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b",
            "key": true,
            "algorithm": "AES-GCM",
            "length": 128,
            "plaintext": "secret",
            "badLengthRejected": true,
            "badUsageRejected": true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn web_crypto_hkdf_derives_bits_and_keys() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const hexBytes = (hex) => Uint8Array.from(hex.match(/../g).map((part) => Number.parseInt(part, 16)));
                const hex = (bytes) => [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
                const ikm = hexBytes('0b'.repeat(22));
                const salt = hexBytes('000102030405060708090a0b0c');
                const info = hexBytes('f0f1f2f3f4f5f6f7f8f9');
                const base = await crypto.subtle.importKey('raw', ikm, 'HKDF', false, ['deriveBits', 'deriveKey']);
                const params = { name: 'HKDF', hash: 'SHA-256', salt, info };
                const bits = new Uint8Array(await crypto.subtle.deriveBits(params, base, 336));
                const encoder = new TextEncoder();
                const decoder = new TextDecoder();
                const key = await crypto.subtle.deriveKey(params, base, { name: 'AES-CBC', length: 128 }, false, ['encrypt', 'decrypt']);
                const iv = new Uint8Array(16);
                const encrypted = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-CBC', iv }, key, encoder.encode('secret')));
                const decrypted = new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-CBC', iv }, key, encrypted));
                let badUsageRejected = false;
                const locked = await crypto.subtle.importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
                try { await crypto.subtle.deriveKey(params, locked, { name: 'AES-CBC', length: 128 }, false, ['encrypt']); } catch { badUsageRejected = true; }
                return new Response(JSON.stringify({
                  base: base.algorithm.name,
                  hex: hex(bits),
                  key: key instanceof CryptoKey,
                  algorithm: key.algorithm.name,
                  plaintext: decoder.decode(decrypted),
                  badUsageRejected,
                }));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/crypto-hkdf"), InvocationLimits::new(4096, 10))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "base": "HKDF",
            "hex": "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865",
            "key": true,
            "algorithm": "AES-CBC",
            "plaintext": "secret",
            "badUsageRejected": true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn web_crypto_rejects_rsa_and_portable_private_key_formats_explicitly() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const errors = [];
                try {
                  await crypto.subtle.generateKey(
                    { name: 'RSASSA-PKCS1-v1_5', modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-256' },
                    false,
                    ['sign', 'verify'],
                  );
                } catch (error) { errors.push(`${error.name}:${error.message.includes('unsupported key algorithm')}`); }
                try {
                  await crypto.subtle.importKey('pkcs8', new Uint8Array([1, 2, 3]), 'Ed25519', false, ['sign']);
                } catch (error) { errors.push(`${error.name}:${error.message.includes('raw Ed25519')}`); }
                const ecdsa = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign', 'verify']);
                try {
                  await crypto.subtle.exportKey('pkcs8', ecdsa.privateKey);
                } catch (error) { errors.push(`${error.name}:${error.message.includes('raw ECDSA public')}`); }
                return new Response(JSON.stringify(errors));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(request("/crypto-refusals"), InvocationLimits::new(1024, 10))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!([
            "NotSupportedError:true",
            "NotSupportedError:true",
            "NotSupportedError:true",
        ])
    );
}
