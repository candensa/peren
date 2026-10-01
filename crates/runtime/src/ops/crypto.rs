use aes::{Aes128, Aes192, Aes256};
use aes_gcm::{
    Aes128Gcm, Aes256Gcm, AesGcm, KeyInit,
    aead::{Aead, Payload, consts::U12},
};
use cbc::cipher::{BlockCipher, BlockDecryptMut, BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};
use deno_core::op2;
use hmac::{Mac, SimpleHmac};
use pbkdf2::pbkdf2_hmac;
use rand::TryRngCore;
use ring::{rand::SystemRandom, signature, signature::KeyPair};
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512, digest::core_api::BlockSizeUser};

#[op2]
#[serde]
pub fn op_crypto_random(#[smi] length: usize) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    if length > 65_536 {
        return Err(deno_error::JsErrorBox::generic(
            "crypto.getRandomValues cannot fill more than 65536 bytes",
        ));
    }
    let mut bytes = vec![0; length];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|source| deno_error::JsErrorBox::generic(source.to_string()))?;
    Ok(bytes)
}

#[op2]
#[serde]
pub fn op_crypto_digest(
    #[string] algorithm: &str,
    #[serde] bytes: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    match normalize_algorithm(algorithm).as_str() {
        "SHA-1" => Ok(Sha1::digest(bytes).to_vec()),
        "SHA-256" => Ok(Sha256::digest(bytes).to_vec()),
        "SHA-384" => Ok(Sha384::digest(bytes).to_vec()),
        "SHA-512" => Ok(Sha512::digest(bytes).to_vec()),
        _ => Err(deno_error::JsErrorBox::generic(format!(
            "unsupported digest algorithm {algorithm:?}"
        ))),
    }
}

#[op2]
#[serde]
pub fn op_crypto_hmac(
    #[string] algorithm: &str,
    #[serde] key: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let key = key.into_boxed_slice();
    let data = data.into_boxed_slice();
    match normalize_algorithm(algorithm).as_str() {
        "SHA-1" => hmac_sha1(&key, &data),
        "SHA-256" => hmac_sha256(&key, &data),
        "SHA-384" => hmac_sha384(&key, &data),
        "SHA-512" => hmac_sha512(&key, &data),
        _ => Err(deno_error::JsErrorBox::generic(format!(
            "unsupported HMAC hash algorithm {algorithm:?}"
        ))),
    }
}

fn hmac_sha1(key: &[u8], data: &[u8]) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let mut mac = <SimpleHmac<Sha1> as Mac>::new_from_slice(key)
        .map_err(|source| deno_error::JsErrorBox::generic(source.to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let mut mac = <SimpleHmac<Sha256> as Mac>::new_from_slice(key)
        .map_err(|source| deno_error::JsErrorBox::generic(source.to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha384(key: &[u8], data: &[u8]) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let mut mac = <SimpleHmac<Sha384> as Mac>::new_from_slice(key)
        .map_err(|source| deno_error::JsErrorBox::generic(source.to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha512(key: &[u8], data: &[u8]) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let mut mac = <SimpleHmac<Sha512> as Mac>::new_from_slice(key)
        .map_err(|source| deno_error::JsErrorBox::generic(source.to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

#[op2]
#[serde]
pub fn op_crypto_pbkdf2(
    #[string] hash: &str,
    #[serde] password: Vec<u8>,
    #[serde] salt: Vec<u8>,
    #[smi] iterations: u32,
    #[smi] length: usize,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    if iterations == 0 {
        return Err(deno_error::JsErrorBox::generic(
            "PBKDF2 iterations must be greater than zero",
        ));
    }
    let password = password.into_boxed_slice();
    let salt = salt.into_boxed_slice();
    let mut output = vec![0; length];
    match normalize_algorithm(hash).as_str() {
        "SHA-1" => pbkdf2_hmac::<Sha1>(&password, &salt, iterations, &mut output),
        "SHA-256" => pbkdf2_hmac::<Sha256>(&password, &salt, iterations, &mut output),
        "SHA-384" => pbkdf2_hmac::<Sha384>(&password, &salt, iterations, &mut output),
        "SHA-512" => pbkdf2_hmac::<Sha512>(&password, &salt, iterations, &mut output),
        _ => {
            return Err(deno_error::JsErrorBox::generic(format!(
                "unsupported PBKDF2 hash algorithm {hash:?}"
            )));
        }
    }
    Ok(output)
}

#[op2]
#[serde]
pub fn op_crypto_hkdf(
    #[string] hash: &str,
    #[serde] key: Vec<u8>,
    #[serde] salt: Vec<u8>,
    #[serde] info: Vec<u8>,
    #[smi] length: usize,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let key = key.into_boxed_slice();
    let salt = salt.into_boxed_slice();
    let info = info.into_boxed_slice();
    match normalize_algorithm(hash).as_str() {
        "SHA-1" => hkdf_with::<Sha1>(&key, &salt, &info, length),
        "SHA-256" => hkdf_with::<Sha256>(&key, &salt, &info, length),
        "SHA-384" => hkdf_with::<Sha384>(&key, &salt, &info, length),
        "SHA-512" => hkdf_with::<Sha512>(&key, &salt, &info, length),
        _ => Err(deno_error::JsErrorBox::generic(format!(
            "unsupported HKDF hash algorithm {hash:?}"
        ))),
    }
}

fn hkdf_with<D>(
    key: &[u8],
    salt: &[u8],
    info: &[u8],
    length: usize,
) -> Result<Vec<u8>, deno_error::JsErrorBox>
where
    D: Digest + Clone + Default + BlockSizeUser,
    SimpleHmac<D>: Mac + hmac::digest::KeyInit,
{
    let digest_len = <D as Digest>::output_size();
    let salt = if salt.is_empty() {
        vec![0; digest_len]
    } else {
        salt.to_vec()
    };
    let mut extract = <SimpleHmac<D> as Mac>::new_from_slice(&salt)
        .map_err(|source| deno_error::JsErrorBox::generic(source.to_string()))?;
    extract.update(key);
    let prk = extract.finalize().into_bytes();
    let max = 255usize.saturating_mul(digest_len);
    if length > max {
        return Err(deno_error::JsErrorBox::generic(
            "HKDF output length exceeds 255 hash blocks",
        ));
    }
    let mut output = Vec::with_capacity(length);
    let mut previous = Vec::new();
    let mut counter = 1u8;
    while output.len() < length {
        let mut expand = <SimpleHmac<D> as Mac>::new_from_slice(&prk)
            .map_err(|source| deno_error::JsErrorBox::generic(source.to_string()))?;
        expand.update(&previous);
        expand.update(info);
        expand.update(&[counter]);
        previous = expand.finalize().into_bytes().to_vec();
        output.extend_from_slice(&previous);
        counter = counter.saturating_add(1);
    }
    output.truncate(length);
    Ok(output)
}

#[op2]
#[serde]
pub fn op_crypto_aes_gcm_encrypt(
    #[serde] key: Vec<u8>,
    #[serde] iv: Vec<u8>,
    #[serde] additional_data: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let key = key.into_boxed_slice();
    let iv = iv.into_boxed_slice();
    let additional_data = additional_data.into_boxed_slice();
    let data = data.into_boxed_slice();
    aes_gcm(&key, &iv, &additional_data, &data, true)
}

#[op2]
#[serde]
pub fn op_crypto_aes_gcm_decrypt(
    #[serde] key: Vec<u8>,
    #[serde] iv: Vec<u8>,
    #[serde] additional_data: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let key = key.into_boxed_slice();
    let iv = iv.into_boxed_slice();
    let additional_data = additional_data.into_boxed_slice();
    let data = data.into_boxed_slice();
    aes_gcm(&key, &iv, &additional_data, &data, false)
}

fn aes_gcm(
    key: &[u8],
    iv: &[u8],
    additional_data: &[u8],
    data: &[u8],
    encrypt: bool,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    if iv.len() != 12 {
        return Err(deno_error::JsErrorBox::generic(
            "AES-GCM requires a 12-byte iv",
        ));
    }
    let nonce = aes_gcm::Nonce::from_slice(iv);
    let payload = Payload {
        msg: data,
        aad: additional_data,
    };
    let result = match key.len() {
        16 => {
            let cipher = Aes128Gcm::new_from_slice(key)
                .map_err(|_| deno_error::JsErrorBox::generic("invalid AES-GCM key"))?;
            if encrypt {
                cipher.encrypt(nonce, payload)
            } else {
                cipher.decrypt(nonce, payload)
            }
        }
        24 => {
            let cipher = AesGcm::<Aes192, U12>::new_from_slice(key)
                .map_err(|_| deno_error::JsErrorBox::generic("invalid AES-GCM key"))?;
            if encrypt {
                cipher.encrypt(nonce, payload)
            } else {
                cipher.decrypt(nonce, payload)
            }
        }
        32 => {
            let cipher = Aes256Gcm::new_from_slice(key)
                .map_err(|_| deno_error::JsErrorBox::generic("invalid AES-GCM key"))?;
            if encrypt {
                cipher.encrypt(nonce, payload)
            } else {
                cipher.decrypt(nonce, payload)
            }
        }
        _ => {
            return Err(deno_error::JsErrorBox::generic(
                "AES-GCM keys must be 128, 192, or 256 bits",
            ));
        }
    };
    result.map_err(|_| deno_error::JsErrorBox::generic("AES-GCM operation failed"))
}

#[op2]
#[serde]
pub fn op_crypto_aes_cbc_encrypt(
    #[serde] key: Vec<u8>,
    #[serde] iv: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let key = key.into_boxed_slice();
    let iv = iv.into_boxed_slice();
    let data = data.into_boxed_slice();
    aes_cbc(&key, &iv, &data, true)
}

#[op2]
#[serde]
pub fn op_crypto_aes_cbc_decrypt(
    #[serde] key: Vec<u8>,
    #[serde] iv: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let key = key.into_boxed_slice();
    let iv = iv.into_boxed_slice();
    let data = data.into_boxed_slice();
    aes_cbc(&key, &iv, &data, false)
}

fn aes_cbc(
    key: &[u8],
    iv: &[u8],
    data: &[u8],
    encrypt: bool,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    if iv.len() != 16 {
        return Err(deno_error::JsErrorBox::generic(
            "AES-CBC requires a 16-byte iv",
        ));
    }
    match key.len() {
        16 => aes_cbc_with::<Aes128>(key, iv, data, encrypt),
        24 => aes_cbc_with::<Aes192>(key, iv, data, encrypt),
        32 => aes_cbc_with::<Aes256>(key, iv, data, encrypt),
        _ => Err(deno_error::JsErrorBox::generic(
            "AES-CBC keys must be 128, 192, or 256 bits",
        )),
    }
}

fn aes_cbc_with<C>(
    key: &[u8],
    iv: &[u8],
    data: &[u8],
    encrypt: bool,
) -> Result<Vec<u8>, deno_error::JsErrorBox>
where
    C: BlockCipher + BlockEncryptMut + BlockDecryptMut,
    cbc::Encryptor<C>: KeyIvInit,
    cbc::Decryptor<C>: KeyIvInit,
{
    if encrypt {
        let mut buffer = vec![0; data.len() + 16];
        buffer[..data.len()].copy_from_slice(data);
        let encrypted = cbc::Encryptor::<C>::new_from_slices(key, iv)
            .map_err(|_| deno_error::JsErrorBox::generic("invalid AES-CBC key or iv"))?
            .encrypt_padded_mut::<Pkcs7>(&mut buffer, data.len())
            .map_err(|_| deno_error::JsErrorBox::generic("AES-CBC encryption failed"))?;
        Ok(encrypted.to_vec())
    } else {
        let mut buffer = data.to_vec();
        let decrypted = cbc::Decryptor::<C>::new_from_slices(key, iv)
            .map_err(|_| deno_error::JsErrorBox::generic("invalid AES-CBC key or iv"))?
            .decrypt_padded_mut::<Pkcs7>(&mut buffer)
            .map_err(|_| deno_error::JsErrorBox::generic("AES-CBC operation failed"))?;
        Ok(decrypted.to_vec())
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Ed25519KeyPair {
    public_key: Vec<u8>,
    private_key: Vec<u8>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EcdsaKeyPair {
    public_key: Vec<u8>,
    private_key: Vec<u8>,
}

#[op2]
#[serde]
pub fn op_crypto_ed25519_generate() -> Result<Ed25519KeyPair, deno_error::JsErrorBox> {
    let rng = SystemRandom::new();
    let pkcs8 = signature::Ed25519KeyPair::generate_pkcs8(&rng)
        .map_err(|_| deno_error::JsErrorBox::generic("failed to generate Ed25519 key"))?;
    let pair = signature::Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())
        .map_err(|_| deno_error::JsErrorBox::generic("failed to load generated Ed25519 key"))?;
    Ok(Ed25519KeyPair {
        public_key: pair.public_key().as_ref().to_vec(),
        private_key: pkcs8.as_ref().to_vec(),
    })
}

#[op2]
#[serde]
pub fn op_crypto_ed25519_sign(
    #[serde] private_key: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let private_key = private_key.into_boxed_slice();
    let data = data.into_boxed_slice();
    let pair = signature::Ed25519KeyPair::from_pkcs8(&private_key)
        .map_err(|_| deno_error::JsErrorBox::generic("invalid Ed25519 private key"))?;
    Ok(pair.sign(&data).as_ref().to_vec())
}

#[op2]
pub fn op_crypto_ed25519_verify(
    #[serde] public_key: Vec<u8>,
    #[serde] signature: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<bool, deno_error::JsErrorBox> {
    let public_key = public_key.into_boxed_slice();
    let signature = signature.into_boxed_slice();
    let data = data.into_boxed_slice();
    if public_key.len() != 32 {
        return Err(deno_error::JsErrorBox::generic(
            "invalid Ed25519 public key",
        ));
    }
    Ok(
        signature::UnparsedPublicKey::new(&signature::ED25519, &public_key)
            .verify(&data, &signature)
            .is_ok(),
    )
}

#[op2]
#[serde]
pub fn op_crypto_ecdsa_p256_generate() -> Result<EcdsaKeyPair, deno_error::JsErrorBox> {
    let rng = SystemRandom::new();
    let pkcs8 =
        signature::EcdsaKeyPair::generate_pkcs8(&signature::ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
            .map_err(|_| deno_error::JsErrorBox::generic("failed to generate ECDSA P-256 key"))?;
    let pair = signature::EcdsaKeyPair::from_pkcs8(
        &signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        pkcs8.as_ref(),
        &rng,
    )
    .map_err(|_| deno_error::JsErrorBox::generic("failed to load generated ECDSA P-256 key"))?;
    Ok(EcdsaKeyPair {
        public_key: pair.public_key().as_ref().to_vec(),
        private_key: pkcs8.as_ref().to_vec(),
    })
}

#[op2]
#[serde]
pub fn op_crypto_ecdsa_p256_sign(
    #[serde] private_key: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let rng = SystemRandom::new();
    let private_key = private_key.into_boxed_slice();
    let data = data.into_boxed_slice();
    let pair = signature::EcdsaKeyPair::from_pkcs8(
        &signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        &private_key,
        &rng,
    )
    .map_err(|_| deno_error::JsErrorBox::generic("invalid ECDSA P-256 private key"))?;
    let der = pair
        .sign(&rng, &data)
        .map_err(|_| deno_error::JsErrorBox::generic("failed to sign with ECDSA P-256 key"))?;
    ecdsa_der_to_raw(der.as_ref())
}

#[op2]
pub fn op_crypto_ecdsa_p256_verify(
    #[serde] public_key: Vec<u8>,
    #[serde] signature: Vec<u8>,
    #[serde] data: Vec<u8>,
) -> Result<bool, deno_error::JsErrorBox> {
    let public_key = public_key.into_boxed_slice();
    let signature = signature.into_boxed_slice();
    let signature = ecdsa_raw_to_der(&signature)?;
    let data = data.into_boxed_slice();
    if public_key.len() != 65 || public_key[0] != 4 {
        return Err(deno_error::JsErrorBox::generic(
            "invalid ECDSA P-256 public key",
        ));
    }
    Ok(
        signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_ASN1, &public_key)
            .verify(&data, &signature)
            .is_ok(),
    )
}

fn ecdsa_der_to_raw(der: &[u8]) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    if der.len() < 8 || der[0] != 0x30 {
        return Err(deno_error::JsErrorBox::generic("invalid ECDSA signature"));
    }
    let mut offset = 2;
    let r = der_integer(der, &mut offset)?;
    let s = der_integer(der, &mut offset)?;
    let mut raw = vec![0; 64];
    write_scalar(&mut raw[..32], r)?;
    write_scalar(&mut raw[32..], s)?;
    Ok(raw)
}

fn der_integer<'a>(der: &'a [u8], offset: &mut usize) -> Result<&'a [u8], deno_error::JsErrorBox> {
    if der.get(*offset) != Some(&0x02) {
        return Err(deno_error::JsErrorBox::generic(
            "invalid ECDSA signature integer",
        ));
    }
    *offset += 1;
    let Some(length) = der.get(*offset).copied().map(usize::from) else {
        return Err(deno_error::JsErrorBox::generic(
            "invalid ECDSA signature length",
        ));
    };
    *offset += 1;
    let end = offset.saturating_add(length);
    let Some(value) = der.get(*offset..end) else {
        return Err(deno_error::JsErrorBox::generic("truncated ECDSA signature"));
    };
    *offset = end;
    Ok(value)
}

fn write_scalar(output: &mut [u8], value: &[u8]) -> Result<(), deno_error::JsErrorBox> {
    let value = if value.first() == Some(&0) {
        &value[1..]
    } else {
        value
    };
    if value.len() > output.len() {
        return Err(deno_error::JsErrorBox::generic(
            "oversized ECDSA signature scalar",
        ));
    }
    let start = output.len() - value.len();
    output[start..].copy_from_slice(value);
    Ok(())
}

fn ecdsa_raw_to_der(signature: &[u8]) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    if signature.len() != 64 {
        return Err(deno_error::JsErrorBox::generic(
            "ECDSA P-256 signatures must be 64 bytes",
        ));
    }
    let r = der_scalar(&signature[..32]);
    let s = der_scalar(&signature[32..]);
    let length = 2 + r.len() + 2 + s.len();
    if length > 127 {
        return Err(deno_error::JsErrorBox::generic(
            "ECDSA signature is too large",
        ));
    }
    let mut der = Vec::with_capacity(2 + length);
    der.push(0x30);
    der.push(u8::try_from(length).expect("ECDSA signature length is bounded"));
    der.push(0x02);
    der.push(u8::try_from(r.len()).expect("ECDSA scalar length is bounded"));
    der.extend_from_slice(&r);
    der.push(0x02);
    der.push(u8::try_from(s.len()).expect("ECDSA scalar length is bounded"));
    der.extend_from_slice(&s);
    Ok(der)
}

fn der_scalar(value: &[u8]) -> Vec<u8> {
    let mut start = 0;
    while start + 1 < value.len() && value[start] == 0 {
        start += 1;
    }
    let mut out = value[start..].to_vec();
    if out.first().is_some_and(|byte| byte & 0x80 != 0) {
        out.insert(0, 0);
    }
    out
}

fn normalize_algorithm(algorithm: &str) -> String {
    algorithm
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .flat_map(char::to_uppercase)
        .collect()
}
