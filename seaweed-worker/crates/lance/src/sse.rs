//! Decodes what the filer and the S3 gateway encrypt, for reads that bypass
//! the gateway, and encodes the same for writes that do. The filer's volume
//! encryption (`cipher_key`) is AES-256-GCM over the chunk, compressed first
//! when `is_compressed`. The gateway's SSE-S3 is AES-256-CTR per chunk under a
//! per-object key, which the chunk's and the object's metadata carry wrapped
//! (AES-256-GCM) by the cluster's KEK; the KEK comes from `s3.sse.kek`,
//! `s3.sse.key` or the filer's `/etc/s3/sse_kek`, as for the gateway.

use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{Context, Result, bail};
use base64::prelude::{BASE64_STANDARD, Engine};
use ctr::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sw_client::ChunkDecoder;
use sw_client::filer_pb::{Entry, FileChunk};

type Aes256Ctr = ctr::Ctr128BE<aes::Aes256>;

const KEY_SIZE: usize = 32;
const IV_SIZE: usize = 16;
const GCM_NONCE_SIZE: usize = 12;
const SSE_S3_KEY_ATTRIBUTE: &str = "x-seaweedfs-sse-s3-key";
const SSE_HEADER_ATTRIBUTE: &str = "X-Amz-Server-Side-Encryption";
const AES256: &str = "AES256";
/// `filer_pb.SSEType`.
const SSE_C: i32 = 1;
const SSE_KMS: i32 = 2;
const SSE_S3: i32 = 3;
pub const KEK_PATH: (&str, &str) = ("/etc/s3", "sse_kek");
const KEK_WRAPPED_V2_MAGIC: &[u8] = b"SWv2";
const KEK_SALT_SIZE: usize = 32;
const LEGACY_KEK_SALT: &[u8] = b"seaweedfs-sse-s3-kek-wrapping-v1";

/// How the gateway's KEK is configured: `security.toml` `s3.sse.kek`,
/// `s3.sse.key` and `s3.sse.kek.passphrase`, or their `WEED_` environment
/// variables.
#[derive(Clone, Default, Debug)]
pub struct SseConfig {
    /// Hex-encoded 256-bit KEK.
    pub kek_hex: Option<String>,
    /// Any secret; the KEK is derived from it.
    pub key: Option<String>,
    /// Unwraps a KEK stored encrypted on the filer.
    pub passphrase: Option<String>,
    /// Refuse objects whose metadata carries no key commitment.
    pub require_commitment: bool,
}

/// The KEK that unwraps SSE-S3 object keys.
pub struct SseS3Keys {
    kek: Option<[u8; KEY_SIZE]>,
    require_commitment: bool,
}

impl std::fmt::Debug for SseS3Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseS3Keys")
            .field("configured", &self.kek.is_some())
            .finish()
    }
}

impl SseS3Keys {
    /// Resolves the KEK the gateway's way: `kek_hex` (which must match the
    /// filer's file when both exist), else `key` (refused while the filer's
    /// file exists), else the filer's file, else none. `filer_file` is the
    /// content of `/etc/s3/sse_kek` when it exists.
    pub fn load(config: &SseConfig, filer_file: Option<&[u8]>) -> Result<Self> {
        let filer_kek = filer_file
            .map(|content| kek_from_filer(content, config.passphrase.as_deref()))
            .transpose()?;
        let kek = match (&config.kek_hex, &config.key) {
            (Some(_), Some(_)) => bail!("only one of s3.sse.kek and s3.sse.key may be set"),
            (Some(hex), None) => {
                let kek = key_from_hex(hex).context("s3.sse.kek")?;
                if filer_kek.is_some_and(|f| f != kek) {
                    bail!("s3.sse.kek does not match the filer's /etc/s3/sse_kek");
                }
                Some(kek)
            }
            (None, Some(secret)) => {
                if filer_kek.is_some() {
                    bail!("s3.sse.key cannot be used while /etc/s3/sse_kek exists on the filer");
                }
                Some(derive_key(secret.as_bytes(), None, b"seaweedfs-sse-s3-kek"))
            }
            (None, None) => filer_kek,
        };
        Ok(Self {
            kek,
            require_commitment: config.require_commitment,
        })
    }

    pub fn configured(&self) -> bool {
        self.kek.is_some()
    }

    /// Wraps an object key under the KEK, as the gateway's key manager does:
    /// AES-256-GCM under a fresh nonce.
    fn wrap_dek(&self, dek: &[u8; KEY_SIZE]) -> Result<(Vec<u8>, [u8; GCM_NONCE_SIZE])> {
        let Some(kek) = &self.kek else {
            bail!(
                "SSE-S3 is not configured: set s3.sse.kek or s3.sse.key for the engine as for the S3 gateway"
            );
        };
        let mut nonce = [0u8; GCM_NONCE_SIZE];
        OsRng.fill_bytes(&mut nonce);
        let wrapped = Aes256Gcm::new(kek.into())
            .encrypt(&Nonce::from(nonce), dek.as_slice())
            .map_err(|_| anyhow::anyhow!("wrapping the object key"))?;
        Ok((wrapped, nonce))
    }

    fn unwrap_dek(&self, wrapped: &[u8], nonce: &[u8]) -> Result<[u8; KEY_SIZE]> {
        let Some(kek) = &self.kek else {
            bail!(
                "SSE-S3 is not configured: set s3.sse.kek or s3.sse.key for the worker as for the S3 gateway"
            );
        };
        gcm_open(kek, nonce, wrapped)
            .context("unwrapping the object key")?
            .try_into()
            .ok()
            .context("the object key is not 256 bits")
    }
}

fn key_from_hex(hex: &str) -> Result<[u8; KEY_SIZE]> {
    hex::decode(hex.trim())
        .context("not hex")?
        .try_into()
        .ok()
        .context("not 32 bytes")
}

fn derive_key(secret: &[u8], salt: Option<&[u8]>, info: &[u8]) -> [u8; KEY_SIZE] {
    let mut key = [0u8; KEY_SIZE];
    Hkdf::<Sha256>::new(salt, secret)
        .expand(info, &mut key)
        .expect("32 bytes fit in one HKDF block");
    key
}

/// The filer's `/etc/s3/sse_kek`: hex, or wrapped under the passphrase as
/// `magic || salt || nonce || ciphertext` (v2) or `nonce || ciphertext` with
/// a fixed salt (v1).
fn kek_from_filer(content: &[u8], passphrase: Option<&str>) -> Result<[u8; KEY_SIZE]> {
    if content.is_empty() {
        bail!("the filer's /etc/s3/sse_kek is empty");
    }
    let text = std::str::from_utf8(content).context("/etc/s3/sse_kek is not text")?;
    let Some(passphrase) = passphrase else {
        return key_from_hex(text).context("/etc/s3/sse_kek");
    };
    let unwrapped = (|| {
        let raw = BASE64_STANDARD.decode(text.trim())?;
        let (salt, payload) = match raw.strip_prefix(KEK_WRAPPED_V2_MAGIC) {
            Some(rest) if rest.len() > KEK_SALT_SIZE => rest.split_at(KEK_SALT_SIZE),
            _ => (LEGACY_KEK_SALT, &raw[..]),
        };
        let wrapping = derive_key(passphrase.as_bytes(), Some(salt), b"kek-wrapping");
        if payload.len() < GCM_NONCE_SIZE {
            bail!("wrapped KEK too short");
        }
        let (nonce, sealed) = payload.split_at(GCM_NONCE_SIZE);
        gcm_open(&wrapping, nonce, sealed)
    })();
    match unwrapped {
        Ok(kek) => kek
            .try_into()
            .ok()
            .context("the unwrapped KEK is not 256 bits"),
        // A KEK written before the passphrase was set is still plain hex.
        Err(err) => key_from_hex(text).map_err(|_| err.context("unwrapping /etc/s3/sse_kek")),
    }
}

fn gcm_open(key: &[u8; KEY_SIZE], nonce: &[u8], sealed: &[u8]) -> Result<Vec<u8>> {
    let nonce: [u8; GCM_NONCE_SIZE] = nonce
        .try_into()
        .ok()
        .with_context(|| format!("nonce is {} bytes, not {GCM_NONCE_SIZE}", nonce.len()))?;
    Aes256Gcm::new(key.into())
        .decrypt(&Nonce::from(nonce), sealed)
        .map_err(|_| anyhow::anyhow!("AES-GCM authentication failed"))
}

/// The filer's volume encryption on the way in: `nonce || ciphertext` under a
/// fresh chunk key, which the caller records as the chunk's `cipher_key`.
fn encrypt_chunk(data: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    OsRng.fill_bytes(&mut key);
    let mut nonce = [0u8; GCM_NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce);
    let mut out = nonce.to_vec();
    out.extend(
        Aes256Gcm::new((&key).into())
            .encrypt(&Nonce::from(nonce), data)
            .expect("AES-GCM encryption of a chunk"),
    );
    (out, key.to_vec())
}

/// The filer's volume encryption: `nonce || ciphertext` under the chunk's key.
fn decrypt_chunk(cipher_key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let key: &[u8; KEY_SIZE] = cipher_key
        .try_into()
        .ok()
        .context("the chunk's cipher key is not 256 bits")?;
    if data.len() < GCM_NONCE_SIZE {
        bail!("encrypted chunk too short");
    }
    let (nonce, sealed) = data.split_at(GCM_NONCE_SIZE);
    gcm_open(key, nonce, sealed).context("decrypting the chunk")
}

/// Undoes the filer's compression by the data's magic bytes; other data is
/// returned as it is, as the filer does.
fn decompress(data: Vec<u8>) -> Result<Vec<u8>> {
    match data.as_slice() {
        [0x1f, 0x8b, ..] => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(data.as_slice())
                .read_to_end(&mut out)
                .context("gunzip")?;
            Ok(out)
        }
        [0x28, 0xb5, 0x2f, 0xfd, ..] => zstd::decode_all(data.as_slice()).context("unzstd"),
        _ => Ok(data),
    }
}

/// An SSE-S3 key as the gateway serializes it: the object key wrapped by the
/// KEK, with the IV and the commitment binding the two.
#[derive(serde::Deserialize)]
struct SerializedKey {
    algorithm: Option<String>,
    #[serde(rename = "encryptedDEK")]
    encrypted_dek: String,
    nonce: String,
    iv: Option<String>,
    #[serde(rename = "keyCommitment")]
    key_commitment: Option<String>,
}

struct ObjectKey {
    key: [u8; KEY_SIZE],
    iv: Option<[u8; IV_SIZE]>,
}

impl ObjectKey {
    fn generate() -> Self {
        let mut key = [0u8; KEY_SIZE];
        OsRng.fill_bytes(&mut key);
        let mut iv = [0u8; IV_SIZE];
        OsRng.fill_bytes(&mut iv);
        Self { key, iv: Some(iv) }
    }

    /// The key as the gateway serializes it, with `iv` as its IV.
    fn serialize(&self, keys: &SseS3Keys, key_id: &str, iv: [u8; IV_SIZE]) -> Result<Vec<u8>> {
        let (wrapped, nonce) = keys.wrap_dek(&self.key)?;
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.key).expect("any key length");
        mac.update(&iv);
        mac.update(AES256.as_bytes());
        let commitment = mac.finalize().into_bytes();
        Ok(serde_json::to_vec(&serde_json::json!({
            "algorithm": AES256,
            "keyId": key_id,
            "encryptedDEK": BASE64_STANDARD.encode(&wrapped),
            "nonce": BASE64_STANDARD.encode(nonce),
            "iv": BASE64_STANDARD.encode(iv),
            "keyCommitment": BASE64_STANDARD.encode(commitment),
        }))?)
    }

    fn parse(keys: &SseS3Keys, data: &[u8]) -> Result<Self> {
        let meta: SerializedKey = serde_json::from_slice(data).context("SSE-S3 metadata")?;
        let b64 = |field: &str| {
            BASE64_STANDARD
                .decode(field)
                .context("SSE-S3 metadata base64")
        };
        let key = keys.unwrap_dek(&b64(&meta.encrypted_dek)?, &b64(&meta.nonce)?)?;
        let iv: Option<[u8; IV_SIZE]> = match &meta.iv {
            Some(iv) => Some(
                <[u8; IV_SIZE]>::try_from(b64(iv)?.as_slice())
                    .ok()
                    .context("the SSE-S3 IV is not 16 bytes")?,
            ),
            None => None,
        };
        let algorithm = meta.algorithm.as_deref().unwrap_or(AES256);
        match (&meta.key_commitment, iv) {
            (Some(commitment), Some(iv)) => {
                let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key).expect("any key length");
                mac.update(&iv);
                mac.update(algorithm.as_bytes());
                mac.verify_slice(&b64(commitment)?)
                    .map_err(|_| anyhow::anyhow!("the SSE-S3 key commitment does not match"))?;
            }
            (None, Some(_)) if keys.require_commitment => {
                bail!("the SSE-S3 metadata carries no key commitment")
            }
            _ => {}
        }
        Ok(Self { key, iv })
    }

    /// Decrypts `data`, the ciphertext from `position` bytes into the CTR
    /// stream that starts at this key's IV.
    fn decrypt(&self, position: u64, data: &mut [u8]) -> Result<()> {
        let iv = self.iv.context("the SSE-S3 key has no IV")?;
        let mut ctr = Aes256Ctr::new(&self.key.into(), &iv.into());
        ctr.try_seek(position)
            .map_err(|e| anyhow::anyhow!("CTR position {position}: {e}"))?;
        ctr.apply_keystream(data);
        Ok(())
    }
}

/// One chunk's bytes as the volume stores them, with what its `FileChunk`
/// records about them.
pub struct EncodedChunk {
    pub data: Vec<u8>,
    pub cipher_key: Vec<u8>,
    pub sse_type: i32,
    pub sse_metadata: Vec<u8>,
}

/// Encodes an object's chunks as the gateway would store them: SSE-S3 when the
/// bucket encrypts, then the filer's volume encryption when the cluster asks
/// for it. The first chunk is under the object's key and IV, which is how the
/// gateway reads an object of one chunk; every other chunk is under a key and
/// IV of its own, as the gateway's multipart parts are and as it reads an
/// object of several, so no two chunks share a keystream whatever their
/// boundaries.
pub struct EntryEncoder {
    keys: Arc<SseS3Keys>,
    object: Option<(ObjectKey, String)>,
    cipher: bool,
}

fn key_id() -> String {
    format!("sse-s3-key-{}", OsRng.next_u64() >> 1)
}

impl EntryEncoder {
    pub fn new(keys: Arc<SseS3Keys>, sse_s3: bool, cipher: bool) -> Result<Self> {
        let object = if sse_s3 {
            if !keys.configured() {
                bail!(
                    "the bucket encrypts with SSE-S3, which needs s3.sse.kek or s3.sse.key for the engine as for the S3 gateway"
                );
            }
            Some((ObjectKey::generate(), key_id()))
        } else {
            None
        };
        Ok(Self {
            keys,
            object,
            cipher,
        })
    }

    /// The bytes of the chunk at `offset` in the object, as they go to the
    /// volume server, with the SSE-S3 metadata that reads them back.
    pub fn encode(&self, offset: u64, mut data: Vec<u8>) -> Result<EncodedChunk> {
        let (sse_type, sse_metadata) = match &self.object {
            Some((object, key_id)) if offset == 0 => {
                let iv = object.iv.expect("a generated key has an IV");
                object.decrypt(0, &mut data)?;
                (SSE_S3, object.serialize(&self.keys, key_id, iv)?)
            }
            Some(_) => {
                let chunk_key = ObjectKey::generate();
                let iv = chunk_key.iv.expect("a generated key has an IV");
                chunk_key.decrypt(0, &mut data)?;
                (SSE_S3, chunk_key.serialize(&self.keys, &key_id(), iv)?)
            }
            None => (0, Vec::new()),
        };
        let (data, cipher_key) = if self.cipher {
            encrypt_chunk(&data)
        } else {
            (data, Vec::new())
        };
        Ok(EncodedChunk {
            data,
            cipher_key,
            sse_type,
            sse_metadata,
        })
    }

    /// The attributes the entry carries for its chunks to be decodable: the
    /// SSE-S3 header and the object key.
    pub fn attributes(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let Some((object, key_id)) = &self.object else {
            return Ok(Vec::new());
        };
        let iv = object.iv.expect("a generated key has an IV");
        Ok(vec![
            (SSE_HEADER_ATTRIBUTE.to_string(), AES256.as_bytes().to_vec()),
            (
                SSE_S3_KEY_ATTRIBUTE.to_string(),
                object.serialize(&self.keys, key_id, iv)?,
            ),
        ])
    }
}

fn attribute<'a>(entry: &'a Entry, name: &str) -> Option<&'a [u8]> {
    entry
        .extended
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_slice())
}

/// Decodes one entry's chunks: the filer's encryption first, then SSE-S3 with
/// the chunk's own key, or the object's key for chunks written without one.
pub struct EntryDecoder {
    keys: Arc<SseS3Keys>,
    object: Option<ObjectKey>,
    chunk_keys: Mutex<HashMap<String, Arc<ObjectKey>>>,
}

impl EntryDecoder {
    pub fn new(keys: Arc<SseS3Keys>, entry: &Entry) -> Result<Self> {
        // As the gateway, an object is SSE-S3 only with both the header and
        // the key; a header left over from a decrypting copy means nothing.
        let object = match (
            attribute(entry, SSE_HEADER_ATTRIBUTE),
            attribute(entry, SSE_S3_KEY_ATTRIBUTE),
        ) {
            (Some(algorithm), Some(key)) if algorithm == AES256.as_bytes() => {
                Some(ObjectKey::parse(&keys, key).context("the object's SSE-S3 key")?)
            }
            _ => None,
        };
        Ok(Self {
            keys,
            object,
            chunk_keys: Mutex::new(HashMap::new()),
        })
    }

    /// Whether anything about this entry needs decoding.
    pub fn is_plain(&self, entry: &Entry) -> bool {
        self.object.is_none()
            && entry
                .chunks
                .iter()
                .all(|c| c.cipher_key.is_empty() && c.sse_type == 0)
    }

    fn chunk_key(&self, chunk: &FileChunk) -> Result<Arc<ObjectKey>> {
        // A chunk out of a manifest carries its id as `fid`, not `file_id`.
        let id = sw_client::read::file_id_string(chunk).unwrap_or_default();
        if let Some(key) = self.chunk_keys.lock().unwrap().get(&id) {
            return Ok(key.clone());
        }
        let key = Arc::new(
            ObjectKey::parse(&self.keys, &chunk.sse_metadata)
                .with_context(|| format!("chunk {id}'s SSE-S3 key"))?,
        );
        self.chunk_keys.lock().unwrap().insert(id, key.clone());
        Ok(key)
    }
}

impl ChunkDecoder for EntryDecoder {
    fn needs_whole_chunk(&self, chunk: &FileChunk) -> bool {
        !chunk.cipher_key.is_empty()
    }

    fn decode(&self, chunk: &FileChunk, offset: u64, data: Vec<u8>) -> Result<Vec<u8>> {
        let mut data = if chunk.cipher_key.is_empty() {
            data
        } else {
            let plain = decrypt_chunk(&chunk.cipher_key, &data)?;
            if chunk.is_compressed {
                decompress(plain)?
            } else {
                plain
            }
        };
        // Manifests list chunks; the gateway encrypts only the bytes.
        if chunk.is_chunk_manifest {
            return Ok(data);
        }
        match chunk.sse_type {
            0 => {
                if let Some(object) = &self.object {
                    object.decrypt(chunk.offset as u64 + offset, &mut data)?;
                }
            }
            SSE_S3 if chunk.sse_metadata.is_empty() => {
                let object = self
                    .object
                    .as_ref()
                    .context("an SSE-S3 chunk without a key in an object without one")?;
                object.decrypt(chunk.offset as u64 + offset, &mut data)?;
            }
            SSE_S3 => self.chunk_key(chunk)?.decrypt(offset, &mut data)?,
            SSE_C => bail!("SSE-C objects need the customer's key, which direct reads do not have"),
            SSE_KMS => bail!("SSE-KMS objects are not supported by direct reads"),
            other => bail!("unknown SSE type {other}"),
        }
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sw_client::filer_pb::FuseAttributes;

    #[derive(serde::Deserialize)]
    struct FixtureChunk {
        offset: i64,
        ciphertext: String,
        sse_metadata: String,
    }

    #[derive(serde::Deserialize)]
    struct Fixture {
        kek_secret: String,
        kek_hex: String,
        kek_passphrase: String,
        kek_wrapped_v1: String,
        kek_wrapped_v2: String,
        plaintext: String,
        object_metadata: String,
        chunks: Vec<FixtureChunk>,
        whole_ciphertext: String,
        cipher_key: String,
        cipher_plain: String,
        cipher_gzip: String,
    }

    /// Written by the gateway's own code: `TestGenerateDirectReadFixture` in
    /// weed/s3api.
    fn fixture() -> Fixture {
        serde_json::from_str(include_str!("../tests/direct_read_fixture.json")).unwrap()
    }

    fn un(hex: &str) -> Vec<u8> {
        hex::decode(hex).unwrap()
    }

    fn keys(config: SseConfig, filer_file: Option<&[u8]>) -> Arc<SseS3Keys> {
        Arc::new(SseS3Keys::load(&config, filer_file).unwrap())
    }

    fn from_secret(f: &Fixture) -> Arc<SseS3Keys> {
        keys(
            SseConfig {
                key: Some(f.kek_secret.clone()),
                ..Default::default()
            },
            None,
        )
    }

    fn sse_entry(f: &Fixture, with_object_key: bool) -> Entry {
        let mut entry = Entry {
            name: "a.lance".into(),
            attributes: Some(FuseAttributes {
                file_size: f.plaintext.len() as u64 / 2,
                ..Default::default()
            }),
            chunks: f
                .chunks
                .iter()
                .enumerate()
                .map(|(i, c)| FileChunk {
                    file_id: format!("3,{i:02x}"),
                    offset: c.offset,
                    size: c.ciphertext.len() as u64 / 2,
                    sse_type: SSE_S3,
                    sse_metadata: c.sse_metadata.clone().into_bytes(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        if with_object_key {
            entry
                .extended
                .insert(SSE_HEADER_ATTRIBUTE.into(), AES256.into());
            entry.extended.insert(
                SSE_S3_KEY_ATTRIBUTE.into(),
                f.object_metadata.clone().into_bytes(),
            );
        }
        entry
    }

    #[test]
    fn sse_s3_chunks_decrypt_from_any_offset() {
        let f = fixture();
        let plain = un(&f.plaintext);
        let entry = sse_entry(&f, false);
        let decoder = EntryDecoder::new(from_secret(&f), &entry).unwrap();
        assert!(!decoder.is_plain(&entry));
        for (i, chunk) in entry.chunks.iter().enumerate() {
            let ciphertext = un(&f.chunks[i].ciphertext);
            assert!(!decoder.needs_whole_chunk(chunk));
            for offset in [0usize, 1, 7, 16, 17, 100, 2047] {
                let want = &plain
                    [chunk.offset as usize + offset..chunk.offset as usize + chunk.size as usize];
                let got = decoder
                    .decode(chunk, offset as u64, ciphertext[offset..].to_vec())
                    .unwrap();
                assert_eq!(got, want, "chunk {i} from {offset}");
            }
        }
    }

    /// Chunks expanded from a manifest name themselves by `fid`; keyed by the
    /// empty `file_id` they would all share one key and IV.
    #[test]
    fn chunks_out_of_a_manifest_keep_their_own_keys() {
        let f = fixture();
        let plain = un(&f.plaintext);
        let mut entry = sse_entry(&f, false);
        for (i, chunk) in entry.chunks.iter_mut().enumerate() {
            chunk.file_id.clear();
            chunk.fid = Some(sw_client::filer_pb::FileId {
                volume_id: 3,
                file_key: i as u64 + 1,
                cookie: 7,
            });
        }
        let decoder = EntryDecoder::new(from_secret(&f), &entry).unwrap();
        for (i, chunk) in entry.chunks.iter().enumerate() {
            let got = decoder
                .decode(chunk, 0, un(&f.chunks[i].ciphertext))
                .unwrap();
            let want = &plain[chunk.offset as usize..chunk.offset as usize + chunk.size as usize];
            assert_eq!(got, want, "chunk {i}");
        }
    }

    #[test]
    fn chunks_without_their_own_key_use_the_objects() {
        let f = fixture();
        let plain = un(&f.plaintext);
        let whole = un(&f.whole_ciphertext);
        let mut entry = sse_entry(&f, true);
        for chunk in &mut entry.chunks {
            chunk.sse_type = 0;
            chunk.sse_metadata.clear();
        }
        let decoder = EntryDecoder::new(from_secret(&f), &entry).unwrap();
        let chunk = &entry.chunks[1];
        let start = chunk.offset as usize;
        let got = decoder
            .decode(chunk, 13, whole[start + 13..start + 500].to_vec())
            .unwrap();
        assert_eq!(got, &plain[start + 13..start + 500]);

        // A header without a key is not encryption, as for the gateway.
        entry.extended.remove(SSE_S3_KEY_ATTRIBUTE);
        let decoder = EntryDecoder::new(from_secret(&f), &entry).unwrap();
        assert!(decoder.is_plain(&entry));
        assert_eq!(
            decoder.decode(chunk, 0, b"as is".to_vec()).unwrap(),
            b"as is"
        );
    }

    #[test]
    fn the_kek_comes_from_hex_a_secret_or_the_filer() {
        let f = fixture();
        let entry = sse_entry(&f, false);
        let plain = un(&f.plaintext);
        let ciphertext = un(&f.chunks[0].ciphertext);
        let v1 = f.kek_wrapped_v1.as_bytes();
        let v2 = f.kek_wrapped_v2.as_bytes();
        let hex_file = f.kek_hex.as_bytes();
        let passphrase = Some(f.kek_passphrase.clone());
        let hex = Some(f.kek_hex.clone());
        let secret = Some(f.kek_secret.clone());
        let cases: Vec<(&str, SseConfig, Option<&[u8]>)> = vec![
            (
                "hex",
                SseConfig {
                    kek_hex: hex.clone(),
                    ..Default::default()
                },
                None,
            ),
            (
                "hex matching the filer",
                SseConfig {
                    kek_hex: hex.clone(),
                    ..Default::default()
                },
                Some(hex_file),
            ),
            (
                "secret",
                SseConfig {
                    key: secret.clone(),
                    ..Default::default()
                },
                None,
            ),
            ("filer hex", SseConfig::default(), Some(hex_file)),
            (
                "filer v2",
                SseConfig {
                    passphrase: passphrase.clone(),
                    ..Default::default()
                },
                Some(v2),
            ),
            (
                "filer v1",
                SseConfig {
                    passphrase: passphrase.clone(),
                    ..Default::default()
                },
                Some(v1),
            ),
            (
                "filer hex with a passphrase",
                SseConfig {
                    passphrase: passphrase.clone(),
                    ..Default::default()
                },
                Some(hex_file),
            ),
        ];
        for (case, config, file) in cases {
            let decoder = EntryDecoder::new(keys(config, file), &entry).unwrap();
            let got = decoder
                .decode(&entry.chunks[0], 0, ciphertext.clone())
                .unwrap();
            assert_eq!(got, &plain[..ciphertext.len()], "{case}");
        }

        let refused: Vec<(&str, SseConfig, Option<&[u8]>)> = vec![
            (
                "both",
                SseConfig {
                    kek_hex: hex.clone(),
                    key: secret.clone(),
                    ..Default::default()
                },
                None,
            ),
            (
                "secret beside a filer file",
                SseConfig {
                    key: secret.clone(),
                    ..Default::default()
                },
                Some(hex_file),
            ),
            (
                "hex differing from the filer",
                SseConfig {
                    kek_hex: Some("00".repeat(32)),
                    ..Default::default()
                },
                Some(hex_file),
            ),
            (
                "wrong passphrase",
                SseConfig {
                    passphrase: Some("other".into()),
                    ..Default::default()
                },
                Some(v2),
            ),
            (
                "wrapped without a passphrase",
                SseConfig::default(),
                Some(v2),
            ),
            ("empty file", SseConfig::default(), Some(b"")),
        ];
        for (case, config, file) in refused {
            assert!(SseS3Keys::load(&config, file).is_err(), "{case}");
        }

        let unconfigured = keys(SseConfig::default(), None);
        assert!(!unconfigured.configured());
        let err = EntryDecoder::new(unconfigured.clone(), &entry)
            .unwrap()
            .decode(&entry.chunks[0], 0, ciphertext)
            .unwrap_err();
        assert!(format!("{err:#}").contains("not configured"), "{err:#}");
        assert!(EntryDecoder::new(unconfigured, &sse_entry(&f, true)).is_err());
    }

    #[test]
    fn a_tampered_iv_fails_the_commitment() {
        let f = fixture();
        let mut entry = sse_entry(&f, false);
        let meta = std::str::from_utf8(&entry.chunks[1].sse_metadata).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(meta).unwrap();
        value["iv"] = serde_json::Value::String(BASE64_STANDARD.encode(b"fedcba9876543210"));
        entry.chunks[1].sse_metadata = value.to_string().into_bytes();
        let decoder = EntryDecoder::new(from_secret(&f), &entry).unwrap();
        let err = decoder
            .decode(&entry.chunks[1], 0, un(&f.chunks[1].ciphertext))
            .unwrap_err();
        assert!(format!("{err:#}").contains("commitment"), "{err:#}");

        let mut without = value.clone();
        without.as_object_mut().unwrap().remove("keyCommitment");
        entry.chunks[1].sse_metadata = without.to_string().into_bytes();
        let strict = keys(
            SseConfig {
                key: Some(f.kek_secret.clone()),
                require_commitment: true,
                ..Default::default()
            },
            None,
        );
        assert!(
            EntryDecoder::new(strict, &entry)
                .unwrap()
                .decode(&entry.chunks[1], 0, un(&f.chunks[1].ciphertext))
                .is_err()
        );
    }

    #[test]
    fn filer_encrypted_chunks_are_decrypted_then_decompressed() {
        let f = fixture();
        let plain = un(&f.plaintext);
        let entry = Entry::default();
        let decoder = EntryDecoder::new(keys(SseConfig::default(), None), &entry).unwrap();
        // zstd is an Enterprise codec the fixture's generator cannot produce;
        // its sealed form is made here with the fixture's key.
        let zstd = zstd::encode_all(plain.as_slice(), 0).unwrap();
        let nonce = [7u8; GCM_NONCE_SIZE];
        let mut cipher_zstd = nonce.to_vec();
        cipher_zstd.extend(
            Aes256Gcm::new(un(&f.cipher_key).as_slice().into())
                .encrypt(&Nonce::from(nonce), zstd.as_slice())
                .unwrap(),
        );
        for (case, data, compressed) in [
            ("plain", un(&f.cipher_plain), false),
            ("gzip", un(&f.cipher_gzip), true),
            ("zstd", cipher_zstd, true),
        ] {
            let chunk = FileChunk {
                file_id: "3,01".into(),
                size: plain.len() as u64,
                cipher_key: un(&f.cipher_key),
                is_compressed: compressed,
                ..Default::default()
            };
            assert!(decoder.needs_whole_chunk(&chunk));
            assert_eq!(decoder.decode(&chunk, 0, data).unwrap(), plain, "{case}");
        }
        let wrong_key = FileChunk {
            cipher_key: vec![1; 32],
            ..Default::default()
        };
        assert!(decoder.decode(&wrong_key, 0, un(&f.cipher_plain)).is_err());
    }

    #[test]
    fn manifests_in_an_encrypted_object_only_undo_the_filers_encryption() {
        let f = fixture();
        let plain = un(&f.plaintext);
        let entry = sse_entry(&f, true);
        let decoder = EntryDecoder::new(from_secret(&f), &entry).unwrap();
        let manifest = FileChunk {
            file_id: "3,0m".into(),
            is_chunk_manifest: true,
            cipher_key: un(&f.cipher_key),
            is_compressed: true,
            ..Default::default()
        };
        assert_eq!(
            decoder.decode(&manifest, 0, un(&f.cipher_gzip)).unwrap(),
            plain
        );
        let plain_manifest = FileChunk {
            is_chunk_manifest: true,
            ..Default::default()
        };
        assert_eq!(
            decoder
                .decode(&plain_manifest, 0, b"proto".to_vec())
                .unwrap(),
            b"proto"
        );
    }

    #[test]
    fn customer_and_kms_keys_are_refused() {
        let decoder =
            EntryDecoder::new(keys(SseConfig::default(), None), &Entry::default()).unwrap();
        for sse_type in [SSE_C, SSE_KMS, 9] {
            let chunk = FileChunk {
                sse_type,
                ..Default::default()
            };
            assert!(
                decoder.decode(&chunk, 0, vec![0; 16]).is_err(),
                "{sse_type}"
            );
        }
    }
}
