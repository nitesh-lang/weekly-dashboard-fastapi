// Fetch + decrypt the dashboard datasets.
//
// public/data.enc is produced by Scripts/encrypt_data.js at build time and has
// the layout:  "BBX1" | salt(16) | iv(12) | AES-256-GCM(gzip(JSON)) + tag(16)
//
// The key is derived from the typed password with PBKDF2-SHA256, so a wrong
// password simply fails to decrypt -- there is no stored hash to compare
// against and nothing readable in the bundle to bypass.

import { setDatasets, clearDatasets } from "./dataStore";

const DATA_URL = `${import.meta.env.BASE_URL || "/"}data.enc`;
const MAGIC = "BBX1";
const PBKDF2_ITERATIONS = 310000;
const SALT_BYTES = 16;
const IV_BYTES = 12;

// The encrypted blob is inert without the key, so caching it is safe and saves
// a re-download when someone mistypes their password.
let blobCache = null;

export class UnlockError extends Error {
  constructor(message, { fatal = false } = {}) {
    super(message);
    this.name = "UnlockError";
    // fatal = something is wrong with the deploy, not with what was typed.
    this.fatal = fatal;
  }
}

function assertCryptoSupport() {
  if (!window.crypto?.subtle) {
    throw new UnlockError(
      "This browser cannot decrypt the dashboard. Use a current version of Chrome, Edge, Firefox or Safari.",
      { fatal: true }
    );
  }
  if (typeof DecompressionStream === "undefined") {
    throw new UnlockError(
      "This browser is too old to open the dashboard. Please update it.",
      { fatal: true }
    );
  }
}

async function fetchBlob() {
  if (blobCache) return blobCache;

  let response;
  try {
    response = await fetch(DATA_URL, { cache: "no-cache" });
  } catch {
    throw new UnlockError("Could not reach the data file. Check your connection and retry.", {
      fatal: true,
    });
  }
  if (!response.ok) {
    throw new UnlockError(
      `Data file missing (HTTP ${response.status}). The site was built without BUYBOX_PASSWORD set.`,
      { fatal: true }
    );
  }

  const bytes = new Uint8Array(await response.arrayBuffer());
  const header = String.fromCharCode(...bytes.slice(0, MAGIC.length));
  if (header !== MAGIC) {
    throw new UnlockError("Data file is corrupt or from an incompatible build.", { fatal: true });
  }

  let offset = MAGIC.length;
  const salt = bytes.slice(offset, (offset += SALT_BYTES));
  const iv = bytes.slice(offset, (offset += IV_BYTES));
  const sealed = bytes.slice(offset);

  blobCache = { salt, iv, sealed };
  return blobCache;
}

async function deriveKeyBytes(password, salt) {
  const baseKey = await window.crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(password),
    "PBKDF2",
    false,
    ["deriveBits"]
  );
  const bits = await window.crypto.subtle.deriveBits(
    { name: "PBKDF2", salt, iterations: PBKDF2_ITERATIONS, hash: "SHA-256" },
    baseKey,
    256
  );
  return new Uint8Array(bits);
}

async function gunzip(buffer) {
  const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Response(stream).text();
}

async function decryptWith(keyBytes) {
  const { iv, sealed } = await fetchBlob();

  const key = await window.crypto.subtle.importKey("raw", keyBytes, "AES-GCM", false, ["decrypt"]);

  let plaintext;
  try {
    plaintext = await window.crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, sealed);
  } catch {
    // GCM tag mismatch -- the only realistic cause is a wrong password.
    throw new UnlockError("Invalid username or password. Contact your admin if you need access.");
  }

  const payload = JSON.parse(await gunzip(plaintext));
  setDatasets(payload);
  return payload;
}

/**
 * Unlock with a typed password. Returns the derived key bytes so the caller can
 * keep the session alive across reloads WITHOUT storing the password anywhere.
 */
export async function unlockWithPassword(password) {
  assertCryptoSupport();
  const { salt } = await fetchBlob();
  const keyBytes = await deriveKeyBytes(password, salt);
  const payload = await decryptWith(keyBytes);
  return { payload, keyBytes };
}

/** Restore a session from previously derived key bytes (see sessionStorage use in App.jsx). */
export async function unlockWithKey(keyBytes) {
  assertCryptoSupport();
  return decryptWith(keyBytes);
}

export function lock() {
  clearDatasets();
}

export function bytesToBase64(bytes) {
  return btoa(String.fromCharCode(...bytes));
}

export function base64ToBytes(value) {
  return Uint8Array.from(atob(value), (char) => char.charCodeAt(0));
}
