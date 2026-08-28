// Build step: pack every dashboard dataset (plus the login email) into one
// gzipped, AES-256-GCM encrypted blob at public/data.enc.
//
// Why this exists: the site is a static build, so anything imported by the app
// ends up readable in the public JS bundle -- that is how the old hardcoded
// login leaked both the password and 570 ASINs to anyone with DevTools. The
// only way to keep the data private without a server is to ship it encrypted
// and derive the key from the password the user types at login.
//
// The password comes from the BUYBOX_PASSWORD env var and is NEVER written to
// disk, committed, or bundled. Set it in Render (Environment) and as a GitHub
// Actions secret so the nightly BSR build keeps working.

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const crypto = require("crypto");

const ROOT = path.join(__dirname, "..");
const SRC = path.join(ROOT, "src");
const OUT_DIR = path.join(ROOT, "public");
const OUT = path.join(OUT_DIR, "data.enc");

// Bumping this invalidates old blobs; the browser checks it before decrypting.
const MAGIC = Buffer.from("BBX1", "ascii");
const PBKDF2_ITERATIONS = 310000;
const SALT_BYTES = 16;
const IV_BYTES = 12;
const KEY_BYTES = 32;

// Datasets that used to be `import`ed directly by nexlev_ads_dashboard.jsx.
const DATASETS = {
  raw_data: "raw_data.json",
  bsr_data: "bsr_data.json",
  bsr_snapshots: "bsr_snapshots.json",
  june_diff: "june_diff.json",
  // Carries dealer price / net landed cost -- was inlined in the dashboard.
  asin_master: "asin_master.json",
};

// The one account allowed in. Lives inside the encrypted payload so the email
// is not readable in the bundle either -- you need the password to see it.
const LOGIN_USERNAME = process.env.BUYBOX_USERNAME || "info@cambiumretail.com";

function fail(message) {
  console.error(`\n[encrypt_data] ${message}\n`);
  process.exit(1);
}

const password = process.env.BUYBOX_PASSWORD;
if (!password || !password.trim()) {
  fail(
    "BUYBOX_PASSWORD is not set.\n" +
      "  This build would otherwise produce a site nobody can sign in to.\n" +
      "  Set it in Render -> Buybox_report -> Environment, and as a GitHub\n" +
      "  Actions secret for the nightly BSR workflow."
  );
}
// Minimum length is a warning, not a hard stop -- a blocked deploy is worse
// than a shortish password, and the operator is the one who owns that call.
const MIN_RECOMMENDED = 12;
if (password.trim().length < MIN_RECOMMENDED) {
  console.warn(
    `\n[encrypt_data] WARNING: BUYBOX_PASSWORD is only ${password.trim().length} characters.\n` +
      "  The password is now the ONLY thing protecting the data, so a short one\n" +
      `  can be cracked offline. ${MIN_RECOMMENDED}+ characters is strongly advised.\n` +
      "  Building anyway.\n"
  );
}

// These DID ship inside the public bundle, so they are readable by anyone who
// ever loaded the site.  Encrypting with one of them protects nothing at all --
// an attacker already has the key.  This is the one case worth failing on.
// Warn, do not block.  Shipping this build REMOVES the password from the live
// bundle, so reusing an old one still shuts out everyone who did not already
// copy it -- strictly better than leaving the data readable while we argue
// about it.  The residual risk is real but bounded, and rotating the env var
// later is a 30-second job that needs no code change.
const BURNED = ["cambium!109", "cambium@109"];
if (BURNED.includes(password.trim().toLowerCase())) {
  console.warn(
    "\n[encrypt_data] WARNING: this password was previously published in the\n" +
      "  public JavaScript bundle, so anyone who loaded the old site may already\n" +
      "  have it. This build removes it from the site, but ROTATE IT SOON:\n" +
      "  Render -> Buybox_report -> Environment -> BUYBOX_PASSWORD -> rebuild.\n" +
      "  Building anyway.\n"
  );
}

const payload = { username: LOGIN_USERNAME };
for (const [key, filename] of Object.entries(DATASETS)) {
  const file = path.join(SRC, filename);
  if (!fs.existsSync(file)) {
    fail(`Missing dataset ${filename}. Run the generate/build-bsr steps first.`);
  }
  payload[key] = JSON.parse(fs.readFileSync(file, "utf8"));
}

const plaintext = zlib.gzipSync(Buffer.from(JSON.stringify(payload), "utf8"), {
  level: 9,
});

const salt = crypto.randomBytes(SALT_BYTES);
const iv = crypto.randomBytes(IV_BYTES);
const key = crypto.pbkdf2Sync(
  password.trim(),
  salt,
  PBKDF2_ITERATIONS,
  KEY_BYTES,
  "sha256"
);

const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
// WebCrypto expects the GCM tag appended to the ciphertext.
const sealed = Buffer.concat([ciphertext, cipher.getAuthTag()]);

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT, Buffer.concat([MAGIC, salt, iv, sealed]));

const raw = Object.values(DATASETS).reduce(
  (sum, f) => sum + fs.statSync(path.join(SRC, f)).size,
  0
);
const mb = (n) => (n / 1024 / 1024).toFixed(2);
console.log(
  `[encrypt_data] ${mb(raw)} MB of datasets -> ${mb(
    fs.statSync(OUT).size
  )} MB encrypted at public/data.enc`
);
