// Neutralized stub for pg-cloudflare.
//
// `pg` declares pg-cloudflare as an OPTIONAL dependency and only require()s it
// from its Cloudflare Workers stream path (pg/lib/stream.js getCloudflareStreamFuncs).
// This project runs on Vercel/Node, where that path is never taken, so the real
// package is dead weight. An npm `overrides` entry in package.json points
// pg-cloudflare at this stub. If a Cloudflare Workers deploy is ever added, drop
// the override and reinstall to restore the real package.
class CloudflareSocket {
  constructor() {
    throw new Error(
      "pg-cloudflare is stubbed out in this project (Node/Vercel only). " +
        "Remove the pg-cloudflare override in package.json to use it on Cloudflare Workers.",
    );
  }
}

module.exports = { CloudflareSocket };
