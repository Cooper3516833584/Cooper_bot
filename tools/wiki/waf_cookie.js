// Solve the Aliyun WAF JS challenge used by api.1037.wiki.
// The challenge script is executed in a minimal vm context.  It has no access
// to require/process/network; only the cookie it sets is returned.
const vm = require("vm");
const fs = require("fs");

const code = fs.readFileSync(process.argv[2], "utf8");
const context = {
  console,
  document: {
    cookie: "",
    location: { reload: () => { throw new Error("RELOAD"); } },
  },
  window: {},
  navigator: {
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
      "AppleWebKit/537.36 (KHTML, like Gecko) " +
      "Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
  },
  location: { reload: () => { throw new Error("RELOAD"); } },
  setTimeout,
  clearTimeout,
  Date,
  Math,
  RegExp,
  String,
  Number,
  Boolean,
  parseInt,
  parseFloat,
  decodeURIComponent,
  encodeURIComponent,
  isNaN,
  Error,
};

try {
  vm.runInNewContext(code, context, { timeout: 5000 });
} catch (error) {
  if (error.message !== "RELOAD") {
    process.stderr.write(String(error.message || error));
  }
}
process.stdout.write(String(context.document.cookie || ""));
