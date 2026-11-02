// Legacy ESLint and Redocly callers require a function with Minimatch properties.
// eslint-disable-next-line @typescript-eslint/no-require-imports -- the consumers require CommonJS
module.exports = require("upstream-minimatch").minimatch;
