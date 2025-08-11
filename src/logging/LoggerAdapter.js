class LoggerAdapter {
  // Host app should implement these methods. Fallbacks are no-op
  info(scope, method, message, meta = {}) {}
  warn(scope, method, message, meta = {}) {}
  error(scope, method, error, meta = {}) {}
}

module.exports = { LoggerAdapter }; 