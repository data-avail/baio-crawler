async = require "async"

exports.LOG_LVL_HIGH = 1000
exports.LOG_LVL_MID = 500
exports.LOG_LVL_LOW = 0

#high level
exports.LOG_CODE_AMQP_CONNECT = "LOG_CODE_AMQP_CONNECT"
exports.LOG_CODE_PARSER_ERROR = "LOG_CODE_PARSER_ERROR"
exports.LOG_CODE_REQ_ERROR = "LOG_CODE_REQ_ERROR"

#mid level
exports.LOG_CODE_AMQP_PUSH = "LOG_CODE_AMQP_PUSH"
exports.LOG_CODE_AMQP_ON_POP = "LOG_CODE_AMQP_ON_POP"

#low level
exports.LOG_CODE_REQ = "LOG_CODE_REQ"
exports.LOG_CODE_REQ_RESP = "LOG_CODE_REQ_RESP"
exports.LOG_CODE_PARSE_LEVEL = "LOG_CODE_PARSE_LEVEL"
exports.LOG_CODE_PARSE_DATA = "LOG_CODE_PARSE_DATA"

_writers = []

_getLvl = (code) ->
  switch code
    when exports.LOG_CODE_AMQP_CONNECT then return exports.LOG_LVL_HIGH
    when exports.LOG_CODE_PARSER_ERROR then return exports.LOG_LVL_HIGH
    when exports.LOG_CODE_REQ_ERROR then return exports.LOG_LVL_HIGH
    when exports.LOG_CODE_AMQP_PUSH then return exports.LOG_LVL_MID
    when exports.LOG_CODE_AMQP_ON_POP then return exports.LOG_LVL_MID
    when exports.LOG_CODE_REQ then return exports.LOG_LVL_LOW
    when exports.LOG_CODE_REQ_RESP then return exports.LOG_LVL_LOW
    when exports.LOG_CODE_PARSE_LEVEL then return exports.LOG_LVL_LOW
    when exports.LOG_CODE_PARSE_DATA then return exports.LOG_LVL_LOW

consoleWriter = (lvl, code, msg) ->
  console.log lvl, code, msg

appendWriter = (writer) ->
  _writers.push writer

write = (lvl, code, msg) ->
  for wr in _writers
    if wr.level <= lvl
      wr.write lvl, code, msg

exports.setOpts = (opts, done) =>
  wrs = []
  wrs.push prop for own prop of opts
  async.map wrs, (name, ck) ->
    if name == "loggly"
      exports.getLoggly opts[name], (err, writer) ->
        ck err, {name : name, write : writer, level : opts[name].level}
    else if name == "console"
      ck null, (if opts[name] then {name : name, write : consoleWriter, level : opts[name].level}  else null)
    else
      ck null, {name : name, write : opts[name], level : 0}
  , (err, results) ->
    if !err
      appendWriter r for r in results.filter((f) -> f)
    done err

exports.write = (code, msg) ->
  exports.writeLvl _getLvl(code), code, msg

exports.writeLvl = (lvl, code, msg) ->
  write lvl, code, msg

exports.getLoggly = (opts, done) ->

  loggly = require "loggly"

  config =
    subdomain: opts.domain
    auth:
      username: opts.username
      password: opts.password
    json: true

  client = loggly.createClient config

  client.getInput opts.input, (err, input) ->
    if !err
      done err, (lvl, code, msg) ->
        input.log level : lvl, code : code, msg : msg
    else
      done err
