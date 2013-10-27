async = require "async"
req = require "request"
amqp = require "../baio-amqp/amqp"
log = require "./logs"
skipInitialState = require "./skip-initial-state"

_opts = null
_parse = null

doneLog = (errCode, done) ->
  (err, arg1) ->
    if err
      log.write errCode, err
    done err, arg1

#send to amqp queue
push2Amqp = (level, urls) ->
  if urls and urls.length
    for url in urls
      level = url.level if url.level != undefined
      log.write log.LOG_CODE_AMQP_PUSH, {level : level, url : url}
      amqp.pub _opts.amqp.queue, level : level, url : url

#make request
request = (url, level, done) ->

  if typeof url == "object"
    opts = url.request
  else
    opts =
      url : "http://" + url if ! url.match /https?:\/\//
      method : "get"

  log.write log.LOG_CODE_REQ, opts
  #req opts, doneLog(log.LOG_CODE_REQ_ERROR, done)
  query opts, level, doneLog(log.LOG_CODE_REQ_ERROR, done)

webQuery = (opts, done) ->
  opts.qs.query = opts.qs.query.replace /([^('])'([^)])/g, "$1\\'$2"
  opts.qs.query = opts.qs.query.replace /<([^>]*)>/g, "iri('$1')"

  console.log opts.qs.query
  req opts, (err, resp, body) ->
    done err, body

query = (opts, level, done) ->
  q = _opts.query? level
  q ?= webQuery
  q opts, done

isSkipInitial = ->
  if _opts.skipInitial.val == null
    wasLocked = skipInitialState.lock _opts.skipInitial.name
    #if initial state was locked already, skip initial
    return wasLocked
  else
    skipInitialState.unlock _opts.skipInitial.name
    return _opts.skipInitial.val

requestAndParse = (level, url, done) ->
  #here make request, send repsonse body to _opts.parse, push urls returned from parser
  async.waterfall [
    (ck) ->
        request url, level, ck
    (body, ck) ->
      log.write log.LOG_CODE_REQ_RESP, body
      if typeof url == "object"
        data = url.data
      _parse level, body, data, doneLog(log.LOG_CODE_PARSER_ERROR, ck)
  ], done

parseLevel = (level, url, done) ->
  _done = (err, links) ->
    if !err and _opts.slaveLevel == -1
      push2Amqp level + 1, links
    done err, links
  log.write log.LOG_CODE_PARSE_LEVEL, level : level, url : url
  if level != -1
      requestAndParse level, url, _done
  else
    if !isSkipInitial()
      _parse level, null, null, doneLog(log.LOG_CODE_PARSER_ERROR, _done)
    else
      _done()

parseData = (data, done) ->
  _done = (err, links) ->
    if !err and _opts.slaveLevel == -1
      push2Amqp null, links
    done err, links
  log.write log.LOG_CODE_PARSE_DATA, data : data
  _parse null, null, data, doneLog(log.LOG_CODE_PARSER_ERROR, _done)

start = (opts, parse, done) ->
  _opts = opts
  _opts.slaveLevel ?= -1
  _parse = parse
  amqp.setConfig opts.amqp.config
  log.setOpts opts.log, (err) ->
    if !err
      amqp.connect ->
        amqp.sub {
          queue : opts.amqp.queue
          onPop: (data, ack) ->
            if _opts.slaveLevel == -1 or data.level == _opts.slaveLevel
              log.write log.LOG_CODE_AMQP_ON_POP, data
              if data.level != undefined
                parseLevel data.level, data.url, ack
              else
                parseData data, ack
            else
              #slave mode, ignore messages not from the slave level
              ack(true)
        }
          , (err) ->
            log.write log.LOG_CODE_AMQP_CONNECT, {err : err, opts, opts : JSON.stringify(_opts)}
            done err
            if !err
              if _opts.slaveLevel == -1
                parseLevel -1, null, ->
    else
      throw err

exports.start = start
