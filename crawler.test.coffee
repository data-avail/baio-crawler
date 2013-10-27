craw = require "./crawler"


startOpts = (opts, onPop) ->
  craw.start opts, onPop, (err) ->
    console.log "started", err

start = (onPop) ->

  opts =
    amqp :
      config :
        url : "amqp://localhost"
      queue : "baio-crawler"
    log :
      level : 0
      write: (level, code, msg) ->
        console.log level, code, msg

  startOpts opts, onPop

onPopEmpty = (level, body, done) ->
  console.log "onPopEmpty", level
  done null, null

onPopFirstLevel = (level, body, done) ->
  console.log "onPopFirstLevel", level
  if level == -1
    done null, ["twitter.com"]
  else
    done null, null

onPopFirst5Levels = (level, body, done) ->
  console.log "onPopFirst5Levels", level
  if level < 5
    done null, ["twitter.com"]
  else
    done null, null

#start onPopEmpty
start onPopFirstLevel
#start onPopFirst5Levels
