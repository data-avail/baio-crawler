fs = require "fs"


#cached is locked, single lock per app
_isLocked = undefined

readFile = (path) ->
  if !fs.existsSync(path)
    fs.writeFileSync(path, "", "utf8")
  fs.readFileSync(path, "utf8")

readFileLines = (path) ->
  if !fs.existsSync(path)
    fs.writeFileSync(path, "", "utf8")
  fs.readFileSync(path, "utf8").split('\r\n')

#return was locked
exports.lock = (name) ->
  if _isLocked then return true
  #read / create file
  f = readFileLines(".crawler-skip-initial").filter((f) -> f == name)[0]
  if !f then fs.appendFileSync(".crawler-skip-initial", name + "\r\n")
  _isLocked = true
  return if f then true else false

exports.unlock = (name) ->
  if !_isLocked then return false
  f = readFileLines(".crawler-skip-initial").filter((f) -> f != name)
  fs.writeFileSync(".crawler-skip-initial", f.join("\r\n"))

