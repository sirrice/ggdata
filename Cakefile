#
#
#
# Adapted from pwnall's dropship cakefile
#
#
#
async = require 'async'
{spawn, exec} = require 'child_process'
fs = require 'fs-extra'
glob = require 'glob'
log = console.log
path = require 'path'
remove = require 'remove'
  


coffeebin = "coffee" #"node_modules/coffee-script/bin/coffee"

# Node 0.6 compatibility hack.
unless fs.existsSync
  fs.existsSync = (filePath) -> path.existsSync filePath

task 'build', ->
  vendor ->
    build()



task 'release', ->
  vendor ->
    build ->
      release()

task 'vendor', ->
  vendor()

task 'test', ->
  build ->
      test()

task 'clean', ->
  clean()


release = (callback) ->
  commands = []
  async.forEachSeries commands, run, ->
    callback() if callback

build = (callback) ->
    create_build_dirs()

    commands = []
    commands.push 'toaster -f toaster.coffee -dc'
    commands.push 'ls build/compiled/*'
    commands.push "#{coffeebin} --output test/js --compile  test/data/*.coffee"
    commands.push 'cp build/compiled/ggdata.js lib/'
    commands.push 'rm -rf test/js'

    async.forEachSeries commands, run, ->
      callback() if callback


clean = (callback) ->
    fs.remove 'build',  ->
        fs.remove 'test/js', ->
          fs.remove 'release', ->
            fs.remove 'lib/gg*.js', ->
              fs.remove 'vendor', ->
                callback() if callback


test = (callback) ->
    commands = []
    commands.push "node_modules/vows/bin/vows"
    async.forEachSeries commands, run, ->

vendor = (callback) ->
  dirs = ['vendor', 'vendor/js', 'vendor/less', 'vendor/font', 'vendor/tmp']
  for dir in dirs
    fs.mkdirSync dir unless fs.existsSync dir

  downloads = [
    # D3
    ['http://d3js.org/d3.v3.min.js', 'vendor/js/d3.min.js'],

    # Zepto.js is a small subset of jQuery.
    ['http://code.jquery.com/jquery-1.9.0.min.js', 'vendor/js/jquery.min.js'],

    # JSON2
    ['https://raw.github.com/douglascrockford/JSON-js/master/json2.js',
     'vendor/js/json2.js'],

    # underscore.js
    ['http://underscorejs.org/underscore-min.js', 'vendor/js/underscore.min.js'],

    # SeedRandom (http://davidbau.com/archives/2010/01/30/random_seeds_coded_hints_and_quintillions.html)
    ['http://davidbau.com/encode/seedrandom.js', 'vendor/js/seedrandom.js'],

    # Require.js
    #['http://requirejs.org/docs/release/2.1.5/minified/require.js', 'vendor/js/require.js'],

    # science
    ["https://raw.github.com/jasondavies/science.js/master/science.v1.js", "vendor/js/science.js"]

  ]

  async.forEachSeries downloads, download, ->
    commands = []

    async.forEachSeries commands, run, ->
      commands = []
      callback() if callback




create_build_dirs = ->
  for dir in [
      'build',
      'build/compiled',
      'build/css',
      'build/js',
      'build/vendor',
      'build/vendor/js',
      'test/js']
    fs.mkdirSync dir unless fs.existsSync dir



run = (args...) ->
  for a in args
    switch typeof a
      when 'string' then command = a
      when 'object'
        if a instanceof Array then params = a
        else options = a
      when 'function' then callback = a

  command += ' ' + params.join ' ' if params?
  cmd = spawn '/bin/sh', ['-c', command], options
  cmd.stdout.on 'data', (data) -> process.stdout.write data
  cmd.stderr.on 'data', (data) -> process.stderr.write data
  process.on 'SIGHUP', -> cmd.kill()
  cmd.on 'exit', (code) -> callback() if callback? and code is 0




download = ([url, file], callback) ->
  if fs.existsSync file
    callback() if callback?
    return

  run "curl -L -o #{file} #{url}", callback




