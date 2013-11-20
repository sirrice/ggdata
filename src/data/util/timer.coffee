try
  timer = performance
catch err
  timer = Date

class data.util.Timer
  constructor: ->
    @_starts = {}
    @_timings = {}

  start: (name=null)->
    @_starts[name] = timer.now()

  stop: (name=null) -> @end name

  end: (name=null) ->
    cost = -1
    start = @_starts[name]
    if start?
      cost = timer.now() - start
      unless name of @_timings
        @_timings[name] = []
      @_timings[name].push cost

    @_starts[name] = null
    cost


  names: ->
    _.keys @_timings

  timings: (name=null) -> 
    if @_timings[name]?
      @_timings[name]
    else
      []

  avg: (name=null) ->
    times = @timings name
    d3.mean times

