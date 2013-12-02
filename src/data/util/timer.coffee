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

  has: (name=null) -> name of @_timings

  names: ->
    _.keys @_timings

  timings: (name=null) -> 
    if @_timings[name]?
      @_timings[name]
    else
      []

  avg: (name=null) ->
    times = @timings name
    if times.length > 0
      d3.mean times
    else
      NaN

  count: (name=null) ->
    @timings(name).length

  sum: (name=null) ->
    times = @timings name
    if times.length > 0
      d3.sum times
    else
      NaN




