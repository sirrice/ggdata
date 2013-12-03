#<< data/table

class data.ops.Array extends data.Table
  constructor: (@schema, @rows, @_children=[]) ->
    throw Error("Array extends a schema") unless @schema?


  nrows: -> @rows.length
  children: -> @_children
  each: (f, n) ->
    n ?= @rows.length
    n = Math.min n, @rows.length
    for i in [0...n]
      f @rows[i], i

  map: (f, n) ->
    n ?= @rows.length
    n = Math.min n, @rows.length
    for i in [0...n]
      f @rows[i], i



  iterator: ->
    class Iter
      constructor: (@rows) ->
        @idx = 0

      reset: -> @idx = 0

      next: ->
        throw Error "iterator has no more items" unless @hasNext()
        @idx += 1
        @rows[@idx-1]

      hasNext: -> @idx < @rows.length

      close: -> 
        @rows = null
    new Iter @rows

