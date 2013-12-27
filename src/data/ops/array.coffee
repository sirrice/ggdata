#<< data/table

class data.ops.Array extends data.Table
  constructor: (@schema, @rows, @_children=[]) ->
    throw Error("Array extends a schema") unless @schema?
    super
    @setProv()


  nrows: -> @rows.length
  children: -> @_children
  each: (f, n) ->
    data.Table.timer.start("#{@name}-#{@id}-each")
    n ?= @rows.length
    n = Math.min n, @rows.length
    ret = for i in [0...n]
      f @rows[i], i
    data.Table.timer.stop("#{@name}-#{@id}-each")
    ret

  map: (f, n) ->
    data.Table.timer.start("#{@name}-#{@id}-map")
    n ?= @rows.length
    n = Math.min n, @rows.length
    ret = for i in [0...n]
      f @rows[i], i
    data.Table.timer.stop("#{@name}-#{@id}-map")
    ret



  iterator: ->
    tid = @id
    class Iter
      constructor: (@schema, @rows) ->
        @idx = 0

      reset: -> 
        @idx = 0

      next: ->
        throw Error "iterator has no more items" unless @hasNext()
        @idx += 1
        @rows[@idx-1].id = data.Row.makeId tid, @idx - 1
        @rows[@idx-1]

      hasNext: -> @idx < @rows.length

      close: -> 
        @rows = null
    new Iter @schema, @rows

