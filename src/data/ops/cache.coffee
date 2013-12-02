#<< data/table

class data.ops.Cache extends data.Table
  constructor: (@table) ->
    super
    @schema = @table.schema
    timer = @timer()
    timer.start()
    @setup()
    timer.stop()

  nrows: -> @_rows.length
  children: -> [@table]

  setup: ->
    tablecols = _.filter @table.schema.cols, (col) =>
      @table.schema.type(col) == data.Schema.table
    rows = @table.map (row) ->
      row = row.shallowClone()
      for col in tablecols
        if row.get(col)?
          row.set col, row.get(col).cache()
      row
    @_rows = rows

  each: (f, n) ->
    n ?= @_rows.length
    n = Math.min n, @_rows.length
    for i in [0...n]
      f @_rows[i], i

  map: (f, n) ->
    n ?= @_rows.length
    n = Math.min n, @_rows.length
    for i in [0...n]
      f @_rows[i], i

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
    new Iter @_rows

  
