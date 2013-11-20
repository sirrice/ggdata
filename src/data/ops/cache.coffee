#<< data/table

class data.ops.Cache extends data.Table
  constructor: (@table) ->
    super
    @schema = @table.schema
    timer = @timer()
    @_rows = @table.all()
    timer.stop()

  nrows: -> @_rows.length
  children: -> [@table]

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

  
