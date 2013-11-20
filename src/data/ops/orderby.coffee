#<< data/table

class data.ops.OrderBy extends data.Table

  constructor: (@table, @sortCols, @reverse=no) ->
    super
    @schema = @table.schema
    cols = _.flatten [@sortCols]
    reverse = if @reverse then -1 else 1
    @cmp = (r1, r2) ->
      for col in cols
        continue unless r1.has(col) and r2.has(col)
        if r1.get(col) > r2.get(col)
          return 1 * reverse
        if r1.get(col) < r2.get(col)
          return -1 * reverse
      return 0

  nrows: -> @table.nrows()
  children: -> [@table]

  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@table, @cmp) ->
        @rows = null
        @schema = @table.schema
        @idx = 0
        timer.start()

      reset: -> 
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        @idx += 1
        @rows[@idx - 1]

      hasNext: -> 
        unless @rows?
          @rows = @table.all()
          @rows.sort @cmp
        @idx < @rows.length

      close: -> 
        @table = null
        @rows = null
        timer.stop()

    new Iter @table, @cmp
