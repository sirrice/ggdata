#<< data/table

class data.ops.OrderBy extends data.Table

  constructor: (@table, @sortCols, @reverse=no) ->
    super
    schema = @schema = @table.schema
    cols = _.flatten [@sortCols]
    colidxs = (schema.index(col) for col in cols)
    reverse = if @reverse then -1 else 1
    @cmp = (r1, r2) ->
      for colidx in colidxs
        v1 = r1.data[colidx]
        v1 = null if v1 == undefined
        v2 = r2.data[colidx]
        v2 = null if v2 == undefined

        if v1 > v2
          return 1 * reverse
        if v1 < v2
          return -1 * reverse
      return 0

    @setProv()


  nrows: -> @table.nrows()
  children: -> [@table]

  iterator: ->
    tid = @id
    timer = @timer()
    class Iter
      constructor: (@table, @cmp) ->
        @rows = null
        @schema = @table.schema
        @idx = 0

      reset: -> 
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        @idx += 1
        ret = @rows[@idx - 1]
        ret.id = data.Row.makeId tid, @idx-1
        ret

      hasNext: -> 
        unless @rows?
          @rows = @table.all()
          timer.start()
          @rows.sort @cmp
          timer.stop()
        @idx < @rows.length

      close: -> 
        @table = null
        @rows = null

    new Iter @table, @cmp
