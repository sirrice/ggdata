#<< data/table

class data.ops.Distinct extends data.Table
  constructor: (@table, @uniqCols=null) ->
    super
    @schema = @table.schema
    @uniqCols ?= @schema.cols

  children: -> [@table]
  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@table, @cols) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @seen = {}
        @_next = null
        timer.start()

      reset: -> 
        @iter.reset()
        @seen = {}

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        ret = @_next
        @_next = null
        ret

      hasNext: -> 
        return true if @_next?
        while @iter.hasNext()
          row = @iter.next()
          vals = _.map @cols, (col) -> row.get(col)
          key = _.hashCode JSON.stringify vals
          unless key of @seen
            @seen[key] = null
            @_next = row
            break
        @_next?

      close: -> 
        @table = null
        @iter.close()
        @seen = {}
        timer.stop()

    new Iter @table, @uniqCols


