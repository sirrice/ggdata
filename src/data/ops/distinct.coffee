#<< data/table

class data.ops.Distinct extends data.Table
  constructor: (@table, @uniqCols=null) ->
    @schema = @table.schema
    @uniqCols ?= @schema.cols
    super

  timer: -> @table.timer()
  children: -> [@table]
  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@table, @cols) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @seen = {}
        @_next = new data.Row @schema
        @needNext = yes

      reset: -> 
        @iter.reset()
        @seen = {}

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        @needNext = yes
        @_next

      hasNext: -> 
        return true unless @needNext
        while @iter.hasNext()
          row = @iter.next()
          timer.start()
          vals = _.map @cols, (col) -> row.get(col)
          key = _.hashCode JSON.stringify vals
          unless key of @seen
            @seen[key] = null
            @_next.reset()
            @_next.steal row
            @needNext = no
            break
          timer.stop()
        not @needNext

      close: -> 
        @table = null
        @iter.close()
        @seen = {}

    new Iter @table, @uniqCols


