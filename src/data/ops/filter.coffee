#<< data/table

class data.ops.Filter extends data.Table
  constructor: (@table, @f) ->
    @schema = @table.schema
    super

  children: -> [@table]

  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@table, @f) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @_next = null
        timer.start()

      reset: -> @iter.reset()
      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        ret = @_next
        @_next = null
        ret

      hasNext: -> 
        return true if @_next?
        while @iter.hasNext()
          row = @iter.next()
          if @f row
            @_next = row
            break
        @_next?

      close: -> 
        @table = null
        @iter.close()
        timer.stop()

    new Iter @table, @f



