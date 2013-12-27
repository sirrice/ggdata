#<< data/table

class data.ops.Offset extends data.Table
  constructor: (@table, @n) ->
    super
    @schema = @table.schema
    @setProv()

  nrows: -> Math.max 0, @table.nrows() - @n
  children: -> [@table]

  iterator: ->
    tid = @id
    timer = @timer()
    class Iter
      constructor: (@table, @n) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @_next = null
        @_ret = new data.Row @schema
        @reset()

      reset: -> 
        @iter.reset()
        timer.start()
        i = 0
        until i >= @n or not @iter.hasNext()
          @iter.next()
          i += 1
        timer.stop()
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        @_ret.reset()
        @_ret.steal @iter.next()
        @_ret.id = data.Row.makeId tid, @idx-1
        @_ret

      hasNext: -> @iter.hasNext()

      close: -> 
        @table = null
        @iter.close()

    new Iter @table, @n



