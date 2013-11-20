#<< data/table

class data.ops.Offset extends data.Table
  constructor: (@table, @n) ->
    super
    @schema = @table.schema

  nrows: -> Math.max 0, @table.nrows() - @n
  children: -> [@table]

  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@table, @n) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @_next = null
        timer.start()
        @reset()

      reset: -> 
        @iter.reset()
        i = 0
        until i >= @n or not @iter.hasNext()
          @iter.next()
          i += 1

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        @iter.next()

      hasNext: -> @iter.hasNext()

      close: -> 
        @table = null
        @iter.close()
        timer.stop()

    new Iter @table, @n



