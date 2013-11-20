#<< data/table

class data.ops.Limit extends data.Table
  constructor: (@table, @n) ->
    super
    @schema = @table.schema

  nrows: ->
    Math.min @table.nrows(), @n

  children: -> [@table]

  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@table, @n) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @idx = 0
        timer.start()

      reset: -> 
        @iter.reset()
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        @idx += 1
        @iter.next()

      hasNext: -> @idx < @n and @iter.hasNext()

      close: -> 
        @table = null
        @iter.close()
        timer.stop()

    new Iter @table, @n



