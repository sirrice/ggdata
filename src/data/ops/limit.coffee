#<< data/table

class data.ops.Limit extends data.Table
  constructor: (@table, @n) ->
    super
    @schema = @table.schema
    @setProv()

  nrows: ->
    Math.min @table.nrows(), @n

  children: -> [@table]

  iterator: ->
    tid = @id
    timer = @timer()
    class Iter
      constructor: (@table, @n) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @idx = 0
        @_ret = new data.Row @schema

      reset: -> 
        @iter.reset()
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        @idx += 1
        @_ret.reset()
        @_ret.steal @iter.next()
        @_ret.id = data.Row.makeId tid, @idx-1
        @_ret

      hasNext: -> @idx < @n and @iter.hasNext()

      close: -> 
        @table = null
        @iter.close()

    new Iter @table, @n



