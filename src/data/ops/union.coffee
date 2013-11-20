#<< data/table

class data.ops.Union extends data.Table

  # @param arguments are all table or list of tables
  constructor: () ->
    super
    @tables = _.compact _.flatten arguments
    if @tables.length == 0
      console.log "[W] Union called with 0 tables."
      @schema = new data.Schema [], []
      @tables = [new data.RowTable(@schema)]
    @schema = @tables[0].schema
    @ensureSchema()

  ensureSchema: ->
    for table in @tables
      unless @schema.equals table.schema
        throw Error "Union table schemas don't match: #{@schema.toString()}  != #{table.schema.toString()}"

  nrows: -> 
    nrowsArr = _.map @tables, (t) -> t.nrows()
    _.reduce nrowsArr, ((a,b)->a+b), 0

  children: -> @tables
  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@schema, @tables) ->
        @reset()

      reset: -> 
        @tableidx = -1
        @iter = null
        timer.start()

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        @iter.next()

      hasNext: -> 
        if @tableidx >= @tables.length
          return no

        until @iter? and @iter.hasNext()
          @tableidx += 1
          if @tableidx >= @tables.length
            return no
          @iter.close() if @iter?
          @iter = @tables[@tableidx].iterator()

        yes

      close: -> 
        @iter.close() if @iter?
        timer.stop()

    new Iter @schema, @tables


