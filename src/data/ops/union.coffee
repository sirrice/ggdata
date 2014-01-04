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
    @ensureSchema()
    @setProv()

  ensureSchema: ->
    @schema = data.Schema.merge _.map(@tables, (t)->t.schema)
    for table, idx in @tables
      unless @schema.equals table.schema
        schemahas = _.reject @schema.cols, (col) -> table.has col
        tablehas = _.reject table.cols, (col) => @schema.has col
        if tablehas.length > 0
          console.log "Union contains cols table #{idx} doesn't have: #{schemahas}"
          console.log "table #{idx} contains cols Union doesn't have: #{tablehas}"
          throw Error "Union table schemas don't match: #{@schema.toString()}  != #{table.schema.toString()}"

  nrows: -> 
    nrowsArr = _.map @tables, (t) -> t.nrows()
    _.reduce nrowsArr, ((a,b)->a+b), 0

  children: -> @tables
  iterator: ->
    tid = @id
    timer = @timer()
    class Iter
      constructor: (@schema, @tables) ->
        @_row = new data.Row @schema
        @idx = 0
        @reset()

      reset: -> 
        @tableidx = -1
        @idx = 0
        @iter = null
        timer.start()

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        @idx += 1
        row = @iter.next()
        timer.start()
        @_row.reset()
        @_row.steal row
        @_row.id = data.Row.makeId tid, @idx-1
        timer.stop()
        @_row

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

    new Iter @schema, @tables


