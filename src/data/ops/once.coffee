#<< data/table

# Ensures that child table only iterates once by caching the results
#
class data.ops.Once extends data.Table
  constructor: (@table) ->
    super
    @_arraytable = null
    @schema = @table.schema
    @setProv()

  nrows: -> 
    if @_arraytable?
      @_arraytable.nrows()
    else
      @table.nrows()

  children: -> [@table]

  each: (f, n) ->
    if @_arraytable?
      data.Table.timer.start("#{@name}-#{@id}-each")
      ret = @_arraytable.each f, n
      data.Table.timer.stop("#{@name}-#{@id}-each")
      ret
    else
      super

  map: (f, n) ->
    if @_arraytable?
      data.Table.timer.start("#{@name}-#{@id}-map")
      ret = @_arraytable.map f, n
      data.Table.timer.stop("#{@name}-#{@id}-map")
      ret
    else
      super


  iterator: ->
    if @_arraytable?
      return @_arraytable.iterator()

    timer = @timer()
    _rows = []
    ondone = () =>
      timer.stop()
      @_arraytable = new data.ops.Array(
        @schema,
        _rows,
        [@table]
      )

    tid = @id
    class Iter
      constructor: (@table) ->
        @iter = @table.iterator()
        @tablecols = _.filter @table.schema.cols, (col) =>
          @table.schema.type(col) == data.Schema.table
        timer.start()
        @idx = 0

      reset: -> 
        @iter.reset()
        @idx = 0

      next: ->
        @idx += 1
        row = @iter.next().shallowClone()
        for col in @tablecols
          v = row.get col
          row.set col, v.cache() if v?
        row.id = data.Row.makeId tid, @idx-1
        _rows.push row

        unless @iter.hasNext()
          ondone()

        row

      hasNext: -> @iter.hasNext()
      close: -> 
        @iter.close()

    new Iter @table


