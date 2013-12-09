#<< data/table

# Ensures that child table only iterates once by caching the results
#
class data.ops.Once extends data.Table
  constructor: (@table) ->
    @_arraytable = null
    @schema = @table.schema
    super

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

    class Iter
      constructor: (@table) ->
        @iter = @table.iterator()
        @tablecols = _.filter @table.schema.cols, (col) =>
          @table.schema.type(col) == data.Schema.table
        timer.start()

      reset: -> @iter.reset()
      next: ->
        row = @iter.next()
        row = row.shallowClone()
        for col in @tablecols
          v = row.get col
          row.set col, v.cache() if v?
        _rows.push row

        unless @iter.hasNext()
          ondone()

        row

      hasNext: -> @iter.hasNext()
      close: -> 
        @iter.close()

    new Iter @table


