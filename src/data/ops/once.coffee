#<< data/table

# Ensures that child table only iterates once by caching the results
#
class data.ops.Once extends data.Table
  constructor: (@table) ->
    super
    @_arraytable = null
    @schema = @table.schema

  nrows: -> 
    if @_arraytable?
      @_arraytable.nrows()
    else
      @table.nrows()

  children: -> [@table]

  each: (f, n) ->
    if @_arraytable?
      @_arraytable.each f, n
    else
      super

  map: (f, n) ->
    if @_arraytable?
      @_arraytable.map f, n
    else
      super


  iterator: ->
    if @_arraytable?
      return @_arraytable.iterator()

    _rows = []
    _this = @
    class Iter
      constructor: (@table) ->
        @iter = @table.iterator()
        @tablecols = _.filter @table.schema.cols, (col) =>
          @table.schema.type(col) == data.Schema.table

      reset: -> @iter.reset()
      next: ->
        row = @iter.next()
        row = row.clone()
        for col in @tablecols
          v = row.get col
          row.set col, v.cache() if v?
        _rows.push row
        row

      hasNext: -> @iter.hasNext()
      close: -> 
        @iter.close()
        _this.arraytable = new data.ops.Array(
          @table.schema,
          _rows,
          [@table]
        )

    new Iter @table


