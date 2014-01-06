#<< data/table

class data.PartitionedTable extends data.Table

  # @param table [partition cols..., table]
  constructor: (@table, @partcols=[], @schema) ->
    super

    @tablecols = @table.cols().filter (col) => @table.schema.type(col) == data.Schema.table

    if @tablecols.length == 0
      @schema = @table.schema
      schema = new data.Schema ['table'], [data.Schema.table]
      table = new data.RowTable schema, [[@table]]
      @table = table
      @tablecols = ['table']

    if @tablecols.length > 1
      throw Error


    for col in @partcols
      unless @table.has col
        throw Error

    @tablecol = @tablecols[0]

    @schema ?= @table.any(@tablecol)
    unless @schema?
      throw Error

    unless _.isType @table, data.ops.Array
      @table = @table.cache()

    @setProv()


  dummyPartition: -> new data.RowTable @schema
  children: -> @table.all @tablecol
  nrows: ->
    n = 0
    for t in @table.all(@tablecol)
      n += t.nrows()
    n


  partition: (cols, alias='table', complete=no) ->
    cols = _.flatten [cols]
    rowdatas = for [key, t] in @partitions cols, complete
      cols.map((c) -> key.get c).concat [t]
    schema = @table.schema.project cols
    schema.addColumn alias, data.Schema.table
    new data.RowTable(schema, rowdatas)

  partitions: (cols, complete=yes) ->
    if _.difference(cols, @partcols).length > 0
      @partitionOn(cols, complete).partitions cols, complete
    else if _.difference(@partcols, cols).length > 0
      part = @table.partition cols, 'table', complete
      part.map (row) =>
        t = row.get('table')
        [row.project(cols), new @constructor(t, @partcols, @schema)]
    else
      @table.map (row) =>
        t = new data.ops.Array(@table.schema, [row])
        [row.project(cols), new @constructor(t, @partcols, @schema)]

  partitionOn: (cols, complete=yes) ->
    cols = _.compact _.flatten [cols]
    diffcols = _.difference(cols, @partcols)
    if diffcols.length > 0
      for col in diffcols
        unless col in @schema.cols
          throw Error "#{col} is not in schema #{@schema.cols}"

      cols = _.union cols, @partcols
      newschema = null
      newrows = []
      @table.each (row) =>
        p = row.get(@tablecol).partition(cols, @tablecol, complete)
        newschema ?= p.schema
        newrows.push.apply newrows, p.all()

      newtable = new data.ops.Array newschema, newrows
      new @constructor newtable, cols, @schema
    else
      @

  addTag: (col, val, type=null) ->
    type ?= data.Schema.type val
    newtable = @table.setColVal col, val, type
    new @constructor newtable, _.union(@partcols, [col]), @schema

  rmTag: (col) ->
    newtable = @table.exclude col
    new @constructor newtable, @partcols.without(col), @schema

  @fromTables: (tables) ->
    tables = _.flatten [tables]
    cols = {}
    tables = for t in tables
      if _.isType t, data.PartitionedTable
        for col in t.partcols
          cols[col] = yes
        t
      else
        new data.PartitionedTable t, [], t.schema

    cols = _.keys cols
    tables = for t in tables
      t.partitionOn cols

    new data.PartitionedTable(
      new data.ops.Union tables
    )

  iterator: ->
    tid = @id
    _me = @
    class Iter
      constructor: (@table, @tablecol) ->
        @iter = @table.iterator()
        @inner = null
        @idx = 0

      reset: ->
        @idx = 0
        @iter.reset()

      next: ->
        throw Error unless @hasNext()
        @idx += 1
        row =  @inner.next().shallowClone()
        row.id = data.Row.makeId tid, @idx-1
        row

      hasNext: ->
        return yes if @inner? and @inner.hasNext()
        while @iter.hasNext()
          @inner.close() if @inner?
          inner = @iter.next().get @tablecol
          @inner = inner.iterator()  if inner?
          break if @inner? and @inner.hasNext()
        @inner? and @inner.hasNext()

      close: ->
        @iter.close()
        @iter = null
    new Iter @table, @tablecol


  apply: (fname, args...) -> 
    schema = null
    rows = @table.map (row) =>
      t = row.get @tablecol
      t = t[fname].call t, args...
      row = row.shallowClone()
      row.set @tablecol, t
      schema ?= t.schema
      row

    newtable = new data.ops.Array @table.schema, rows
    new data.PartitionedTable newtable, @partcols, schema

  project: -> @apply 'project', arguments...
  filter: -> @apply 'filter', arguments...
  distinct: -> @apply 'distinct', arguments...
  cache: -> @apply 'cache', arguments...
  once: -> @apply 'once', arguments...
  cross: -> @apply 'cross', arguments...
  join: -> @apply 'join', arguments...
  flatten: -> @apply 'flatten', arguments...
  groupby: -> @apply 'groupby', arguments...
  aggregate: -> 
    @apply 'aggregate', arguments...

  orderby: (cols, reverse=no) ->
    if _.difference(cols, @partcols).length > 0
      new @constructor new data.ops.Union(@table.all(@tablecol)).orderby(cols, reverse)
    else
      new @constructor @table.sort(cols)

  union: (tables...) -> data.PartitionedTable.fromTables tables.concat([@])

  limit: (n) -> 
    newrows = []
    sofar = 0
    @table.each (row) =>
      return if sofar >= n
      t = row.get @tablecol
      if sofar+t.nrows() > n
        row = row.shallowClone()
        row.set @tablecol, t.limit(n-sofar)
        sofar = n
      else
        sofar += t.nrows()
      newrows.push row

    new data.PartitionedTable new data.ops.Array(@table.schema, newrows)

  offset: (n) -> 
    newrows = []
    sofar = 0
    @table.each (row) =>
      if sofar >= n
        newrows.push row
        return
      t = row.get @tablecol
      if sofar+t.nrows() >= n
        offset = (n - sofar)
        if t.nrows() - offset > 0
          row = row.shallowClone()
          row.set @tablecol, t.offset(offset)
          newrows.push row
      sofar += t.nrows()

    new data.PartitionedTable new data.ops.Array(@table.schema, newrows)

