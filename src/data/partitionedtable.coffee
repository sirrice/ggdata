#<< data/table

class data.PartitionedTable extends data.Table

  # @param table [partition cols..., table]
  # @param bcache should we cache @table?
  constructor: (@table, @partcols=[], @schema, @bcache=yes) ->
    super

    @tablecols = @table.cols().filter (col) => @table.schema.type(col) == data.Schema.table

    if @tablecols.length == 0
      @schema = @table.schema
      schema = new data.Schema ['table'], [data.Schema.table]
      row = new data.Row schema, [@table]
      table = new data.ops.Array schema, [row]
      @table = table
      @tablecols = ['table']

    if @tablecols.length > 1
      throw Error

    for col in @partcols
      unless @table.has col
        throw Error

    @tablecol = @tablecols[0]
    @normcols = _.without @table.cols(), @tablecol

    @schema ?= @table.any(@tablecol)
    unless @schema?
      throw Error

    unless @bcache and _.isType @table, data.ops.Array
      data.Table.timer.start 'ptcache'
      @table = @table.cache()
      data.Table.timer.stop 'ptcache'
      console.log ['ptcache', data.Table.timer.avg('ptcache'), data.Table.timer.count('ptcache')]

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
        [row.project(cols), new data.PartitionedTable(t, @partcols, @schema, no)]
    else
      @table.map (row) =>
        t = new data.ops.Array(@table.schema, [row])
        [row.project(cols), new data.PartitionedTable(t, @partcols, @schema)]

  partitionOn: (cols, complete=yes) ->
    cols = _.compact _.flatten [cols]
    diffcols = _.difference(cols, @partcols)
    if diffcols.length > 0
      for col in diffcols
        unless (col in @schema.cols) or (col in @partcols)
          throw Error "#{col} is not in schema #{@schema.cols}"

      cols = _.union cols, @partcols
      tagcols = _.difference cols, @schema.cols
      newschema = null
      newrows = []
      @table.each (row) =>
        p = row.get(@tablecol).partition(cols, @tablecol, complete)
        for tagcol in tagcols
          p = p.setColVal tagcol, row.get(tagcol), row.schema.type(tagcol)
        newschema ?= p.schema
        newrows.push.apply newrows, p.all()

      newtable = new data.ops.Array newschema, newrows
      new data.PartitionedTable newtable, cols, @schema
    else
      @

  tags: ->
    _.reject @partcols, (c) => @has c

  addTag: (col, val, type=null) ->
    type ?= data.Schema.type val
    newtable = @table.setColVal col, val, type
    cols = _.union [col], @partcols
    new data.PartitionedTable newtable, cols, @schema, no

  rmTag: (col) ->
    newtable = @table.exclude col
    new data.PartitionedTable newtable, _.without(@partcols, col), @schema, no

  @fromTables: (tables) ->
    tables = _.flatten [tables]
    cols = {}
    tables = for t in tables
      if _.isType t, data.PartitionedTable
        for col in t.partcols
          cols[col] = yes
        t
      else
        new data.PartitionedTable t, [], t.schema, no

    return tables[0] if tables.length == 1

    cols = _.keys cols
    schema = null
    tables = for t in tables
      t = t.partitionOn cols
      schema ?= t.schema
      t.table

    union = new data.ops.Union tables
    part = union.partition cols
    newtable = part.project {
      alias: 'table'
      cols: ['table']
      type: data.Schema.table
      f: (tablerows) ->
        tables = tablerows.map (row) ->
          row.get 'table'
        new data.ops.Union tables
    }

    new data.PartitionedTable(
      newtable
      cols
      schema
      yes
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
  cache: -> @apply 'cache', arguments...
  once: -> @apply 'once', arguments...
  join: -> @apply 'join', arguments...
  flatten: -> @apply 'flatten', arguments...
  groupby: -> @apply 'groupby', arguments...
  aggregate: -> @apply 'aggregate', arguments...

  cross: (table, type, leftf, rightf) -> 
    newschema = @table.schema.clone()
    table = data.PartitionedTable.fromTables table
    newschema = newschema.merge table.table.schema.project(table.partcols)
    allcols = _.union @partcols, table.partcols, ['table']
    subschema = null
    newrows = []
    @table.each (lrow) =>
      table.table.each (rrow) =>
        newrow = new data.Row newschema
        newrow.steal lrow
        newrow.steal rrow
        cross = lrow.get(@tablecol).cross(rrow.get(table.tablecol))
        newrow.set 'table', cross
        subschema ?= cross.schema
        newrows.push newrow

    partcols = _.union @partcols, table.partcols
    newtable = new data.ops.Array newschema, newrows
    return new data.PartitionedTable newtable, partcols, subschema
    @apply 'cross', arguments...

  distinct: (cols) -> 
    cols ?= @schema.cols
    cols = _.flatten [cols]

    if _.difference(cols, @partcols).length == 0
      rows = @table.distinct(cols).map (row) =>
        t = row.get(@tablecol).limit(1)
        row = row.shallowClone()
        row.set @tablecol, t
        row
      t = new data.ops.Array(@table.schema, rows)
      new data.PartitionedTable t, @partcols, @schema
    else
      @apply 'distinct', arguments...

  orderby: (cols, reverse=no) ->
    if _.difference(cols, @partcols).length > 0
      new data.PartitionedTable new data.ops.Union(@table.all(@tablecol)).orderby(cols, reverse)
    else
      new data.PartitionedTable(@table.sort(cols, reverse)).apply('orderby', cols, reverse)

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


