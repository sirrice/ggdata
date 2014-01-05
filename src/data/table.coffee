#<< data/util/*

ggprov = require 'ggprov'

#
# The data model consists of a list of tuples (rows)
#
# Each tuple (row) contains a list of columns
# The data types include
# 1) atomic datatypes -- numeric, string, datetime
# 2) function datatype
# 3) object data type -- tuple knows how to inspect into it
# 3) array data type of mappings -- not inspected
#
# Attribute resolution
# 1) check for attributes containing atomic data types
# 2) check each column that is of type object
#
#
class data.Table
  @ggpackage = "data.Table"
  @log = data.util.Log.logger @ggpackage, "Table"
  @timer = new data.util.Timer(100)
  @id: -> "t#{data.Table::_id += 1}"
  _id: 0

  constructor: ->
    @id = data.Table.id()
    @name ?= @constructor.name 
    unless @name?
      print @
      throw Error

  tabletype: -> "row"

  timer: -> @_timer ?= new data.util.Timer()

  timings: (name) -> @timer().timings name

  pstore: -> ggprov.Prov.get()



  setProv: ->
    pstore = @pstore()
    pstore.tag @, "table"
    for child in @children()
      pstore.connect child, @, "table"

    # schema col provenance
    for col in @cols()
      @pstore().connect @, col, 'col'
      deps = @colDependsOn col
      for dep in deps
        @pstore().connect col, dep, 'coldep'


  # 
  # Required/Overridable methods
  #

  # Internal function that returns a data.Row iterator
  # Users should call all/any/each/map
  iterator: -> throw Error("iterator not implemented")

  toSQL: -> throw Error("toSQL not implemented")

  # the tables accessed by this table
  children: -> []

  # return columns that {@param col} depends on
  colDependsOn: (col, type) ->
    cols = _.flatten [col]



  #
  # General stats/schema methods
  #

  # does schema contain col with type?
  has: (col, type) -> @contains col, type
  contains: (col, type) -> @schema.has col, type

  hasCols: (cols, types=null) ->
    _.all cols, (col, idx) =>
      type = null
      type = types[idx] if types? and types.length > idx
      @has col, type

  cols: -> @schema.cols

  ncols: -> @schema.ncols()

  # computes and caches number of rows in table
  # @override if more efficient implementations
  nrows: -> 
    unless @_nrows?
      i = 0
      iter = @iterator()
      while iter.hasNext()
        i += 1
        iter.next()
      iter.close()
      @_nrows = i
    @_nrows

  toJSON: ->
    schema: @schema.toJSON()
    data: _.toJSON @raw()
    tabletype: @tabletype()

  @fromJSON: (json) ->
    klass = @type2class(json.tabletype)
    klass ?= data.ColTable
    klass.fromJSON json

  toString: ->
    JSON.stringify @raw()




  #
  # Row and column accessors
  #

  # Extracts any row, column value, or values from table
  # NOTE: no guarantee on order.
  #
  # @param cols can be null, a column name, or an array of column names
  # @return 
  #   if cols is null: 
  #     a rows
  #   if cols is a column name:
  #     any column value
  #   if cols is an array:
  #     array of column values, one for each column name in {@param cols}
  #
  any: (cols=null) ->
    iter = @iterator()
    row = null
    row = iter.next() if iter.hasNext()
    row.id = "#{@id}:#{0}"
    iter.close()
    return null unless row?

    if cols?
      if _.isArray cols
        _.map cols, (col) -> row.get col
      else
        row.get cols
    else
      row

  # Extracts all rows, a column, or a list of columns from the table
  #
  # @param cols can be null, a column name, or an array of column names
  # @return 
  #   if cols is null: 
  #     all rows
  #   if cols is a column name:
  #     an array of the column values
  #   if cols is an array:
  #     an array of column values.  Each element is the list of values 
  #     for the corresponding column name in {@param cols}
  #
  all: (cols=null) ->
    if cols?
      if _.isArray cols
        ret = _.map cols, () -> []
        @each (row) -> 
          for col, idx in cols
            ret[idx].push row.get(col)
        ret
      else
        ret = []
        col = cols
        @each (row) ->
          ret.push row.get(col)
    else
      tid = @id
      ret = @map (row, rowidx) -> 
        row = row.clone()
        row.id = "#{tid}:#{rowidx}"
        row
    ret

  raw: -> @map (row) -> row.raw()

  # @param f functiton to run.  takes data.Row, index as input
  # @param n number of rows
  # XXX: clone rows?
  map: (f, n=null) ->
    data.Table.timer.start("#{@name}-#{@id}-map")
    data.Table.timer.start("#{@name}-map")
    iter = @iterator()
    idx = 0
    ret = []
    while iter.hasNext()
      ret.push f(iter.next(), idx)
      idx +=1 
      break if n? and idx >= n
    iter.close()
    data.Table.timer.stop("#{@name}-#{@id}-map")
    data.Table.timer.stop("#{@name}-map")
    ret

  # each, doesn't return anything!
  each: (f, n) -> 
    data.Table.timer.start("#{@name}-#{@id}-each")
    data.Table.timer.start("#{@name}-each")
    iter = @iterator()
    idx = 0
    while iter.hasNext()
      f iter.next(), idx
      idx += 1
      break if n? and idx >= n
    iter.close()
    data.Table.timer.stop("#{@name}-#{@id}-each")
    data.Table.timer.stop("#{@name}-each")



  #
  # Convenience methods that wrap operators
  #

  limit: (n) ->
    new data.ops.Limit @, n

  offset: (n) ->
    new data.ops.Offset @, n

  sort: (cols, reverse=no) ->
    @orderby cols, reverse

  orderby: (cols, reverse=no) ->
    new data.ops.OrderBy @, cols, reverse

  filter: (f) ->
    new data.ops.Filter @, f

  distinct: (cols) ->
    new data.ops.Distinct @, cols

  cache: ->
    if _.isType(@, data.ops.Array) 
      @
    else
      new data.ops.Cache @

  once: ->
    if (
      _.isType(@, data.ops.Array) or 
      _.isType(@, data.RowTable) or
      _.isType(@, data.ColTable))
      @
    else
      new data.ops.Once @

  union: (tables...) ->
    tables = _.compact _.flatten tables
    new data.ops.Union @, tables

  cross: (table, jointype='outer', leftf, rightf) ->
    new data.ops.Cross @, table, jointype, leftf, rightf

  join: (table, cols, type="outer", leftf, rightf) ->
    (new data.ops.HashJoin @, table, cols, type, leftf, rightf)

  exclude: (cols) ->
    cols = _.flatten [cols]
    keep = _.reject @cols(), (col) -> col in cols
    mappings = _.map keep, (col) =>
      alias: col
      type: @schema.type col
      cols: col
      f: _.identity
    @project mappings, no


  # @param col col name
  # @param data array of values.  If setting constant, use setColVal
  setCol: (col, colData, type=null) ->
    type ?= data.Schema.type colData[0] if colData.length > 0
    type ?= data.Schema.object

    f = (idx) -> colData[idx] if idx < colData.length 
        
    mapping = [
      {
        alias: col
        f: f
        type: type
        cols: []
      }
    ]
    @project mapping, yes

  # add or set column to a single constant value
  setColVal: (col, val, type=null) ->
    type ?= data.Schema.type val 
    f = -> val
    mapping = [
      {
        alias: col
        f: f
        type: type
        cols: []
      }
    ]
    @project mapping, yes

  # @param extend keep existing columns (if not overwritten by mappings)?
  project: (mappings, extend=yes) ->
    (new data.ops.Project @, mappings, extend)

  # @param extend keep existing columns (if not overwritten by mappings)?
  blockproject: (mappings, extend=yes, blocksize) ->
    (new data.ops.BlockProject @, mappings, extend, blocksize)

  # @param alias name of the table column that will store the partitions
  partition: (cols, alias="table") ->
    (new data.ops.Partition @, cols, alias)

  flatten: ->
    new data.ops.Flatten @

  # @param alias name of the table column (containing the partitions)
  aggregate: (aggs, alias=null) ->
    new data.ops.Aggregate @, aggs, alias

  groupby: (cols, aggs) ->
    new data.ops.Aggregate(
      @partition(cols),
      aggs)

  partitionJoin: (table, cols, type="outer") ->
    partition1 = @partition cols
    partition2 = table.partition cols
    partition1.join partition2, cols, type





  #
  # Static Methods
  #

  @type2class: (tabletype="row") ->
    switch tabletype
      when "row", "RowTable"
        data.RowTable
      when "col", "ColTable"
        data.ColTable
      else
        null

  @deserialize: (str) ->
    json = JSON.parse str
    switch json.type
      when 'col'
        data.ColTable.deserialize json
      when 'row'
        data.RowTable.deserialize json
      else
        throw Error "can't deserialize data of type: #{json.type}"


  #
  # Convert an array of objects into a table
  # Routes to Concrete table classes (coltable, rowtable) 
  #
  # @param rows [ { attr: val, .. } ]
  @fromArray: (rows, schema=null, tabletype="row") ->
    klass = @type2class tabletype
    unless klass?
      throw Error "#{tabletype} doesnt have a class"

    klass.fromArray rows, schema
