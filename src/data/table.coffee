#<< data/util/log

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
# Methods that start with "_" are update in place and return the same table
#


class data.Table
  @ggpackage = "data.Table"
  @log = data.util.Log.logger @ggpackage, "Table"


  # 
  # Required methods
  #

  iterator: -> throw Error("iterator not implemented")

  # is this columnar or row
  tabletype: -> "col"


  # 
  # schema related methods
  #

  has: (col, type) -> @contains col, type

  contains: (col, type) -> @schema.has col, type

  hasCols: (cols, types=null) ->
    _.all cols, (col, idx) =>
      type = null
      type = types[idx] if types? and types.length > idx
      @has col, type

  cols: -> @schema.cols

  ncols: -> @schema.ncols()

  # @override
  nrows: -> 
    unless @_nrows?
      i = 0
      @each (row) -> i += 1
      @_nrows = i
    @_nrows


  # actually iterate through the iterator and create the rows
  getCol: (col) -> @each (row) -> row.get col

  raw: -> @each (row) -> row.raw()


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


  # Tries to infer a schema for a list of objects
  #
  # @param rows [ { attr: val, .. } ]
  @fromArray: (rows, schema=null, tabletype="row") ->
    klass = @type2class tabletype
    unless klass?
      throw Error "#{tabletype} doesnt have a class"

    klass.fromArray rows, schema

  @reEvalJS = /^{.*}$/
  @reVariable = /^[a-zA-Z]\w*$/
  @reNestedAttr = /^[a-zA-Z]+\.[a-zA-Z]+$/

  @isEvalJS: (s) ->@reEvalJS.test s
  @isVariable: (s) -> @reVariable.test s
  @isNestedAttr: (s) -> @reNestedAttr.test s



  #
  # Convenience methods that wrap operators
  #

  anyRow: ->
    iter = @iterator()
    ret = null
    ret = iter.next() if iter.hasNext()
    iter.close()
    ret

  any: (col=null) ->
    row = @anyRow()
    if col?
      row.get col
    else
      row

  all: (col=null) ->
    if col?
      @each (row) -> row.get col
    else
      @each _.identity

  # @param f functiton to run.  takes data.Row, index as input
  # @param n number of rows
  each: (f, n=null) ->
    iter = @iterator()
    idx = 0
    ret = []
    while iter.hasNext()
      ret.push f(iter.next(), idx)
      idx +=1 
      break if n? and idx >= n
    iter.close()
    ret

  fastEach: (f, n) -> @each f, n

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
    new data.ops.Cache @

  union: (tables...) ->
    tables = _.compact _.flatten tables
    new data.ops.Union @, tables

  cross: (table) ->
    new data.ops.Cross @, table, 'outer'

  join: (table, cols, type="outer") ->
    new data.ops.HashJoin @, table, cols, type

  exclude: (cols) ->
    cols = _.flatten [cols]
    keep = _.reject @cols(), (col) -> col in cols
    mappings = _.map keep, (col) =>
      alias: col
      type: @schema.type col
      cols: col
      f: _.identity
    @project mappings, no

  # Transforms individual columns 
  #
  # @param mappings list of 
  #  { 
  #    alias: 'x', 
  #    f: (x) -> , 
  #    type: table.schema.type alias
  #  }
  mapCols: (mappings) ->
    mappings = _.flatten [mappings]
    mappings = _.map mappings, (desc) =>
      unless _.isString desc.alias
        throw Error "alias #{desc.alias} not found"
      unless @has desc.alias
        throw Error "mapCol got unknown col #{desc.alias}.  scheam: #{@schema.toString()}"
      desc.type ?= @schema.type desc.alias
      desc.cols = desc.alias
      desc
    @project mappings, yes

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
    mappings = _.flatten [mappings]

    if extend
      newcols = {}
      _.each mappings, (desc) ->
        alias = desc.alias
        alias = desc if _.isString desc
        _.each _.flatten([alias]), (newcol) ->
          newcols[newcol] = yes

      oldcols = _.reject @cols(), (col) -> col of newcols
      mappings = mappings.concat oldcols

    # allow String mappings as shorthand for "copy exsiting column"
    mappings = _.map mappings, (desc) =>
      if _.isString desc
        unless @has desc
          throw Error("project: #{desc} not in table. schema: #{@schema.cols}")
        {
          alias: desc
          f: _.identity
          type: @schema.type desc
          cols: desc
        }
      else
        desc

    new data.ops.Project @, mappings

  # @param alias name of the table column that will store the partitions
  partition: (cols, alias="table") ->
    new data.ops.Partition @, cols, alias

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
