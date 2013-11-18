#<< data/table

class data.ops.Aggregate extends data.Table
  # @param aggs of the form
  #    alias: col | [col, ...]
  #    f: (table) -> val
  #    type: schema type    (default: schema.object)
  #    col: col | [col*]    columnsaccessed in aggregate function
  #    
  #   if alias is a list, then f is expected to return a dictionary 
  #
  # @param alias name of the column containing the partition, if null
  #        will try to sniff it out
  # XXX: support incremental aggs
  constructor: (@table, @aggs, @alias=null) ->
    @alias ?= @sniffAlias()
    @schema = @table.schema
    unless @schema.has @alias
      throw Error("agg schema doesn't have table column #{@alias}: #{@schema.toString}")

    @schema = @schema.exclude @alias
    @parseAggs()

  sniffAlias: ->
    schema = @table.schema
    for col in schema.cols
      if schema.type(col) == data.Schema.table
        return col
    throw Error "could not find column with table type! #{schema.toString()}"


  nrows: -> @table.nrows()

  iterator: ->
    class Iter
      constructor: (@schema, @table, @aggs, @tablealias) ->
        @iter = @table.iterator()
        @idx = -1

      reset: -> 
        @iter.reset()
        @idx = -1

      next: ->
        @idx += 1
        row = @iter.next()
        newrow = row.project @schema

        for agg in @aggs
          if _.isArray agg.alias
            o = agg.f row.get(@tablealias), @idx
            for col in agg.alias
              newrow.set col, o[col]
          else
            val = agg.f row.get(@tablealias), @idx
            newrow.set agg.alias, val
        newrow

      hasNext: -> @iter.hasNext()
      close: -> @iter.close()

    new Iter @schema, @table, @aggs, @alias


  parseAggs: ->
    @aggs = _.flatten [@aggs]
    for agg in @aggs
      data.ops.Aggregate.normalizeAgg agg, @schema

  @normalizeAgg: (agg, schema) ->
    agg.type ?= data.Schema.object
    if _.isArray agg.alias
      if _.isArray agg.type
        unless agg.alias.length  == agg.type.length
          throw Error "alias.len != type.len: #{desc.alias} != #{desc.type}"
      else
        agg.type = _.times agg.alias.length, () -> agg.type
      for col, idx in agg.alias
        schema.addColumn col, agg.type[idx]
    else
      schema.addColumn agg.alias, agg.type
    agg

   


  #
  # static methods for creating aggregate specifications
  # (@aggs) param in Aggregate.constructor
  #

  @count: (alias="count") ->
    f = (t) -> 
      if t?
        t.nrows()
      else 
        0
    {
      alias: alias
      f: f
      type: data.Schema.numeric
      col: []
    }

  @sums: (cols, aliases) ->
    unless _.isArray alias 
      alias = []
    while alias.length < cols.length
      alias.push "sum#{alias.length}"
    _.map cols, (col, idx) ->
      data.ops.Aggregate.sum col, alias[idx]

  # @param col column name of list of column names
  @sum: (col, alias='sum') ->

    f = (t) ->
      sum = 0
      for v in t.all(col)
        sum += v if _.isValid v
      sum
    {
      alias: alias
      f: f
      type: data.Schema.numeric
      col: col
    }
