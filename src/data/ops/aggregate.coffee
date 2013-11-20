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
    super
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
  children: -> [@table]

  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@schema, @table, @aggs, @tablealias) ->
        @_row = new data.Row @schema
        @iter = @table.iterator()
        @idx = -1
        timer.start()

      reset: -> 
        @iter.reset()
        @idx = -1

      next: ->
        @idx += 1
        row = @iter.next()
        @_row.steal row

        for agg in @aggs
          if _.isArray agg.alias
            o = agg.f row.get(@tablealias), @idx
            for col in agg.alias
              @_row.set col, o[col]
          else
            val = agg.f row.get(@tablealias), @idx
            @_row.set agg.alias, val
        @_row

      hasNext: -> @iter.hasNext()
      close: -> 
        @iter.close()
        timer.stop()

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

  @agg: (aggtype, alias="agg", col, args...) ->
    f = switch aggtype 
      when 'min'
        (t) -> d3.min t.all(col)
      when 'max'
        (t) -> d3.max t.all(col)
      when 'count', 'cnt'
        (t) -> t.nrows()
      when 'sum', 'total'
        (t) -> d3.sum t.all(col)
      when 'avg', 'mean', 'average'
        (t) -> d3.mean t.all(col)
      when 'median'
        (t) -> d3.median t.all(col)
      when 'quantile'
        k = args[0]
        (t) ->
          vals = t.all col
          vals.sort d3.ascending
          d3.quantile vals, k
    {
      alias: alias
      f: f
      type: data.Schema.numeric
      col: col
    }

  @count: (alias="count") -> @agg 'count', alias
  @average: (alias='avg', col) -> @agg 'avg', alias, col
  @sum: (alias='sum', col) -> @agg 'sum', alias, col
