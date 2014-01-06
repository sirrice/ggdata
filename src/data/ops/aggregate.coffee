#<< data/table


class data.ops.Aggregate extends data.Table
  # don't care about column value.  
  # fills it in with 1s
  @STAR = '*'


  # @param aggs of the form
  #    alias: col | [col, ...]
  #    f: (table) -> val
  #    type: schema type | [ type,...]    (default: schema.object)
  #    col: col | [col*]    columns accessed in aggregate function
  #    
  #   if alias is a list, then f is expected to return a dictionary 
  #
  # @param alias name of the column containing the partition, if null
  #        will try to sniff it out
  # XXX: support incremental aggs
  constructor: (@table, @aggs, @alias=null) ->
    super

    @alias ?= @constructor.sniffAlias @table.schema
    @schema = @table.schema
    unless @schema.has @alias
      throw Error("agg schema doesn't have table column #{@alias}: #{@schema.toString}")

    @schema = @schema.exclude @alias
    @parseAggs()
    @setProv()


  nrows: -> @table.nrows()
  children: -> [@table]

  colDependsOn: (col, type) ->
    _.compact _.flatten _.map @aggs, (agg) ->
      if (col == agg.alias) or (col in _.flatten([agg.alias]))
        agg.col

  toSQL: ->
    partcolstrs = @table.partcols.join ', '
    # XXX: assumes that aggs are all simple SQL aggs and
    #      aliased with the SQL agg function name
    aggstrs = @aggs.map (agg) ->
      "#{agg.alias}(#{agg.col}) as #{agg.alias}"

    """
    select #{partcolstrs}, #{aggstrs}
    from (#{@table.toSQL()}) as #{@id}
    group by #{partcolstrs}
    """

  iterator: ->
    tid = @id
    timer = @timer()
    class Iter
      constructor: (@schema, @table, @aggs, @tablealias) ->
        @_row = new data.Row @schema
        @iter = @table.iterator()
        @idx = -1

      reset: -> 
        @iter.reset()
        @idx = -1

      next: ->
        timer.start()
        @idx += 1
        row = @iter.next()
        @_row.reset()
        @_row.steal row

        cols = _.map @aggs, (agg) -> agg.col
        colVals = _.o2map cols, (col) -> [col, []]
        colVals[data.ops.Aggregate.STAR] = []
        partition = row.get @tablealias

        depRows = []
        partition.each (prow) ->
          for col in cols
            if col != data.ops.Aggregate.STAR
              colVals[col].push prow.get(col)
          colVals[data.ops.Aggregate.STAR].push 1
          depRows.push.apply depRows, prow.prov()

        for agg in @aggs
          if _.isArray agg.alias
            o = agg.f colVals[agg.col], @idx
            for col in agg.alias
              @_row.set col, o[col]
          else
            val = agg.f colVals[agg.col], @idx
            @_row.set agg.alias, val
        @_row.id = data.Row.makeId tid, @idx-1
        prov.Prov.get()
        @_row.addProv depRows
        timer.stop()

        @_row

      hasNext: ->
        res = @iter.hasNext()
        res
      close: -> 
        @iter.close()

    new Iter @schema, @table, @aggs, @alias


  parseAggs: ->
    @aggs = _.flatten [@aggs]
    for agg in @aggs
      data.ops.Aggregate.normalizeAgg agg, @schema

  # update the schema with aggregate's aliases
  #
  # @return aggregate with structure:
  # {
  #   
  # }
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
    unless _.isString(agg.col) 
      throw Error "Only support single column aggregates: got #{agg.col}"
    unless (agg.col == data.ops.Aggregate.STAR or schema.has(agg.col))
      console.log "[W] col #{agg.col} not in table #{schema.cols}.  Reverting to STAR"
      agg.col = data.ops.Aggregate.STAR
    agg

   @sniffAlias: (schema) ->
    for col in schema.cols
      if schema.type(col) == data.Schema.table
        return col
    throw Error "could not find column with table type! #{schema.toString()}"

  


  #
  # static methods for creating aggregate specifications
  # (@aggs) param in Aggregate.constructor
  #

  @agg: (aggtype, alias="agg", col='y', args...) ->
    f = switch aggtype 
      when 'min'
        (t) -> d3.min t
      when 'max'
        (t) -> d3.max t
        d3.max
      when 'count', 'cnt'
        (t) -> t.nrows()
        (vals) -> vals.length
      when 'sum', 'total'
        (t) -> d3.sum t
      when 'avg', 'mean', 'average'
        (t) -> d3.mean t
      when 'median'
        (t) -> d3.median t
      when 'quantile'
        k = args[0]
        (vals) ->
          vals.sort d3.ascending
          d3.quantile vals, k
    {
      alias: alias
      f: f
      type: data.Schema.numeric
      col: col
    }

  @count: (alias="count") -> @agg 'count', alias, data.ops.Aggregate.STAR
  @average: (alias='avg', col) -> @agg 'avg', alias, col
  @sum: (alias='sum', col) -> @agg 'sum', alias, col
