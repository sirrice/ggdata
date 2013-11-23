#<< data/table


class data.ops.Project extends data.Table
  # @param mappings list of
  #    alias: colname | [col, col..]
  #
  #         if alias is a list, then f is expected to return a dictionary 
  #         with alias keys, or null if absent
  #
  #    f: (row) ->  |  (col, ...) ->
  #
  #    type: schema type     (default: schema.object)
  #
  #    cols: '*' | [list of col args to f ] 
  #
  #         '*'  : f accepts row as argument (default)
  #         [..] : f accepts a list of column values as args
  #
  #  for example:
  #
  #  1) add 10 to x:
  #
  #     { alias: 'x', f: (x) -> x+10, cols: 'x' }
  #
  constructor: (@table, @mappings) ->
    super
    @mappings = _.compact _.flatten [@mappings]
    @mappings = data.ops.Project.normalizeMappings @mappings, @table.cols()
    cols = _.flatten _.map(@mappings, (desc) -> desc.alias)
    types = _.flatten _.map(@mappings, (desc) -> desc.type)
    @schema = new data.Schema cols, types
    @inferUnknownCols()

  colDependsOn: (col, type) ->
    cols = _.map @mappings, (desc) ->
      if (col == desc.alias) or (col in desc.alias)
        desc.cols
    _.compact _.flatten cols

  inferUnknownCols: ->
    mappings = _.filter @mappings, (desc) -> desc.type == data.Schema.unknown
    cols = _.flatten _.map mappings, (desc) -> desc.alias
    colVals = _.o2map cols, (col) -> [col, []]

    # get a sample of 5 rows
    sample = @table.limit(2)

    rows = []
    sample.each (row, idx) ->
      o = {}
      _.each mappings, (desc) ->
        v = desc.f row, idx
        if _.isArray desc.alias
          for col in desc.alias
            o[col] = v[col]
            colVals[col].push v[col]
        else
          col = desc.alias
          o[col] = v
          colVals[col].push v
      rows.push o

    schema = data.Schema.infer rows
    for col in cols
      types = _.map colVals[col], (v) -> data.Schema.type v
      type = d3.max types
      @schema.setType col, type unless type == data.Schema.unknown



  @normalizeMappings: (mappings, allcols) ->
    _.map mappings, (desc) ->
      data.ops.Project.normalizeMapping desc, allcols

  # ensure that the projection description has all attributes:
  #   cols
  #   type
  #   f
  # 
  # assumes alias exists
  @normalizeMapping: (desc, allcols) ->
    throw Error("mapping must has an alias: #{desc}") unless desc.alias?
    desc = _.clone desc
    desc.cols ?= desc.col
    desc.cols ?= '*'
    desc.cols = _.flatten [desc.cols] unless desc.cols == '*'
    desc.type ?= data.Schema.unknown

    if _.isArray desc.alias
      if _.isArray desc.type
        unless desc.type.lenghth == desc.alias.length
          throw Error "alias and type lens don't match: #{desc.alias} != #{desc.type}"
      else
        desc.type = _.times desc.alias.length, () -> desc.type

    if desc.cols != '*' and _.isArray desc.cols
      desc.f = ((f, cols) ->
        (row, idx) ->
          args = _.map cols, (col) -> row.get(col)
          args.push idx
          f.apply f, args
        )(desc.f, desc.cols)
    else
      desc.cols = _.clone(allcols)
    desc




  nrows: -> @table.nrows()
  children: -> [@table]

  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@schema, @table, @mappings) ->
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
        @_row.reset()
        for desc in @mappings
          if _.isArray desc.alias
            o = desc.f row, @idx
            for col in desc.alias
              @_row.set col, o[col]
          else
            val = desc.f row, @idx
            @_row.set desc.alias, val
        @_row

      hasNext: -> @iter.hasNext()
      close: -> 
        @iter.close()
        timer.stop()
    new Iter @schema, @table, @mappings



