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
  constructor: (@table, @mappings, @extend=yes) ->
    @mappings = _.compact _.flatten [@mappings]
    @mappings = data.ops.Project.normalizeMappings @mappings, @table.schema
    @mappings = data.ops.Project.extendMappings @mappings, @table.schema if @extend
    cols = _.flatten _.map(@mappings, (desc) -> desc.alias)
    types = _.flatten _.map(@mappings, (desc) -> desc.type)
    @schema = new data.Schema cols, types
    @inferUnknownCols()

    if @table.isFrozen()
      #console.log @table
      throw Error "cannot project (modify) frozen table"
    super

  nrows: -> @table.nrows()
  children: -> [@table]

  colDependsOn: (col, type) ->
    cols = _.map @mappings, (desc) ->
      if (col == desc.alias) or (col in _.flatten([desc.alias]))
        desc.cols
    _.compact _.flatten cols


  inferUnknownCols: ->
    return if @_infered?
    @_infered = yes

    mappings = _.filter @mappings, (desc) -> desc.type == data.Schema.unknown
    return unless mappings.length > 0
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


  iterator: ->
    @inferUnknownCols()

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
          if desc.isArray
            o = desc.f row, @idx
            for col in desc.alias
              @_row.set col, o[col]
          else
            val = desc.f row, @idx
            @_row.set desc.alias, val
        @_row

      hasNext: -> 
        @iter.hasNext()
      close: -> 
        @iter.close()
        timer.stop()
    new Iter @schema, @table, @mappings




  # @params mappings for this projection
  # @params schema schema of base table
  @extendMappings: (mappings, schema) ->
    newcols = {}
    for newcol in _.flatten(_.map mappings, (desc) -> [desc.alias])
      newcols[newcol] = yes
    oldmappings = for col in schema.cols
      continue if col of newcols
      data.ops.Project.normalizeMapping col, schema
    mappings.concat _.compact oldmappings



  @normalizeMappings: (mappings, schema) ->
    _.map mappings, (desc) ->
      data.ops.Project.normalizeMapping desc, schema

  # ensure that the projection description has all attributes:
  #   cols
  #   type
  #   f
  # 
  # assumes alias exists
  # Constructs optimized functions to execute projects in blocks of rows
  # Optimizes for raw column projections (copy value over) and single argument functions
  @normalizeMapping: (desc, schema) ->
    if _.isString desc
      unless schema.has desc
        throw Error "project: #{desc} not in table w schema #{schema.cols}"
      desc = 
        alias: desc
        f: _.identity
        type: schema.type desc
        cols: desc
        isArray: no
        isRawCol: yes

    unless desc.alias?
      throw Error("mapping must have alias: #{desc}") 

    desc = _.clone desc
    desc.cols ?= desc.col
    desc.cols ?= '*'
    desc.cols = _.flatten [desc.cols] unless desc.cols == '*'
    desc.isRawCol ?= no
    desc.type ?= data.Schema.unknown

    if _.isArray desc.alias
      if _.isArray desc.type
        unless desc.type.lenghth == desc.alias.length
          throw Error "alias and type lens don't match: #{desc.alias} != #{desc.type}"
      else
        desc.type = _.times desc.alias.length, () -> desc.type


    if desc.cols != '*' and _.isArray desc.cols
      if desc.isRawCol
        colidx = schema.index desc.cols[0]
        desc.f = ((colidx) ->
          (row) -> 
            v = row.data[colidx]
            v = null if v == undefined
            v
        )(colidx)


      else if desc.cols.length == 1 
        colidx = schema.index desc.cols[0]
        desc.f = ((f, colidx) ->
          (row, idx) -> 
            v = row.data[colidx]
            v = null if v == undefined
            f v, idx
        )(desc.f, colidx)


      else
        colidxs = _.map desc.cols, (col) -> schema.index col
        desc.f = ((f, cols, colidx) ->
          (row, idx) ->
            args = for colidx in colidxs
              v = row.data[colidx]
              v = null if v == undefined
              v
            args.push idx
            f.apply f, args
          )(desc.f, desc.cols, colidxs)

    else
      _row = new data.Row schema
      desc.cols = _.clone(schema.cols)

    desc.isArray = _.isArray desc.alias
    desc






