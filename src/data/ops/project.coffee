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
    super
    @mappings = _.compact _.flatten [@mappings]
    @mappings = @constructor.normalizeMappings @mappings, @table.schema
    @mappings = @constructor.extendMappings @mappings, @table.schema if @extend
    cols = _.flatten _.map(@mappings, (desc) -> desc.alias)
    types = _.flatten _.map(@mappings, (desc) -> desc.type)
    @schema = new data.Schema cols, types
    @inferUnknownCols()
    @setProv()

  nrows: -> @table.nrows()
  children: -> [@table]
  colDependsOn: (col, type) ->
    cols = _.map @mappings, (desc) ->
      if (col == desc.alias) or (col in _.flatten([desc.alias]))
        desc.cols
    _.compact _.flatten cols

  toSQL: ->

    """
    select 
    FROM (#{@table.toSQL()})
    """



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
    tid = @id
    
    class Iter
      constructor: (@schema, @table, @mappings) ->
        @_row = new data.Row @schema
        @iter = @table.iterator()
        @idx = -1

      reset: -> 
        @iter.reset()
        @idx = -1

      next: ->
        @idx += 1
        row = @iter.next()
        timer.start()
        @_row.reset()
        @_row.id = data.Row.makeId tid, @idx-1
        @_row.addProv row.prov()
        for desc in @mappings
          if desc.isArray
            o = desc.f row, @idx
            for col in desc.alias
              @_row.set col, o[col]
          else
            val = desc.f row, @idx
            @_row.set desc.alias, val
        timer.stop()
        @_row

      hasNext: -> 
        @iter.hasNext()
      close: -> 
        @iter.close()
    new Iter @schema, @table, @mappings




  # @params mappings for this projection
  # @params schema schema of base table
  @extendMappings: (mappings, schema) ->
    newcols = {}
    for newcol in _.flatten(_.map mappings, (desc) -> [desc.alias])
      newcols[newcol] = yes
    oldmappings = for col in schema.cols
      continue if col of newcols
      @normalizeMapping col, schema
    mappings.concat _.compact oldmappings



  @normalizeMappings: (mappings, schema) ->
    for desc in mappings
      @normalizeMapping desc, schema

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
        desc.f = ((col) ->
          colidx = null
          (row) -> 
            colidx ?= row.schema.index col
            v = row.data[colidx]
            v = null if v == undefined
            v
        )(desc.cols[0])


      else if desc.cols.length == 1 
        desc.f = ((f, col) ->
          colidx = null
          (row, idx) -> 
            colidx ?= row.schema.index col
            v = row.data[colidx]
            v = null if v == undefined
            f v, idx
        )(desc.f, desc.cols[0])



      else
        desc.f = ((f, cols) ->
          colidxs = null
          (row, idx) ->
            colidxs ?= _.map cols, (col) -> row.schema.index col
            args = for colidx in colidxs
              v = row.data[colidx]
              v = null if v == undefined
              v
            args.push idx
            f.apply f, args
          )(desc.f, desc.cols)

    else
      _row = new data.Row schema
      desc.cols = _.clone(schema.cols)

    desc.isArray = _.isArray desc.alias
    desc






