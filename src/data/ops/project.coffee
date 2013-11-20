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

  @normalizeMappings: (mappings, allcols) ->
    _.map mappings, (desc) ->
      data.ops.Project.normalizeMapping desc, allcols

  @normalizeMapping: (desc, allcols) ->
    throw Error("mapping must has an alias: #{desc}") unless desc.alias?
    desc = _.clone desc
    desc.cols ?= desc.col
    desc.cols ?= '*'
    desc.cols = _.flatten [desc.cols] unless desc.cols == '*'
    desc.type ?= data.Schema.object

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



