#<< data/table


class data.ops.BlockProject extends data.ops.Project
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
  constructor: (@table, @mappings, @extend=yes, @blocksize=15) ->
    super

  iterator: ->
    @inferUnknownCols()

    timer = @timer()
    class BlockIter
      constructor: (@schema, @table, @mappings, @blocksize) ->
        @_blocksize = 0
        @_blockidx = -1
        @_rows = _.times @blocksize, () => new data.Row @schema
        @_rowdatas = _.times @blocksize, () => null
        @idx = -1
        @iter = @table.iterator()

      reset: ->
        @_blocksize = 0
        @_blockidx = -1
        @idx = -1
        @iter.reset()

      next: ->
        throw Error("no more elements") unless @hasNext()
        @idx += 1
        row = @_rows[@_blockidx]
        @_blockidx += 1
        row


      hasNext: ->
        return yes if @_blockidx != -1 and @_blockidx < @_blocksize
        return no unless @iter.hasNext()

        @_blocksize = 0
        @_blockidx = 0
        while @_blocksize < @_rows.length and @iter.hasNext()
          @_rowdatas[@_blocksize] = @iter.next()
          @_blocksize += 1

        timer.start()
        for idx in [0...@_blocksize]
          @_rowdatas[idx] = @_rowdatas[idx].shallowClone()
          @_rows[idx].reset()

        for desc in @mappings
          if desc.isArray
            os = desc.blockf @_rowdatas, @_blocksize, @idx
            for o, idx in os
              _row = @_rows[idx]
              for col in desc.alias
                _row.set col, o[col]
          else
            vals = desc.blockf @_rowdatas, @_blocksize, @idx
            for v, idx in vals
              @_rows[idx].set desc.alias, v

        timer.stop()

        @_blockidx < @_blocksize

      close: ->
        @_rows = @_rowdatas = null
        @iter.close()
    return new BlockIter @schema, @table, @mappings, @blocksize




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
        desc.blockf = ((f, colidx) ->
          (rows, len, rowidx) ->
            for idx in [0...len]
              v = rows[idx].data[colidx]
              v = null if v == undefined
              v
        )(desc.f, colidx)

      else if desc.cols.length == 1 
        colidx = schema.index desc.cols[0]
        desc.blockf = ((f, colidx) ->
          (rows, len, rowidx) ->
            for idx in [0...len]
              v = rows[idx].data[colidx]
              v = null if v == undefined
              f v, rowidx+idx
        )(desc.f, colidx)

      else
        colidxs = _.map desc.cols, (col) -> schema.index col
        desc.blockf = ((f, cols, colidxs) ->
          (rows, len, rowidx) ->
            for idx in [0...len]
              args = for col, colidx in cols
                v = rows[idx].data[colidxs[colidx]]
                v = null if v == undefined
                v
              args.push rowidx+idx
              f.apply f, args
        )(desc.f, desc.cols, colidxs)
    else
      desc.cols = _.clone(schema.cols)
      desc.blockf = ((f) ->
        (rows, len, rowidx) ->
          for idx in [0...len]
            f rows[idx], rowidx+idx
      )(desc.f)

    desc.isArray = _.isArray desc.alias
    desc






