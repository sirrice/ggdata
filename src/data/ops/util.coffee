

class data.ops.Util 

  # faster implementation for array inputs rather than data.Table objects
  @crossArrayIter: (schema, lefts, rights, jointype='outer', leftf, rightf) ->
    defaultf = -> new data.Row(new data.Schema)
    leftf ?= defaultf
    rightf ?= defaultf
    rhasRows = rights.length > 0
    lhasRows = lefts.length > 0

    switch jointype
      when "left"
        unless rhasRows
          rights = [null]
          rights = [rightf()]
          
      when "right"
        unless lhasRows
          lefts = rights
          rights = [null]
          rights = [leftf()]

      when "outer"
        unless lhasRows
          lefts = rights
          rights = [leftf()]
        else unless rhasRows
          rights = [rightf()]


    class Iter
      constructor: (@schema, @lefts, @rights) ->
        @_row = new data.Row @schema
        @nrows = lefts.length * rights.length
        @idx = 0

      reset: -> @idx = 0

      next: ->
        throw Error "no more elements in iter" unless @hasNext()
        lidx = Math.floor(@idx / rights.length)
        ridx = @idx % rights.length
        @idx += 1
        l = lefts[lidx]
        r = rights[ridx]
        @_row.reset()
        @_row.steal(l) if l?
        @_row.steal(r) if r?
        @_row

      hasNext: -> @idx < @nrows
      close: ->
        @lefts = @rights = null
    new Iter schema, lefts, rights

  # Creates a table that's cross product of the attrs
  # @param cols { attr1: [ values], ... }
  @cross: (cols, tabletype=null) ->
    rows = data.ops.Util._cross cols
    return data.Table.fromArray rows, null, tabletype

  # @param cols object of { colname: list of values }
  @_cross: (cols, tabletype=null) ->
    if _.size(cols) == 0
      return [{}]
    rows = []
    col = _.first _.keys cols
    data = cols[col]
    cols = _.omit cols, col
    subrows = @_cross cols
    for v, idx in data
      for subrow in subrows
        row = {}
        row[col] = v
        _.extend row, subrow
        rows.push row
    return rows

  @createKeyF: (cols, schema) ->
    objcols = {}
    if schema?
      for col in cols
        if schema.type(col) == data.Schema.object
          objcols[col] = yes
 
    getkey = (row) -> 
      vals = []
      res = ""
      for col in cols
        v = row.get col
        vals.push v
        if col of objcols
          for k,vv of v
            res += "#{k}++#{vv}"
        else
          res += v
        res += "::"
      [vals, res]


  #
  # build hash table based on equality of columns
  # @param cols columns to use for equality test
  # @param complete create partition for cross product of
  #        partitioned columns?
  # @return [ht, keys]  where
  #   ht = JSON.stringify(key) -> rows
  #   keys: JSON.stringify(key) -> key
  @buildHT: (t, cols, complete=no) ->
    getkey = @createKeyF cols, t.schema

    uniqvals = {}
    ht = {}
    t.each (oldrow) ->
      row = oldrow.shallowClone()
      [key, strkey] = getkey row
      # XXX: may need to use toJSON on key
      unless strkey of ht
        ht[strkey] = { str: strkey, key: key, rows: [] } 
      ht[strkey].rows.push row

      if complete
        for col, idx in cols
          uniqvals[col] = {} unless col of uniqvals
          uniqvals[col][key[idx]] = yes

    if complete
      for key, o of uniqvals
        uniqvals[key] = _.keys(o)

      for row in _.cross(uniqvals)
        [key, strkey] = getkey row
        unless strkey of ht
          ht[strkey] = { str: strkey, key: key, rows: [] }

    ht

