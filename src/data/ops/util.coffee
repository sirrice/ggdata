

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
          rights = [rightf()]
          
      when "right"
        unless lhasRows
          lefts = rights
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
        @_row.reset().steal(l).steal(r)
        @_row

      hasNext: -> @idx < @nrows
      close: ->
        @lefts = @rights = null
    new Iter schema, lefts, rights

  # Creates a table that's cross product of the attrs
  # @param cols { attr1: [ values], ... }
  @cross: (cols, tabletype=null) ->
    rows = @_cross cols
    return data.Table.fromArray rows, null, tabletype

  # @param cols object of { colname: list of values }
  @_cross: (cols, tabletype=null) ->
    if _.size(cols) == 0
      return [{}]
    rows = []
    col = _.first _.keys cols
    data = cols[col]
    cols = _.omit cols, col
    for v, idx in data
      for subrow in @_cross cols
        row = {}
        row[col] = v
        _.extend row, subrow
        rows.push row
    return rows


  #
  # build hash table based on equality of columns
  # @param cols columns to use for equality test
  # @return [ht, keys]  where
  #   ht = JSON.stringify(key) -> rows
  #   keys: JSON.stringify(key) -> key
  @buildHT: (t, cols) ->
    getkey = (row) -> _.map cols, (col) -> row.get(col)
    ht = {}
    keys = {}
    t.each (row) ->
      row = row.clone()
      key = getkey row
      strkey = JSON.stringify key
      # XXX: may need to use toJSON on key
      ht[strkey] = [] unless strkey of ht
      ht[strkey].push row
      keys[strkey] = key

    _.o2map ht, (rows, keystr) ->
      [ keystr,
        { str: keystr, key: keys[keystr], table: ht[keystr] }
      ]

