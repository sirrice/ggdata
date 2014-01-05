#<< data/pairtable

class data.PartitionedPairTable extends data.PairTable

  constructor: (@table, @cols, @lschema, @rschema) ->
    unless @table.has 'left'
      throw Error
    unless @table.has 'right'
      throw Error

    if @cols.length + 2 != @table.cols().length
      #throw Error "table should have #{@cols.length+2} cols, has #{@table.ncols()}"
      1

    @keyf = data.ops.Util.createKeyF cols, @table.schema

    # cache a local clone of the rows, these will be update in place
    @table = @table.cache() unless _.isType @table, data.ops.Array
    @ht = {}
    @rows = @table.rows
    for row in @rows
      str = @keyf(row)[1]
      if str of @ht
        cur = @ht[str]
        cur.set 'left', (cur.get('left').union row.get('left'))
        cur.set 'right', (cur.get('right').union row.get('right'))
      else
        @ht[str] = row

    @left_ = null
    @right_ = null

  addProv: (lefts, rights)->
    lefts = _.compact _.flatten [lefts]
    rights = _.compact _.flatten [rights]
    pstore = ggprov.Prov.get()
    for l in lefts
      pstore.connect l, @left(), 'table'
    for r in rights
      pstore.connect r, @right(), 'table'
    

  left: (t) ->
    if t?
      throw Error "PartitionedPairTable cannot set left"
    ls = @lefts()
    if @left_?
      @left_.tables = ls
      @left_.ensureSchema()
      @left_.setProv()
    else
      @left_ = new data.ops.Union ls
      ggprov.Prov.get().connect @table, @left_, 'table'
    @left_

  lefts: -> _.compact @rows.map (row) -> row.get 'left'

  right: (t) ->
    if t?
      throw Error "PartitionedPairTable cannot set right"
    rs = @rights()
    if @right_?
      @right_.tables = rs
      @right_.ensureSchema()
      @right_.setProv()
    else
      @right_ = new data.ops.Union rs
      ggprov.Prov.get().connect @table, @right_, 'table'
    @right_

  rights: -> _.compact @rows.map (row) -> row.get 'right'

  leftSchema: -> @lschema
  rightSchema: -> @rschema
  clone: -> 
    ret = new data.PartitionedPairTable @table, @cols, @lschema, @rschema
    ret.addProv @left(), @right()
    ret

  rmSharedCol: (col) ->
    unless col in @cols
      return @

    schema = @table.schema.exclude col
    lschema = @lschema.exclude col
    rschema = @rschema.exclude col
    cols = _.without @cols, col

    rows = for row in @rows
      schema.cols.map (c) -> row.get c
    table = new data.RowTable schema, rows
    ret = new data.PartitionedPairTable table, cols, lschema, rschema
    ret.addProv @left(), @right()
    ret

  addSharedCol: (col, val, type) ->
    if col in @cols
      throw Error "cannot add shared col that exists"

    type ?= data.Schema.type val 
    schema = @table.schema.clone()
    schema.addColumn col, type
    cols = @cols.concat [col]

    rows = for row in @rows
      row = row.data.map _.identity
      row.push val
      row
    table = new data.RowTable schema, rows
    ret = new data.PartitionedPairTable table, cols, @lschema, @rschema
    ret.addProv @left(), @right()
    ret

  unpartition: ->
    new data.PairTable(
      @left()
      @right()
    )

  update: (key, pt) ->
    unless key of @ht
      throw Error
    @ht[key].set 'left', pt.left()
    @ht[key].set 'right', pt.right()
    @

  ensure: (cols) ->
    ensureCols = _.difference cols, @cols
    if ensureCols.length > 0
      newtable = @table.project {
        alias: ['left', 'right']
        cols: ['left', 'right']
        type: data.Schema.table
        f: (l, r) ->
          pt = data.PairTable.ensure l, r, ensureCols
          {
            left: pt.left()
            right: pt.right()
          }
      }
      ret = new data.PartitionedPairTable newtable, @cols, @lschema, @rschema
      ret.addProv @left(), @right()
      return ret

    return @


  partition: (cols) ->
    if _.difference(cols, @cols).length > 0
      @partitionOn(cols).partition cols
    else if _.difference(@cols, cols).length > 0
      ht = {}
      for row in @rows
        key = @keyf(row)[1]
        ht[key] = [] unless key of ht
        ht[key].push row

      for key, rows of ht
        ls = []
        rs = []
        for row in rows
          ls.push row.get 'left'
          rs.push row.get 'right'
        l = new data.ops.Union ls
        r = new data.ops.Union rs
        [key, new data.PairTable(l, r)]
    else
      for key, row of @ht
        l = row.get 'left'
        r = row.get 'right'
        [key, new data.PairTable(l, r)]

  partitionOn: (cols) ->
    cols = _.compact _.flatten [cols]
    diffcols = _.difference(cols, @cols)
    mycols = _.difference @cols, cols
    if diffcols.length > 0
      newtables = []
      _.each @rows, (row) ->
        partitions = data.PairTable.partition row.get('left'), row.get('right'), diffcols
        mapping = mycols.map (mycol) ->
          {
            alias: mycol
            cols: []
            type: row.schema.type mycol
            f: -> row.get(mycol)
          }
        proj = partitions.partition.project mapping
        newtables.push proj
      union = new data.ops.Union newtables
      ret = new data.PartitionedPairTable union, _.union(@cols,cols), @lschema, @rschema
      ret.addProv @left(), @right()
      ret
    else
      @

      
  @fromPairTables: (pts) ->
    cols = {}
    pts = for pt in pts
      if _.isType pt, data.PartitionedPairTable
        for col in pt.cols
          cols[col] = yes
        pt
      else
        [l, r] = [pt.left(), pt.right()]
        schema = new data.Schema ['left', 'right'], [data.Schema.table, data.Schema.table]
        table = new data.RowTable schema, [[l, r]]
        newppt = new data.PartitionedPairTable table, [], pt.leftSchema(), pt.rightSchema()
        newppt.addProv l, r
        newppt

    cols = _.keys cols

    rows = []
    schema = null
    prov = for pt in pts
      pt = pt.partitionOn cols
      rows.push.apply rows, pt.table.rows
      schema ?= pt.table.schema
      pt.left()

    newtable = new data.ops.Array schema, rows, prov
    new data.PartitionedPairTable newtable, cols, pts[0].leftSchema(), pts[0].rightSchema()




