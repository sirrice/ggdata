#<< data/table

# 
# wrapper class for pair of tables
#
class data.PairTable 

  constructor: (@left_=null, @right_=null) ->
    @left_ ?= new data.RowTable(new data.Schema())
    @right_ ?= new data.RowTable(new data.Schema())
    @left @left_
    @right @right_

    @log = data.util.Log.logger 'data.pairtable', 'pairtable'

  
  left: (t) ->
    if t?
      unless _.isType t, data.PartitionedTable
        t = new data.PartitionedTable t
      @left_ = t
    @left_
  
  right: (t) ->
    if t?
      unless _.isType t, data.PartitionedTable
        t = new data.PartitionedTable t
      @right_ = t
    @right_

  leftSchema: -> @left().schema
  rightSchema: -> @right().schema
  clone: -> new data.PairTable @left().clone(), @right().clone()

  sharedCols: ->
    data.PairTable.sharedCols @leftSchema(), @rightSchema()

  addTag: (col, val, type) ->
    new data.PairTable(
      @left().addTag col, val, type
      @right().addTag col, val, type
    )

  rmTag: (col) ->
    new data.PairTable(
      @left().rmTag col
      @right().rmTag col
    )

  partitionOn: (cols, type='outer') ->
    new data.PairTable(
      @left().partitionOn(cols)
      @right().partitionOn(cols)
    )

  # create a list of pairtables.  one for each partition
  partition: (cols, type='outer') ->
    data.PairTable.partition(@left(), @right(), cols, type)

  # partition on _all_ of the shared columns
  # 
  # enforces invariant: each md should have 1+ row
  fullPartition: -> 
    cols = @sharedCols()
    pt = @ensure cols
    pt.partition cols

  # ensures there MD tuples for each unique combination of keys
  # if MD partition has records, clone any record and overwrite keys
  # otherwise use MD schema to create new record
  ensure: (cols=[]) ->
    data.PairTable.ensure @left(), @right(), cols, @log

  @ensure: (left, right, cols, log) ->
    cols = _.flatten [cols]

    sharedl = _.intersection(
      cols, 
      _.union(left.partcols, left.cols())
    )
    sharedr = _.intersection(
      cols, 
      _.union(right.partcols, right.cols())
    )
    left = left.partitionOn sharedl
    right = right.partitionOn sharedr

    newrSchema = right.schema.clone()
    newrSchema.merge left.schema.project(sharedl)

    mapping = _.map cols, (col) =>
      if left.has col
        col
      else
        {
          alias: col
          f: () -> null
          type: right.schema.type(col)
          cols: []
        }
    ldistinct = left.project(mapping, no).distinct()



    rights = []
    for [key, rp] in right.partitions right.tags()
      pt = new data.PairTable ldistinct, rp
      ps = pt.partition _.intersection(sharedl, sharedr)
      for p in ps
        l = p.left()
        r = p.right()
        if l.nrows() == 0
          newright = r
        newright = l.cross r, 'outer', null, () ->
          if rp.nrows() > 0
            row = rp.any()
            for col in cols
              row.set col, null if row.has(col)
          else
            row = new data.Row newrSchema
          row
        for tag in right.tags()
          newright = newright.addTag tag, rp.table.any(tag)
        rights.push newright

    right = data.PartitionedTable.fromTables rights
    return new data.PairTable left, right



    sharedCols = _.filter cols, (col) -> left.has(col) and right.has(col)
    restCols = _.reject cols, (col) => right.schema.has col
    unknownCols = _.reject restCols, (col) => left.schema.has col
    restCols = _.filter restCols, (col) => left.schema.has col
    if log? and unknownCols.length > 0
      log.warn "ensure dropping unknown cols: #{unknownCols}"


    # fast path if we know nothing needs to be ensured
    if cols.length == 0
      if right.nrows() == 0
        row = new data.Row right.schema
        right = new data.ops.Array right.schema, [row]
        return new data.PairTable left, right
      return new data.PairTable left, right

    newrSchema = right.schema.clone()
    newrSchema.merge left.schema.project(restCols)
    mapping = _.map cols, (col) =>
      if left.has col
        col
      else
        {
          alias: col
          f: () -> null
          type: right.schema.type(col)
          cols: []
        }
    canonicalMD = new data.Row newrSchema
    createcopy = () -> [canonicalMD.clone()]

    data.Table.timer.start('ensure')
    ldistinct = left.project(mapping, no).distinct()

    # fast path if right doesn't have any of the ensured cols
    if sharedCols.length == 0
      right = ldistinct.cross right, 'left', null, createcopy
      data.Table.timer.stop('ensure')
      return new data.PairTable left, right

    nrows = right.nrows()
    partitioned = @partition(ldistinct, right, sharedCols)
    rights = for pt in partitioned
      r = pt.right().join pt.left(), cols, 'right', createcopy, null
      r

    right = data.PartitionedTable.fromTables rights
    data.Table.timer.stop('ensure')
    new data.PairTable left, right


  # create a list of pairtables.  one for each partition
  @partition: (left, right, cols, type='outer') ->
    sharedcols = _.intersection(_.union(left.partcols, left.cols()),
      _.union(right.partcols, right.cols()))
    cols = _.flatten [cols]
    cols = _.intersection cols, sharedcols

    left = left.partitionOn cols
    right = right.partitionOn cols
    leftp = left.partition(cols, 'left')
    rightp = right.partition(cols, 'right')
    leftf = () => 
      row = new data.Row leftp.schema
      row.set 'left', new data.RowTable(left.schema)
      row
    rightf = () => 
      row = new data.Row rightp.schema
      row.set 'right', new data.RowTable(right.schema)
      row
    pairs = leftp.join rightp, cols, type, leftf, rightf
    pairs.map (row) -> 
      l = row.get 'left'
      r = row.get 'right'
      l ?= new data.RowTable leftp.schema
      r ?= new data.RowTable rightp.schema
      new data.PairTable(l, r)


  @sharedCols: (s1, s2) -> 
    _.intersection s1.cols, s2.cols


  @union: () ->
    pts = _.flatten arguments
    lefts = _.map pts, (pt) -> pt.left()
    rights = _.map pts, (pt) -> pt.right()
    new data.PairTable(
      data.PartitionedTable.fromTables lefts
      data.PartitionedTable.fromTables rights
    )



