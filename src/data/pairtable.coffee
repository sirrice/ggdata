#<< data/table

# 
# wrapper class for pair of tables
#
class data.PairTable 

  constructor: (@left_=null, @right_=null) ->
    @left_ ?= new data.RowTable(new data.Schema())
    @right_ ?= new data.RowTable(new data.Schema())
    @log = data.util.Log.logger 'data.pairtable', 'pairtable'

  
  left: (t) ->
    @left_ = t if t?
    @left_
  
  right: (t) ->
    @right_ = t if t?
    @right_

  lefts: -> [@left()]
  rights: -> [@right()]

  leftSchema: -> @left().schema
  rightSchema: -> @right().schema
  clone: -> new data.PairTable @left().clone(), @right().clone()

  unpartition: -> @

  sharedCols: ->
    data.PairTable.sharedCols @leftSchema(), @rightSchema()

  addSharedCol: (col, val) ->
    @left @left().setColVal col, val
    @right @right().setColVal col, val
    @

  partitionOn: (cols, type='outer') ->
    cols = _.flatten [cols]
    o = data.PairTable.partition @left(), @right(), cols, type
    new data.PartitionedPairTable o.partition, cols, @leftSchema(), @rightSchema()

  # create a list of pairtables.  one for each partition
  partition: (cols, type='outer') ->
    data.PairTable.partition(@left(), @right(), cols, type).pts

  # partition on _all_ of the shared columns
  # 
  # enforces invariant: each md should have 1+ row
  fullPartition: () -> 
    cols = @sharedCols()
    right = @left().melt().project(cols, no).distinct().join(@right(), cols)
    pt = new data.PairTable(@left(), right)
    pt.partition cols

  # ensures there MD tuples for each unique combination of keys
  # if MD partition has records, clone any record and overwrite keys
  # otherwise use MD schema to create new record
  ensure: (cols=[]) ->
    data.PairTable.ensure @left(), @right(), cols, @log

  @ensure: (left, right, cols, log) ->
    cols = _.flatten [cols]
    sharedCols = _.filter cols, (col) -> left.has(col) and right.has(col)
    restCols = _.reject cols, (col) => right.schema.has col
    unknownCols = _.reject restCols, (col) => left.schema.has col
    restCols = _.filter restCols, (col) => left.schema.has col
    if log? and unknownCols.length > 0
      log.warn "ensure dropping unknown cols: #{unknownCols}"

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
    for col in cols
      canonicalMD.set(col, left.any(col)) if left.has col
    createcopy = () -> 
      row = canonicalMD.clone().reset()
      [row]
    nrows = right.nrows()
    partitioned = (new data.PairTable(left.project(mapping, no).distinct(), right)).partition(sharedCols)
    rights = for [key, p] in partitioned
      p.left().cross(p.right(), 'outer', null, createcopy)

    right = new data.ops.Union rights
    new data.PairTable left, right


  # create a list of pairtables.  one for each partition
  @partition: (left, right, cols, type='outer') ->
    sharedcols = data.PairTable.sharedCols(left.schema, right.schema)
    cols = _.flatten [cols]
    cols = _.intersection cols, sharedcols
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
    keyf = data.ops.Util.createKeyF cols, leftp.schema
    pts = pairs.map (row) -> 
      l = row.get 'left'
      r = row.get 'right'
      l ?= new data.RowTable leftp.schema
      r ?= new data.RowTable rightp.schema
      str = keyf(row)[1]
      [str, new data.PairTable(l, r)]
    {
      pts: pts,
      partition: pairs
    }


  @sharedCols: (s1, s2) ->
    cols = _.uniq _.flatten [s1.cols, s2.cols]
    _.filter cols, (col) =>
      (s1.has(col) and s2.has(col) and 
      (s1.type(col) == s2.type(col)))


  @union: () ->
    pts = _.flatten arguments
    lefts = _.map pts, (pt) -> pt.left()
    rights = _.map pts, (pt) -> pt.right()
    new data.PairTable(
      new data.ops.Union(lefts),
      new data.ops.Union(rights)
    )



