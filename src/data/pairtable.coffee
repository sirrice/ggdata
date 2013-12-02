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

  leftSchema: -> @left().schema
  rightSchema: -> @right().schema
  clone: -> new data.PairTable @left().clone(), @right().clone()

  sharedCols: ->
    s1 = @leftSchema()
    s2 = @rightSchema()
    cols = _.uniq _.flatten [s1.cols, s2.cols]
    _.filter cols, (col) =>
      (s1.has(col) and s2.has(col) and 
      (s1.type(col) == s2.type(col)))

  # create a list of pairtables.  one for each partition
  partition: (cols, type='outer') ->
    cols = _.flatten [cols]
    cols = _.intersection cols, @sharedCols()
    left = @left().partition(cols, 'left')
    right = @right().partition(cols, 'right')
    pairs = left.join right, cols, type
    pairs.map (row) => 
      l = row.get 'left'
      l = new data.RowTable(@left().schema) unless l?
      r = row.get 'right'
      r = new data.RowTable(@right().schema) unless r?
      new data.PairTable l, r

  # partition on _all_ of the shared columns
  # 
  # enforces invariant: each md should have 1+ row
  fullPartition: () -> 
    cols = @sharedCols()
    right = @left().project(cols, no).distinct().join(@right(), cols)
    pt = new data.PairTable(@left(), right)
    pt.partition cols

  # ensures there MD tuples for each unique combination of keys
  # if MD partition has records, clone any record and overwrite keys
  # otherwise use MD schema to create new record
  ensure: (cols=[]) ->
    cols = _.flatten [cols]
    left = @left()
    right = @right()
    sharedCols = _.filter cols, (col) -> left.has(col) and right.has(col)
    restCols = _.reject cols, (col) => right.schema.has col
    unknownCols = _.reject restCols, (col) => left.schema.has col
    restCols = _.filter restCols, (col) => left.schema.has col
    if unknownCols.length > 0
      @log.warn "ensure dropping unknown cols: #{unknownCols}"

    newrSchema = right.schema.clone()
    newrSchema.merge left.schema.project(restCols)
    cols = _.map cols, (col) =>
      if left.has col
        col
      else
        {
          alias: col
          f: () -> null
          type: data.Schema.ordinal
          cols: []
        }


    rights = []
    for p in @partition sharedCols
      l = p.left()
      r = p.right()

      if r.nrows() > 0
        createcopy = ((r) -> () -> r.all())(r)
      else if right.nrows() > 0
        # use any copy from right() but erase the values of the join columns
        createcopy = ((r) -> 
          () -> 
            row = r.any().clone()
            for col in cols
              row.set col, null
            [row]
        )(right)
      else
        createcopy = () -> [new data.Row newrSchema]
        
      ldistinct = l.project(cols, no).distinct()
      r = ldistinct.cross(p.right(), 'outer', null, createcopy)
      rights.push r

    right = new data.ops.Union rights
    new data.PairTable left, right


  @union: () ->
    pts = _.flatten arguments
    lefts = _.map pts, (pt) -> pt.left()
    rights = _.map pts, (pt) -> pt.right()
    new data.PairTable(
      new data.ops.Union(lefts),
      new data.ops.Union(rights)
    )

