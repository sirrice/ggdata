# Stores data as an array of values + schema
class data.Row
  @ggpackage = "data.Row"

  # @param data [ value,... ]
  # @param schema 
  constructor: (@schema, @data=null) ->
    unless @schema?
      throw Error "Row needs a schema"
    
    @data ?= []
    while @data.length < @schema.ncols()
      @data.push null

  reset: -> 
    @data = []
    @

  cols: -> @schema.cols
  has: (col, type=null) -> @schema.has col, type
  contains: (col, type=null) -> @schema.has col, type
  get: (col) -> 
    v = @data[@schema.index(col)] 
    v = null if v == undefined
    v
  set: (col, v) -> @data[@schema.index(col)] = v
  project: (cols) ->
    if _.isType cols, data.Schema
      schema = cols
      cols = schema.cols
    else
      cols = _.flatten [cols]
      schema = @schema.project cols
    rowData = _.map cols, (col) => @get col
    new data.Row schema, rowData

  # This is not performant within a tight loop because it infers the 
  # merged schema
  #
  # XXX: assumes null value means col value not set. e.g., 
  #      {x: 1}.merge({x:null}) returns {x: 1}
  #
  # @param cols columns to merge into this row.  if null, merges all
  merge: (row, cols=null) ->
    unless _.isType row, data.Row
      throw Error "can't merge non row"
    cols ?= row.schema.cols
    schema = @schema.clone()
    schema.merge row.schema
    ret = new data.Row schema
    for col in @schema.cols
      ret.set col, @get(col)
    for col in cols
      v = row.get(col)
      ret.set col, v if v?
        
    ret
  
  # Steal column values from row argument
  # Keep existing schema
  steal: (row, cols=null) ->
    cols ?= @schema.cols
    for col in cols
      v = row.get col
      @set col, v if v?
        
    @

  shallowClone: ->
    rowData = (d for d in @data)
    new data.Row @schema, rowData

  clone: ->
    rowData = _.map @data, (v) ->
      if v? and v.clone?
        v.clone()
      else if v == undefined
        null
      else
        v
    new data.Row @schema, rowData


  toJSON: -> 
    o = {}
    for col, idx in @schema.cols
      o[col] = @data[idx]
    o
  raw: -> @toJSON()
  toString: -> JSON.stringify(@toJSON())

  # turns an { } object into a data.Row
  @toRow: (o, schema=null) ->
    return o if _.isType o, data.Row

    unless schema?
      schema = new data.Schema 
      for k,v of o
        schema.addColumn k, data.Schema.type(v)

    rowData = []
    for col in schema.cols
      idx = schema.index col
      rowData[idx] = o[col]
    new data.Row schema, rowData

  # given a target row (first arg), pick column values from
  # list of rows in order (last row overwrites others)
  @merge: (schema, r1, r2) ->
    ret = new data.Row schema
    for col, idx in schema.cols
      if r2.has col
        ret.data[idx] = r2.get(col)
      else
        ret.data[idx] = r1.get(col)
    ret



