# Stores data as an array of values + schema
class data.Row
  @ggpackage = "data.Row"
  @id: -> data.Row::_id += 1
  _id: 0

  # @param data [ value,... ]
  # @param schema 
  constructor: (@schema, @data=null) ->
    unless @schema?
      throw Error "Row needs a schema"
    
    @id = @constructor.id()
    @parents = []
    @data ?= []
    while @data.length < @schema.ncols()
      @data.push null

  reset: -> 
    @parents = []
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
    ret = new data.Row schema, rowData
    ret.parents.push @id
    ret

  # Steal column values from row argument
  # Keep existing schema
  steal: (row, cols=null) ->
    cols ?= @schema.cols
    for col in cols
      v = row.get col
      @set col, v if v?
    @parents.push row.id
        
    @

  shallowClone: ->
    rowData = (d for d in @data)
    ret = new data.Row @schema, rowData
    ret.parents.push @id
    ret

  clone: ->
    rowData = for d in @data
      if d? and d.clone?
        d.clone()
      else if d == undefined
        null
      else
        d
    ret = new data.Row @schema, rowData
    ret.parents.push @id
    ret


  toJSON: -> 
    o = {
      schema: @schema.toJSON()
      data: @data
    }
    for col, idx in @schema.cols
      o[col] = @data[idx]
    o

  raw: -> 
    o = {}
    for col, idx in @schema.cols
      o[col] = @data[idx]
    o

  toString: -> JSON.stringify(@raw())

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
