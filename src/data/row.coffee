# Stores data as an array of values + schema
class data.Row
  @ggpackage = "data.Row"
  @id: -> @makeId -1, (data.Row::_id += 1)
  @makeId: (tid, rid) -> "r:#{tid}:#{rid}"
  _id: 0

  # @param data [ value,... ]
  # @param schema 
  constructor: (@schema, @data=null) ->
    unless @schema?
      throw Error "Row needs a schema"
    
    # universally unique row id
    @rid = @constructor.id()
    # id set by the table
    @id = @rid

    @parents = {}
    @data ?= []
    while @data.length < @schema.ncols()
      @data.push null

  reset: -> 
    @data = []
    @parents = {}
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
    ret.addProv @prov()
    ret

  # Steal column values from row argument
  # Keep existing schema
  # @param prov should we copy provenance info from @param row
  steal: (row, cols=null, prov=yes) ->
    set = no
    cols ?= @schema.cols
    for col in cols
      v = row.get col
      if v?
        @set col, v
        set = yes

    if set and prov
      @addProv row.prov() 
        
    @

  shallowClone: ->
    rowData = (d for d in @data)
    ret = new data.Row @schema, rowData
    ret.addProv @prov()
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
    ret.addProv @prov()
    ret


  addProv: (ids) ->
    for id in _.flatten [ids]
      @parents[id] = yes

  prov: -> _.keys @parents

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
