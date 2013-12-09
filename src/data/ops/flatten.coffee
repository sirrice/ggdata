#<< data/table

class data.ops.Flatten extends data.Table
  constructor: (@table) ->
    @schema = @table.schema
    tablecols = _.filter @schema.cols, (col) =>
      @schema.type(col) == data.Schema.table
    tablecol = tablecols[0]
    othercols =  _.reject @schema.cols, (col) =>
      @schema.type(col) == data.Schema.table
    otherSchema = @schema.project othercols
    super

    @timer().start()
    newtables = @table.map (row) ->
      lefto = _.o2map othercols, (col) ->
        [col, row.get(col)]
      left = data.Table.fromArray [lefto], otherSchema
      console.log row.get(tablecol)
      right = data.Table.fromArray row.get(tablecol)
      left.cross right
    @timer().stop()

    @iter = new data.ops.Union newtables

  nrows: -> @iter.nrows()
  children: -> [@table]
  iterator: -> @iter.iterator()
