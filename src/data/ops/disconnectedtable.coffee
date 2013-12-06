#<< data/table

class data.ops.DisconnectedTable extends data.Table

  constructor: (@table) ->
    @schema = @table.schema
    super

  children: -> []

  nrows: -> @table.nrows()
  iterator: -> @table.iterator()
