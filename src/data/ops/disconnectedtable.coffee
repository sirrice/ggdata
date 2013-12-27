#<< data/table

class data.ops.DisconnectedTable extends data.Table

  constructor: (@table) ->
    super
    @schema = @table.schema
    @setProv()

  children: -> []

  nrows: -> @table.nrows()
  iterator: -> @table.iterator()
