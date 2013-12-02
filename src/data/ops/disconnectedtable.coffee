
class data.ops.DisconnectedTable extends data.Table

  constructor: (@table) ->
    @schema = @table.schema

  children: -> []

  nrows: -> @table.nrows()
