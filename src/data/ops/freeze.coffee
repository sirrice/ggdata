class data.ops.Freeze extends data.Table
  constructor: (@table) ->
    @schema = @table.schema
    super

  children: -> [@table]
  melt: -> @table