class data.ops.Freeze extends data.Table
  constructor: (@table) ->
    @schema = @table
    super

  children: -> [@table]
  melt: -> @table