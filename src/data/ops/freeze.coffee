#<< data/table

class data.ops.Freeze extends data.Table
  constructor: (@table) ->
    @schema = @table.schema
    super
    @frozen = yes

  children: -> [@table]
  melt: -> @table
  iterator: -> @table.iterator()