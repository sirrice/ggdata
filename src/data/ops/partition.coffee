#<< data/table

class data.ops.Partition extends data.Table

  constructor: (@table, @cols, @alias='table') ->
    @cols = _.flatten [@cols]
    @schema = @table.schema.project @cols
    @schema.addColumn @alias, data.Schema.table


  iterator: ->
    class Iter
      constructor: (@schema, @table, @cols, @alias) ->
        @idx = 0

      reset: -> 
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        row = new data.Row @schema
        htrow = @ht[@idx]
        @idx += 1
        for col, idx in @cols
          row.set col, htrow.key[idx]
        partition = data.Table.fromArray htrow.table, @table.schema
        row.set @alias, partition
        row

      hasNext: -> 
        unless @ht?
          @ht = _.values(data.ops.Util.buildHT @table, @cols)
        @idx < @ht.length

      close: -> 
        @table = null

    new Iter @schema, @table, @cols, @alias



