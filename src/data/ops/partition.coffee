#<< data/table

# Equi-partition on a set of columns
#
class data.ops.Partition extends data.Table

  # @param alias attribute name for the partition table
  constructor: (@table, @cols, @alias='table') ->
    @cols = _.flatten [@cols]
    @schema = @table.schema.clone()#.project @cols
    @schema.addColumn @alias, data.Schema.table
    @ht = null
    super


  children: -> [@table]
  iterator: ->
    timer = @timer()
    unless @ht?
      timer.start()
      @ht = _.values(data.ops.Util.buildHT @table, @cols)
      timer.stop()

    class Iter
      constructor: (@schema, @table, @ht, @cols, @alias) ->
        @_row = new data.Row @schema
        @idx = 0

      reset: -> 
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        timer.start()
        htrow = @ht[@idx]
        @idx += 1
        @_row.reset()
        for col, idx in @cols
          @_row.set col, htrow.key[idx]

        partitionf = new data.ops.Array(
          @table.schema,
          htrow.rows,
          [@table]
        )

        if partitionf.nrows() > 0
          @_row.steal partitionf.any()
        @_row.set @alias, partitionf
        timer.stop()
        @_row

      hasNext: -> 
        @idx < @ht.length

      close: -> 
        @table = null

    new Iter @schema, @table, @ht, @cols, @alias



