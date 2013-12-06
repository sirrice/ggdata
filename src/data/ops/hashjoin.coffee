#<< data/table

class data.ops.HashJoin extends data.Table

  # @param leftf/rightf methods to generate default rows 
  #        if left or right arrays are empty. default:
  #
  #          () -> new data.Row(t1.schema/t2.schema)
  #
  constructor: (@t1, @t2, @joincols, @jointype, @leftf=null, @rightf=null) ->
    @joincols = _.flatten [@joincols]
    @schema = @t1.schema.clone()
    @schema.merge @t2.schema.clone()
    @ensureSchema()
    @getkey = (row) -> _.map cols, (col) -> row.get(col)
    @timer().start 'buildht'
    @ht1 = data.ops.Util.buildHT @t1, @joincols
    @ht2 = data.ops.Util.buildHT @t2, @joincols
    @timer().stop 'buildht'
    super

    # methods to create dummy rows for outer/left/right joins
    schema1 = @t1.schema.clone()
    schema2 = @t2.schema.clone()
    @leftf ?= -> new data.Row schema1
    @rightf ?= -> new data.Row schema2

  # make sure joincols are present in t1 and t2
  ensureSchema: ->
    for col in @joincols
      unless @t1.schema.has col
        throw Error "joincol #{col} not in left table #{@t1.schema.toString()}"
      unless @t2.schema.has col
        throw Error "joincol #{col} not in right table #{@t2.schema.toString()}"

  children: -> [@t1, @t2]
  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@schema, @lschema, @rschema, @ht1, @ht2, @jointype, @leftf, @rightf) ->
        keys1 = _.keys @ht1
        keys2 = _.keys @ht2
        switch @jointype
          when "inner"
            @keys = _.intersection keys1, keys2
          when "left"
            @keys = keys1
          when "right"
            @keys = keys2
          when "outer"
            @keys = _.uniq _.flatten [keys1, keys2]
          else
            @keys = _.uniq _.flatten [keys1, keys2]

        timer.start()
        @reset()

      reset: -> 
        @keyidx = -1
        @key = null
        @iter = null

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        @iter.next()

      hasNext: -> 
        if @iter? and not @iter.hasNext()
          @iter.close()
          @iter = null

        while @iter is null and @keyidx < @keys.length
          @keyidx += 1
          @key = @keys[@keyidx]
          if @key of @ht1
            left = @ht1[@key].table 
          else
            left = new data.RowTable @lschema
          if @key of @ht2
            right = @ht2[@key].table 
          else
            right = new data.RowTable @rschema
          @iter = data.ops.Util.crossArrayIter @schema, left, right, @jointype, @leftf, @rightf
          break if @iter.hasNext()
          @iter = null

        @iter != null and @iter.hasNext()

      close: -> 
        @ht1 = @ht2 = null
        @iter.close() if @iter?
        @iter = null
        timer.stop()

    new Iter @schema, @t1.schema, @t2.schema, @ht1, @ht2, @jointype, @leftf, @rightf
