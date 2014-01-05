#<< data/table

class data.ops.HashJoin extends data.Table

  # @param leftf/rightf methods to generate default rows 
  #        if left or right arrays are empty. default:
  #
  #          () -> new data.Row(t1.schema/t2.schema)
  #
  constructor: (@t1, @t2, @joincols, @jointype, @leftf=null, @rightf=null) ->
    super
    @joincols = _.flatten [@joincols]
    @schema = @t1.schema.clone()
    @schema.merge @t2.schema.clone()
    @ensureSchema()
    @getkey = (row) -> _.map cols, (col) -> row.get(col)
    @timer().start()
    @ht1 = null
    @ht2 = null
    @timer().stop()

    # methods to create dummy rows for outer/left/right joins
    schema1 = @t1.schema.clone()
    schema2 = @t2.schema.clone()
    @leftf ?= -> new data.Row schema1
    @rightf ?= -> new data.Row schema2

    @setProv()

  # make sure joincols are present in t1 and t2
  ensureSchema: ->
    for col in @joincols
      unless @t1.schema.has col
        throw Error "joincol #{col} not in left table #{@t1.schema.toString()}"
      unless @t2.schema.has col
        throw Error "joincol #{col} not in right table #{@t2.schema.toString()}"

  children: -> [@t1, @t2]
  iterator: ->
    tid = 0
    _me = @
    timer = @timer()
    class Iter
      constructor: (@schema, @lschema, @rschema, @jointype, @leftf, @rightf) ->
        @keyidx = -1
        @idx = 0
        @reset()

      reset: -> 
        @keyidx = -1
        @key = null
        @iter = null
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        ret = @iter.next()
        @idx += 1
        ret.id = data.Row.makeId tid, @idx-1
        ret

      hasNext: -> 
        unless _me.ht1?
          _me.ht1 = data.ops.Util.buildHT _me.t1, _me.joincols
          _me.ht2 = data.ops.Util.buildHT _me.t2, _me.joincols

        unless @keys?
          keys1 = _.keys _me.ht1
          keys2 = _.keys _me.ht2
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



        if @iter? and not @iter.hasNext()
          @iter.close()
          @iter = null

        timer.start()
        while @iter is null and @keyidx < @keys.length
          @keyidx += 1
          @key = @keys[@keyidx]
          if @key of _me.ht1
            left = _me.ht1[@key].rows
          else
            left = new data.RowTable @lschema
          if @key of _me.ht2
            right = _me.ht2[@key].rows
          else
            right = new data.RowTable @rschema
          @iter = data.ops.Util.crossArrayIter @schema, left, right, @jointype, @leftf, @rightf
          break if @iter.hasNext()
          @iter = null

        ret = @iter != null and @iter.hasNext()
        timer.stop()
        ret

      close: -> 
        @iter.close() if @iter?
        @iter = null

    new Iter @schema, @t1.schema, @t2.schema, @jointype, @leftf, @rightf
