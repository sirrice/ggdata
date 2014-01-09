class data.ops.Cross extends data.Table
  constructor: (@left, @right, @jointype, @leftf=null, @rightf=null) ->
    super
    @schema = @left.schema.clone()
    @schema.merge @right.schema.clone()
    @setup()
    @setProv()


  setup: ->
    @timer().start()
    defaultf = -> new data.Row(new data.Schema)
    @leftf ?= defaultf
    @rightf ?= defaultf
    liter = @left.iterator()
    riter = @right.iterator()
    lhasRows = liter.hasNext()
    rhasRows = riter.hasNext()
    liter.close()
    riter.close()

    rrows = _.flatten [@rightf()]
    lrows = _.flatten [@leftf()]
    switch @jointype
      when "left"
        unless rhasRows
          @right = new data.ops.Array @right.schema, rrows, []
          
      when "right"
        unless lhasRows
          @left = @right
          @right = new data.ops.Array @left.schema, lrows, []

      when "outer"
        unless lhasRows
          @left = @right
          @right = new data.ops.Array @left.schema, lrows, []
        else unless rhasRows
          @right = new data.ops.Array @right.schema, rrows, []

    @timer().stop()

  nrows: -> @left.nrows() * @right.nrows()
  children: -> [@left, @right]
          
  iterator: ->
    timer = @timer()
    tid = @id
    class Iter
      constructor: (@schema, @left, @right) ->
        @_row = new data.Row @schema
        @rowidx = 0
        @liter = @left.iterator()
        @riter = @right.once().iterator()
        @lrow = new data.Row @left.schema
        @needNext = yes
        @reset()

      reset: ->
        @liter.reset()
        @riter.reset()
        @needNext = yes

      next: ->
        throw Error("iterator has no more elements") unless @hasNext()
        rrow = @riter.next()
        timer.start()
        @_row.reset()
        @_row.steal @lrow
        @_row.steal rrow.clone()
        @_row.id = data.Row.makeId tid, @rowidx
        @rowidx += 1
        timer.stop()
        @_row

      hasNext: ->
        timer.start()
        while @liter.hasNext() and (@needNext or not @riter.hasNext())
          @riter.reset()
          @riter.hasNext()
          @lrow.reset().steal @liter.next()
          @needNext = no
        timer.stop()
        not(@needNext) and @riter.hasNext()

      close: ->
        @left = @right = null
        @liter.close()
        @riter.close()

    new Iter @schema, @left, @right


