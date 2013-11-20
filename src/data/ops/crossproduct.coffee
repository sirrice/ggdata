class data.ops.Cross extends data.Table
  constructor: (@left, @right, @jointype, @leftf=null, @rightf=null) ->
    super
    @schema = @left.schema.clone()
    @schema.merge @right.schema.clone()
    @setup()


  setup: ->
    @timer().start 'setup'
    defaultf = -> new data.Row(new data.Schema)
    @leftf ?= defaultf
    @rightf ?= defaultf
    liter = @left.iterator()
    riter = @right.iterator()
    lhasRows = liter.hasNext()
    rhasRows = riter.hasNext()
    liter.close()
    riter.close()

    switch @jointype
      when "left"
        unless rhasRows
          @right = data.Table.fromArray([@rightf()], @right.schema) 
          
      when "right"
        unless lhasRows
          tmp = @left
          @left = @right
          @right = tmp
          @right = data.Table.fromArray([@leftf()], @left.schema) 

      when "outer"
        unless lhasRows
          tmp = @left
          @left = @right
          @right = tmp
          @right = data.Table.fromArray([@leftf()], @left.schema) 
        else unless rhasRows
          @right = data.Table.fromArray([@rightf()], @right.schema) 

    @timer().stop 'setup'

  nrows: -> @left.nrows() * @right.nrows()
  children: -> [@left, @right]
          
  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@schema, @left, @right) ->
        @_row = new data.Row @schema
        @liter = @left.iterator()
        @riter = @right.cache().iterator()
        @lrow = new data.Row @left.schema
        @needNext = yes
        @reset()
        timer.start()

      reset: ->
        @liter.reset()
        @riter.reset()

      next: ->
        throw Error("iterator has no more elements") unless @hasNext()
        @_row.reset()
        @_row.steal(@lrow)
        @_row.steal(@riter.next())
        @_row

      hasNext: ->
        @needNext = yes unless @riter.hasNext()

        while @liter.hasNext() and (@needNext or not @riter.hasNext())
          timer.end 'innerloop'
          @riter.reset()
          @lrow.reset()
          @lrow.steal @liter.next()
          @needNext = no
          timer.start 'innerloop'

        not(@needNext) and @riter.hasNext()

      close: ->
        @left = @right = null
        @liter.close()
        @riter.close()
        timer.stop()

    new Iter @schema, @left, @right


