require "../env"
vows = require "vows"
assert = require "assert"

suite = vows.describe "perf.js"
Table = data.Table
Schema = data.Schema

makeTable = (n=10, type="row") ->
  rows = _.times n, (i) -> {
    a: i%2, 
    b: "#{i}"
    c: i%5
    d: i%10
    e: i%100
    f: i%500
    x: i
    y: {
      z: i
    }
  }
  Table.fromArray rows, null, type

test = (name, n, table) ->
  perrowCosts = []
  _.times n, (i) ->
    table.each(()->)

  timer = table.timer()
  avgCost = table.timer().avg()
  setup = table.timer().avg('iter')
  console.log "#{name}\ttook: #{avgCost}  #{setup}*#{timer.count('iter')}"#\t#{avgPerRow}/outputrow\t#{d3.mean nrows} rows"

testf = (name, n, f) ->
  timer = new data.util.Timer()
  _.times n, (i) ->
    timer.start()
    f()
    timer.stop()
  console.log "#{name}\ttook: #{timer.avg()}"





table = makeTable(1000)
niters = 20

if no
  test "base", niters, table

if no
  test "single project", niters, table.project('x')

  proj = table
  for i in [1...501]
    proj = proj.project [{ alias: 'x', cols: 'x', f: (v) -> v+1 }]
    if i < 5 or (i%100 == 0)
      test "projected #{i} times ", 50, proj

if no
  union = table.union table
  test "union", niters, union

  filter = table.filter (row) -> row.get('x') < 100
  test "filter", niters, filter

 
if no
  for col in ['a', 'c', 'd', 'e', 'f', ['a', 'c']]
    f = ((table, col) -> () -> data.ops.Util.buildHT table, col)(table, col)
    testf "buildHT on #{col} with #{_.size f()} bucks", niters, f


for col in ['d', 'e']
  part = table.partition(col)
  test "Cached Part on #{col}", niters, part
  for i in [1]
    aggs = _.times(i, () -> data.ops.Aggregate.count())
    res = part.aggregate aggs
    test "Part on #{col} w/ #{part.nrows()} parts & #{i} aggs: ", niters, res


if no
  for jointype in ['left', 'outer']
    for nrows in [10, 50, 100, 500]
      t = makeTable(nrows)
      rows = t.all()
      schema = t.schema
      f = ((schema, rows) -> () -> 
        iter = data.ops.Util.crossArrayIter schema, rows, rows
        i = 0
        while iter.hasNext()
          iter.next()
          i += 1
        iter.close()
        i
      )(schema, rows)
      nrows = f()
      testf "#{jointype} arrcross on #{col} with nres #{nrows}", niters, f


if no
  for col in ['a', 'c', 'd', 'e', 'f', ['a', 'c']]
    join = table.join table, col
    test "join on #{col} with nres #{join.nrows()}", niters, join

  for col in ['a', 'c', 'd', 'e', 'f', ['a', 'c']]
    part = table.partition col
    test "partition on #{col} with #{part.nrows()} rows", niters, part

 
