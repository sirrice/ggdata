require "./env"
us = require "underscore"
assert = require "assert"


rows = _.times 1000, (i) ->  {
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
t = data.fromArray rows, null, 'col'

test = (name, n, table) ->
  perrowCosts = []
  _.times n, (i) ->
    table.each(()->)

  timer = table.timer()
  avgCost = table.timer().avg()
  setup = table.timer().avg('iter')
  console.log "#{name}\ttook: #{avgCost}  #{setup}*#{timer.count('iter')}"#\t#{avgPerRow}/outputrow\t#{d3.mean nrows} rows"
  print table


print = (t1) ->
  console.log t1.schema.toString()
  console.log t1.raw()[0..10]
  console.log "timings: #{t1.timings()}"
  console.log "\n"

desc = {
  alias: 'x'
  f: (x) -> x+1
  type: data.Schema.numeric
  cols: 'x'
}

test "* project", 10, t.project({alias: 'foo', type: data.Schema.unknown, f: (row) -> row.get('a')})
test "col unknown project", 10, t.project({
  alias: 'foo', 
  col: [],
  type: data.Schema.unknown,
  f: () -> 1})

table = t
for i in [1..200]
  table = table.project [desc]
test "2k projects", 10, table

table = t
for i in [1..200]
  table = table.project 'x'
test "2k raw projects", 10, table

###

xytable = data.ops.Util.cross({
  'facet-x': [0, 1],
  'facet-y': [1] 
});
print xytable
rows = [ 
  { 'facet-x': 0, 'facet-y': 1, layer: 0 }, 
  {'facet-x': null, layer:9}]
md = data.fromArray rows, null
pt = new data.PairTable(xytable, md)
pt = pt.ensure(['facet-x', 'facet-y'])
md = pt.right()

print md

print xytable.join(md, ['facet-x', 'facet-y'], 'outer')

leftrows = [ {'facet-x': 0, 'facet-y': 1, data: 9}]
left = data.fromArray leftrows, null
pt = new data.PairTable left, md
ps = pt.partition ['facet-x', 'facet-y']
for p in ps
  print p.left()
  print p.right()


###
