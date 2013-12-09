require "./env"
us = require "underscore"
assert = require "assert"


rows = _.times 4, (i) ->  {
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
t.each (row) ->
  console.log row.prov()

proj = t.project 'a'

proj.each (row) ->
  console.log row.clone().prov()

console.log "filter"
f = t.filter {col: 'a', val: 0}
f.each (row) ->
  console.log row.prov()

console.log "filter2"
f = t.filter {col: 'a', f: (a) -> a==0}
f.each (row) ->
  console.log row.prov()

console.log "filter"
f = t.filter ((row)->row.get('a')==0)
f.each (row) ->
  console.log row.prov()

console.log "filter"
f = t.filter {f:(row)->row.get('a')==0}
f.each (row) ->
  console.log row.prov()

throw Error

console.log "partitioning"
part = proj.partition 'a'
part.each (row) ->
  console.log row.prov()


console.log "aggregate"
agg = part.aggregate data.ops.Aggregate.count()
agg.each (row) -> console.log row.prov()

console.log "second table"
r = data.fromArray [{a: 1, zz: 9}, {a:1, zz:10}, {a: 3, zz:11}]
r.each (row) -> 
  console.log row.prov()


console.log "rawjoin"
join = t.join r, 'a', 'left'
join.each (row) -> 
  console.log row.prov()

console.log "partition join"
join = part.join r, 'a', 'outer'
join.each (row) ->
  console.log row.prov()


pt = new data.PairTable part, r
pt = pt.ensure 'a'
console.log "ensure left"
pt.left().each (row) ->
  console.log row.prov()

console.log "ensure right"
pt.right().each (row) ->
  console.log row.get 'a'
  console.log row.prov()

console.log "flatten"
part.flatten().each (row) ->
  console.log row.prov()

console.log "t union t"
t.union(t).each (row) ->
  console.log row.prov()

console.log "join.cache"
join.cache().each (row) ->
  console.log row.prov()

console.log "join.once 1st"
once = join.once()
once.each (row) ->
  console.log row.prov()

console.log "join.once 2nd"
once.each (row) ->
  console.log row.prov()

console.log "pt.partition 'a'"
for pt2, idx in pt.partition 'a'
  console.log "left partition #{idx}"
  pt2.left().each (row) ->
    console.log row.prov()
  console.log "right partition #{idx}"
  pt2.right().each (row) ->
    console.log row.prov()



throw Error

test = (name, n, table) ->
  perrowCosts = []
  timer = new data.util.Timer()
  _.times n, (i) ->
    timer.start()
    table.each(()->)
    timer.stop()

  avgCost = timer.avg()
  setup = timer.avg('iter')
  console.log "#{name}\ttook: #{avgCost}  #{setup}*#{timer.count('iter')}"#\t#{avgPerRow}/outputrow\t#{d3.mean nrows} rows"
  print table


print = (t1) ->
  console.log t1.schema.toString()
  console.log t1.raw()[0..10]
  console.log "\n"

desc = {
  alias: 'x'
  f: (x) -> x+1
  type: data.Schema.numeric
  cols: 'x'
}

print t.distinct([])
throw Error


table = t.project({alias: 'foo', type: data.Schema.unknown, f: (row) -> row.get('a')})
test "* project", 10, table
test "* project", 10, table
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

table = t
for i in [1..200]
  table = table.blockproject [desc]
test "2k projects", 10, table

table = t
for i in [1..200]
  table = table.blockproject 'x'
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
