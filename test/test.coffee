require "./env"
us = require "underscore"
assert = require "assert"


rows = _.times 10, (i) -> { a: i%2, x: i, y: i, b: i%5}
t = data.fromArray rows, null, 'col'


print = (t1) ->
  console.log t1.schema.toString()
  console.log t1.raw()
  console.log "timings: #{t1.timings()}"
  console.log "\n"

desc = {
  alias: 'x'
  f: (x) -> x
  type: data.Schema.numeric
  cols: 'x'
}

table = t
for i in [1...2000]
  table = table.project [desc]
for i in [1...100]
  table = table.setColVal 'x', 99
print table



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

print t

arr1 = [ {a: 0}, {a:1} ]
arr2 = [ {z: 1} ]
arr3 = [ ]
l = data.Table.fromArray arr1
r = data.Table.fromArray arr3
n = new data.RowTable l.schema, arr3
pt = new data.PairTable l,r
pt = pt.ensure []
pt = pt.ensure ['a']
print pt.right()


t = t.project [{
  alias: 'foo'
  f: (x,y) -> x * y + 100000
  cols: ['x', 'y']
}
{
  alias: 'bar'
  f: (x) -> new Date("2013/#{x}/01")
  cols: 'x'
  }
{
  alias: 'baz'
  f: (x) -> "#{x}"
  cols: 'x'
}
{
  alias: 'tam'
  f: (x) -> ["#{x}"]
  cols: 'x'
}
{
  alias: 'mine'
  f: -> 99
  cols: []
  }
]

print t.limit(1)

console.log t.limit(1).colProv('mine')
console.log t.project([{alias: 'tam', f: ((v) -> v + " foo"), cols: 'baz'}]).colProv('tam')
tt = t.project([{alias: 'tam', f: ((v) -> v + " foo"), cols: 'baz'}]).mapCols({alias: 'tam', f: (v) -> v})
console.log tt.colProv('tam')
console.log t.limit(1).colProv('tam')

pt = new data.PairTable tt, t


for p in pt.fullPartition()
  console.log p.left().graph()

console.log l.partition('a')
console.log l.partition('a')

###
###

print(t.filter (row) -> row.get('x') < 5)

rows = _.times 10, (i) -> { x: i+ 6, b: i%3}
t2 = data.Table.fromArray rows
print t.join t2, ['x'], 'outer'

print new data.ops.Cross t, t2

print t.cross t2
###

