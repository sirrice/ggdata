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
]
print t


###

print(t.filter (row) -> row.get('x') < 5)

rows = _.times 10, (i) -> { x: i+ 6, b: i%3}
t2 = data.Table.fromArray rows
print t.join t2, ['x'], 'outer'

print new data.ops.Cross t, t2

print t.cross t2
###

