require "./env"
us = require "underscore"
assert = require "assert"


rows = _.times 10, (i) -> { a: i%2, x: i}
t = data.fromArray rows, null, 'col'


print = (t1) ->
  console.log t1.schema.toString()
  console.log t1.raw()
  console.log "\n"

print t.join t, ['a', 'x']

print t.distinct ['a']

md = data.fromArray [
  {x: 0, scale: 1, foo: 1}
  {x: 1, scale: 1, foo: 1}
  {x: 2, scale: 1, foo: 1}
]

pt = new data.PairTable t, md
pt = pt.ensure 'a'

console.log "pt.ensure"
print pt.right()
print pt.ensure('a').right()


console.log "full partition"
print pt.fullPartition()[0].right()

console.log "partition on a,x"
_.map pt.ensure('a').partition(['a']), (pt) ->
  print pt.right()

print t.partition('a').flatten('a')
as = t.project('a', no).distinct('a')
print  t.partition(['a', 'x']).join(as.cross(md, 'a'), ['a', 'x'], 'left').project(['a', 'x', 'scale', 'foo'], no)



###

print(t.filter (row) -> row.get('x') < 5)

rows = _.times 10, (i) -> { x: i+ 6, b: i%3}
t2 = data.Table.fromArray rows
print t.join t2, ['x'], 'outer'

print new data.ops.Cross t, t2

print t.cross t2
###
