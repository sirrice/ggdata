require "./env"
us = require "underscore"
assert = require "assert"


rows = _.times 10, (i) -> { a: i%2, x: i}
t = data.fromArray rows, null, 'col'


print = (t1) ->
  console.log t1.schema.toString()
  console.log t1.raw()
  console.log "timings: #{t1.timings()}"
  console.log "\n"

print t

console.log "join"
print t.join t, ['a', 'x']

left = data.Table.fromArray [
  { x: 1, y: 1, l: 1}
  { x: 1, y: 2, l: 1}
  { x: 2, y: 2, l: 3}
  { x: 2, y: 3, l: 2}
]
right = data.Table.fromArray [
  { x: 1, l: 1, z: 0 }
  { x: 1, l: 3, z: 1 }
  { x: 2, l: 2, z: 2 }
  { x: 2, l: 1, z: 3 }
]
console.log "first ensure on xy"
pt = new data.PairTable left, right
pt = pt.ensure ['x', 'y']
console.log pt.right().raw()
console.log "\n"

#console.log "cross"
#cross = t.cross t
#cache = cross.cache()
#console.log cross.timings()
#console.log cross.timings('setup')
#console.log cross.timer().avg('innerloop')
#print cross
#console.log "cache"
#console.log cache.graph()
#
#console.log "distinct"
#print t.distinct ['a']

md = data.fromArray [
  { z: 9 }
]

pt = new data.PairTable t, md
pta = pt.ensure 'a'

console.log "pt.ensure 'a'"
print pta.right()

console.log "pt.ensure []"
ptnone = pt.ensure []
print ptnone.right()


###

print(t.filter (row) -> row.get('x') < 5)

rows = _.times 10, (i) -> { x: i+ 6, b: i%3}
t2 = data.Table.fromArray rows
print t.join t2, ['x'], 'outer'

print new data.ops.Cross t, t2

print t.cross t2
###

