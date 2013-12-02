

class data.util.Traverse

  @bfs: (t, f=_.identity) ->
    seen = {}
    q = [t]
    ret = []

    while q.length > 0
      n = q.shift()
      continue if n.id of seen

      seen[n.id] = yes
      ret.push f n

      for c in n.children()
        q.push c
    ret

  @dfs: (t, f=_.identity, path=[], seen={}, ret=[]) ->
    return ret if t.id of seen
    seen[t.id] = yes
    ret.push f(t, path)
    path.push t
    for c in t.children()
      @dfs c, f, path, seen, ret
    path.pop()
    ret


  @toString: (t, f=null) ->
    f ?= (n)->"#{n.constructor.name}:#{n.id} (#{n.timer().avg()}/#{n.timer().count()})"
    @_toString(t, f).join("\n")


  @_toString: (t, f) ->
    if t.children().length == 0
      [f(t)]
    else
      ret = [f(t)]
      for c in t.children()
        for line in @_toString(c, f)
          line = " #{line}"
          ret.push line
      ret



