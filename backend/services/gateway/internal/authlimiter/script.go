package authlimiter

import "github.com/redis/go-redis/v9"

var admissionScript = redis.NewScript(`
local retry=0
for i,key in ipairs(KEYS) do
 local raw=redis.call('GET',key)
 if raw then
  if not string.match(raw,'^%d+$') then return -1 end
  local count=tonumber(raw)
  local ttl=redis.call('PTTL',key)
  if not count or count<1 or ttl<=0 then return -1 end
  if count>=tonumber(ARGV[2*i-1]) then retry=math.max(retry,ttl) end
 end
end
if retry>0 then return retry end
for i,key in ipairs(KEYS) do
 if redis.call('EXISTS',key)==0 then
  redis.call('SET',key,'1','PX',ARGV[2*i])
 else redis.call('INCR',key) end
end
return 0
`)
