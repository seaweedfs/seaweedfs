-- KEYS[1]: full path of entry
local full_path = KEYS[1]
-- KEYS[2]: dir of the entry
local dir_list_key = KEYS[2]

-- ARGV[1]: content of the entry
local entry = ARGV[1]
-- ARGV[2]: TTL of the entry
local ttlSec = tonumber(ARGV[2])
-- ARGV[3]: isSuperLargeDirectory
local isSuperLargeDirectory = ARGV[3] == "1"
-- ARGV[4]: zscore of the entry in zset
local zscore = tonumber(ARGV[4])
-- ARGV[5]: name of the entry
local name = ARGV[5]

if ttlSec > 0 then
    redis.call("SET", full_path, entry, "EX", ttlSec)
else
    redis.call("SET", full_path, entry)
end

if not isSuperLargeDirectory and name ~= "" then
    redis.call("ZADD", dir_list_key, "NX", zscore, name)
end

return 0