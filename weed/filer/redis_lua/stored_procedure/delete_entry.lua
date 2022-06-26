-- KEYS[1]: full path of entry
local fullpath = KEYS[1]
-- KEYS[2]: full path of entry
local fullpath_list_key = KEYS[2]
-- KEYS[3]: dir of the entry
local dir_list_key = KEYS[3]

-- ARGV[1]: isSuperLargeDirectory
local isSuperLargeDirectory = ARGV[1] == "1"
-- ARGV[2]: name of the entry
local name = ARGV[2]

redis.call("DEL", fullpath, fullpath_list_key)

if not isSuperLargeDirectory and name ~= "" then
    redis.call("ZREM", dir_list_key, name)
end

return 0