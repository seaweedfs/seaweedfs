-- KEYS[1]: full path of entry
local fullpath = KEYS[1]

if fullpath ~= "" and string.sub(fullpath, -1) == "/" then
    fullpath = string.sub(fullpath, 0, -2)
end

local files = redis.call("ZRANGE", fullpath .. "\0", "0", "-1")

for _, name in ipairs(files) do
    local file_path = fullpath .. "/" .. name
    redis.call("DEL", file_path, file_path .. "\0")
end

return 0