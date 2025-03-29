local vshard = require('vshard')
local log = require('log')

-- Bootstrap the vshard router.
while true do
    local ok, err = vshard.router.bootstrap({
        if_not_bootstrapped = true,
    })
    if ok then
        break
    end
    log.info(('Router bootstrap error: %s'):format(err))
end

-- functions for filer_metadata space
local filer_metadata = {
    delete_by_directory_idx = function(directory)
        -- find all storages
        local storages = require('vshard').router.routeall()
        -- on each storage
        for _, storage in pairs(storages) do
            -- call local function
            local result, err = storage:callrw('filer_metadata.delete_by_directory_idx', { directory })
            -- check for error
            if err then
                error("Failed to call function on storage: " .. tostring(err))
            end
        end
        -- return
        return true
    end,
    find_by_directory_idx_and_name = function(dirPath, startFileName, includeStartFile, limit)
        -- init results
        local results = {}
        -- find all storages
        local storages = require('vshard').router.routeall()
        -- on each storage
        for _, storage in pairs(storages) do
            -- call local function
            local result, err = storage:callro('filer_metadata.find_by_directory_idx_and_name', {
                dirPath,
                startFileName,
                includeStartFile,
                limit
            })
            -- check for error
            if err then
                error("Failed to call function on storage: " .. tostring(err))
            end
            -- add to results
            for _, tuple in ipairs(result) do
                table.insert(results, tuple)
            end
        end
        -- sort
        table.sort(results, function(a, b) return a[3] < b[3] end)
        -- apply limit
        if #results > limit then
            local limitedResults = {}
            for i = 1, limit do
                table.insert(limitedResults, results[i])
            end
            results = limitedResults
        end
        -- return
        return results
    end,
}

rawset(_G, 'filer_metadata', filer_metadata)

-- register functions for filer_metadata space, set grants
for name, _ in pairs(filer_metadata) do
    box.schema.func.create('filer_metadata.' .. name, { if_not_exists = true })
    box.schema.user.grant('app', 'execute', 'function', 'filer_metadata.' .. name, { if_not_exists = true })
    box.schema.user.grant('client', 'execute', 'function', 'filer_metadata.' .. name, { if_not_exists = true })
end
