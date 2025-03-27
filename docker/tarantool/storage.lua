box.watch('box.status', function()
    if box.info.ro then
        return
    end

    -- ====================================
    -- key_value space
    -- ====================================
    box.schema.create_space('key_value', {
        format = {
            { name = 'key', type = 'string' },
            { name = 'bucket_id', type = 'unsigned' },
            { name = 'value', type = 'string' }
        },
        if_not_exists = true
    })

    -- create key_value space indexes
    box.space.key_value:create_index('id', {type = 'tree', parts = { 'key' }, unique = true, if_not_exists = true})
    box.space.key_value:create_index('bucket_id', { type = 'tree', parts = { 'bucket_id' }, unique = false, if_not_exists = true })

    -- ====================================
    -- filer_metadata space
    -- ====================================
    box.schema.create_space('filer_metadata', {
        format = {
            { name = 'directory', type = 'string' },
            { name = 'bucket_id', type = 'unsigned' },
            { name = 'name', type = 'string' },
            { name = 'expire_at', type = 'unsigned' },
            { name = 'data', type = 'string' }
        },
        if_not_exists = true
    })

    -- create filer_metadata space indexes
    box.space.filer_metadata:create_index('id', {type = 'tree', parts = { 'directory', 'name' }, unique = true, if_not_exists = true})
    box.space.filer_metadata:create_index('bucket_id', { type = 'tree', parts = { 'bucket_id' }, unique = false, if_not_exists = true })
    box.space.filer_metadata:create_index('directory_idx', { type = 'tree', parts = { 'directory' }, unique = false, if_not_exists = true })
    box.space.filer_metadata:create_index('name_idx', { type = 'tree', parts = { 'name' }, unique = false, if_not_exists = true })
    box.space.filer_metadata:create_index('expire_at_idx', { type = 'tree', parts = { 'expire_at' }, unique = false, if_not_exists = true})
end)

-- functions for filer_metadata space
local filer_metadata = {
    delete_by_directory_idx = function(directory)
        local space = box.space.filer_metadata
        local index = space.index.directory_idx
        -- for each finded directories
        for _, tuple in index:pairs({ directory }, { iterator = 'EQ' }) do
            space:delete({ tuple[1], tuple[3] })
        end
        return true
    end,
    find_by_directory_idx_and_name = function(dirPath, startFileName, includeStartFile, limit)
        local space = box.space.filer_metadata
        local directory_idx = space.index.directory_idx
        -- choose filter name function
        local filter_filename_func
        if includeStartFile then
            filter_filename_func = function(value) return value >= startFileName end
        else
            filter_filename_func = function(value) return value > startFileName end
        end
        -- init results
        local results = {}
        -- for each finded directories
        for _, tuple in directory_idx:pairs({ dirPath }, { iterator = 'EQ' }) do
            -- filter by name
            if filter_filename_func(tuple[3]) then
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
    is_expired = function(args, tuple)
        return (tuple[4] > 0) and (require('fiber').time() > tuple[4])
    end
}

-- register functions for filer_metadata space, set grants
rawset(_G, 'filer_metadata', filer_metadata)
for name, _ in pairs(filer_metadata) do
    box.schema.func.create('filer_metadata.' .. name, { setuid = true, if_not_exists = true })
    box.schema.user.grant('storage', 'execute', 'function', 'filer_metadata.' .. name, { if_not_exists = true })
end
