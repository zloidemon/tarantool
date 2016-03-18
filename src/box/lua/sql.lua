-- sqlite.lua

local sqlite = sqlite

local function execute(self, sql)
    local tuples = self.driver.execute(self.driver, sql)
    return tuples
end

local conn_mt = {
    __index = {
        execute = execute;
    }
}

local function connect(opts)
    opts = opts or ''

    local c = driver.connect(opts)
    return setmetatable({
    	driver = c,
        db = opts,
        raise       = opts.raise
    }, conn_mt)
end

return setmetatable(sqlite, {
	__index = {
		connect = connect
	}
})