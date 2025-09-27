---@meta mysql

--[[
  Lua Language Server annotations + helpers for the mysql X DevAPI Lua bindings.
  Matches the current native API in mysql.core.
  Indices are **1-based** to match Lua conventions.
]]

--------------------------------------------------------------------------------
-- Module entry (callable constructor)
--------------------------------------------------------------------------------

---Create a new Session.
local core = import("mysql.core", "local")

---@class mysql
---@overload fun(mysql_connection_str: string): Session
---@overload fun(host: string, user: string, pass: string, schema: string): Session
---@overload fun(host: string, port: integer, user: string, pass: string, schema: string): Session
local mysql = setmetatable({}, {
    __call = function(self, ...)
        return core(...)
    end
})

---Create a new Session.
---@overload fun(mysql_connection_str: string): Session
---@overload fun(host: string, user: string, pass: string, schema: string): Session
---@overload fun(host: string, port: integer, user: string, pass: string, schema: string): Session
---@return Session
mysql.connect = import("mysql.core")

local function mysql_escape(s)
    assert(type(s) == "string", "mysql_escape expects a string")
    if s:find("[^%w%s%p]") then
        -- Non-printable characters found, escape the whole string as hex.
        local hex = ""
        for i = 1, #s do
            hex = hex .. string.format("%02X", s:byte(i))
        end
        return "X'" .. hex .. "'"
    end
    return "'" .. (s
            :gsub("\\", "\\\\")         -- backslash
            :gsub("%z", "\\0")          -- NUL (0x00)
            :gsub("\n", "\\n")          -- newline
            :gsub("\r", "\\r")          -- carriage return
            :gsub("\t", "\\t")          -- tab
            :gsub("\f", "\\f")          -- form feed
            :gsub("\b", "\\b")          -- backspace
            :gsub(string.char(26), "\\Z") -- Ctrl+Z (0x1A)
            :gsub("'", "\\'")           -- single quote
            :gsub('"', '\\"'))          -- double quote
        .. "'"
end
mysql.escape = mysql_escape
local function sql_value(v)
    if v == nil then return "NULL" end
    local t = type(v)
    if t == "number" then
        assert(v == v and v ~= math.huge and v ~= -math.huge, "invalid number")
        return tostring(v)
    elseif t == "boolean" then
        return v and "1" or "0"
    elseif t == "string" then
        return mysql_escape(v)
    elseif t == "cdata" then
        return "0x" .. string.format("%X", v)
    elseif t == "table" then
        local hex = ""
        local s, e = pcall(function()
            for i = 1, #v do
                hex = hex .. string.format("%02X", v[i] or 0)
            end
        end)
        if not s then
            for i = 1, #v do
                print(i, type(v[i]), v[i])
            end
            error(e)
        end
        return "X'" .. hex .. "'"
    else
        error("unsupported parameter type: " .. t)
    end
end
mysql.sql_value = sql_value
local function bind_placeholders(sql, params)
    local out, i, n = {}, 1, #sql
    local state = "code" -- code | squote | dquote | bquote | line | block
    local pi, arrN = 1, #params
    local function is_ident_start(ch) return ch:match("[A-Za-z_]") ~= nil end
    local function is_ident_cont(ch)  return ch:match("[A-Za-z0-9_]") ~= nil end
    while i <= n do
        local c  = sql:sub(i,i)
        local c2 = sql:sub(i,i+1)
        if state == "code" then
            if c == "'" then state = "squote"; out[#out+1] = c; i = i + 1
            elseif c == '"' then state = "dquote"; out[#out+1] = c; i = i + 1
            elseif c == "`" then state = "bquote"; out[#out+1] = c; i = i + 1
            elseif c2 == "--" then state = "line";  out[#out+1] = c2; i = i + 2
            elseif c == "#" then state = "line"; out[#out+1] = c; i = i + 1
            elseif c2 == "/*" then state = "block"; out[#out+1] = c2; i = i + 2
            elseif c == "?" then
                if pi > arrN then error("not enough positional parameters for '?' placeholders") end
                out[#out+1] = sql_value(params[pi]); pi = pi + 1; i = i + 1
            elseif c == ":" then
                -- Optional: keep :: intact (useful if you ever port to a dialect that uses it)
                if i < n and sql:sub(i+1,i+1) == ":" then
                    out[#out+1] = "::"; i = i + 2
                else
                    local name, j
                    if i < n and sql:sub(i+1,i+1) == "{" then
                        -- :{name with spaces}
                        j = i + 2
                        local close = sql:find("}", j, true)
                        assert(close, "unterminated :{...} placeholder")
                        name = sql:sub(j, close - 1)
                        i = close + 1
                    else
                        -- :name
                        j = i + 1
                        if j <= n and sql:sub(j,j):match("[A-Za-z_]") then
                            j = j + 1
                            while j <= n and sql:sub(j,j):match("[A-Za-z0-9_]") do j = j + 1 end
                            name = sql:sub(i + 1, j - 1)
                            i = j
                        else
                            -- Lone ':' not followed by an identifier → literal
                            out[#out+1] = ":"; i = i + 1; continue
                        end
                    end
                    local val = params[name]
                    assert(val ~= nil, ("missing value for :%s"):format(name))
                    out[#out+1] = sql_value(val)
                end
            else
                out[#out+1] = c; i = i + 1
            end
        elseif state == "squote" then
            if c == "\\" then
                out[#out+1] = c
                if i < n then out[#out+1] = sql:sub(i+1,i+1); i = i + 2 else i = i + 1 end
            elseif c == "'" then
                if i < n and sql:sub(i+1,i+1) == "'" then
                    out[#out+1] = c; out[#out+1] = c; i = i + 2
                else
                    out[#out+1] = c; i = i + 1; state = "code"
                end
            else
                out[#out+1] = c; i = i + 1
            end
        elseif state == "dquote" then
            if c == "\\" then
                out[#out+1] = c
                if i < n then out[#out+1] = sql:sub(i+1,i+1); i = i + 2 else i = i + 1 end
            elseif c == '"' then
                if i < n and sql:sub(i+1,i+1) == '"' then
                    out[#out+1] = c; out[#out+1] = c; i = i + 2
                else
                    out[#out+1] = c; i = i + 1; state = "code"
                end
            else
                out[#out+1] = c; i = i + 1
            end
        elseif state == "bquote" then
            if c == "`" then
                if i < n and sql:sub(i+1,i+1) == "`" then
                    out[#out+1] = c; out[#out+1] = c; i = i + 2
                else
                    out[#out+1] = c; i = i + 1; state = "code"
                end
            else
                out[#out+1] = c; i = i + 1
            end
        elseif state == "line" then
            out[#out+1] = c
            if c == "\n" then state = "code" end
            i = i + 1
        elseif state == "block" then
            if c2 == "*/" then out[#out+1] = c2; i = i + 2; state = "code"
            else out[#out+1] = c; i = i + 1 end
        end
        ::next::
    end
    if pi <= arrN then
        error(("too many positional parameters: expected %d, got %d"):format(pi - 1, arrN), 3)
    end
    return table.concat(out)
end
local function minify_sql(sql, opts)
    opts = opts or {}
    local remove_comments = opts.remove_comments ~= false        -- default: true
    local keep_version_comments = opts.keep_version_comments ~= false -- default: true
    local out = {}
    local i, n = 1, #sql
    local state = "code" -- code | squote | dquote | bquote | line | block
    local pending_space = false
    local function emit(ch)
        out[#out+1] = ch
    end
    -- Helper to finalize a whitespace run: maybe emit one space.
    local function flush_space(next_char)
        if not pending_space then return end
        -- Avoid space before , ) ; and after ( or , already emitted.
        local prev = out[#out]
        if next_char == "," or next_char == ")" or next_char == ";" then
            -- skip
        elseif prev == "(" or prev == "," then
            -- skip
        else
            emit(" ")
        end
        pending_space = false
    end
    while i <= n do
        local c  = sql:sub(i,i)
        local c2 = sql:sub(i,i+1)
        if state == "code" then
            -- whitespace run
            if c == " " or c == "\t" or c == "\n" or c == "\r" or c == "\f" then
                pending_space = true
                i = i + 1
            -- comments
            elseif c2 == "--" then
                if remove_comments then
                    i = i + 2
                    while i <= n and sql:sub(i,i) ~= "\n" do i = i + 1 end
                    pending_space = true
                else
                    flush_space("-"); emit("-"); emit("-"); i = i + 2; state = "line"
                end
            elseif c == "#" then
                if remove_comments then
                    i = i + 1
                    while i <= n and sql:sub(i,i) ~= "\n" do i = i + 1 end
                    pending_space = true
                else
                    flush_space("#"); emit("#"); i = i + 1; state = "line"
                end
            elseif c2 == "/*" then
                local is_version = (i+2 <= n) and sql:sub(i,i+2) == "/*!"
                if remove_comments and (not is_version or not keep_version_comments) then
                    i = i + 2
                    while i <= n-1 and sql:sub(i,i+1) ~= "*/" do i = i + 1 end
                    i = math.min(n+1, i + 2)
                    pending_space = true
                else
                    flush_space("/") ; emit("/"); emit("*"); i = i + 2; state = "block"
                end
            -- strings / identifiers
            elseif c == "'" then flush_space("'"); emit(c); i = i + 1; state = "squote"
            elseif c == '"' then flush_space('"'); emit(c); i = i + 1; state = "dquote"
            elseif c == "`" then flush_space("`"); emit(c); i = i + 1; state = "bquote"
            -- punctuation: tighten around them
            elseif c == "(" then flush_space("("); emit(c); i = i + 1
            elseif c == "," then
                -- remove any pending space BEFORE comma
                if out[#out] == " " then out[#out] = nil end
                emit(c); i = i + 1; pending_space = true -- allow one space AFTER comma
            elseif c == ")" or c == ";" then
                if out[#out] == " " then out[#out] = nil end
                emit(c); i = i + 1
            else
                flush_space(c); emit(c); i = i + 1
            end
        elseif state == "squote" then
            if c == "\\" then emit(c); if i < n then emit(sql:sub(i+1,i+1)); i = i + 2 else i = i + 1 end
            elseif c == "'" then
                if i < n and sql:sub(i+1,i+1) == "'" then emit("'"); emit("'"); i = i + 2
                else emit("'"); i = i + 1; state = "code" end
            else emit(c); i = i + 1 end
        elseif state == "dquote" then
            if c == "\\" then emit(c); if i < n then emit(sql:sub(i+1,i+1)); i = i + 2 else i = i + 1 end
            elseif c == '"' then
                if i < n and sql:sub(i+1,i+1) == '"' then emit('"'); emit('"'); i = i + 2
                else emit('"'); i = i + 1; state = "code" end
            else emit(c); i = i + 1 end
        elseif state == "bquote" then
            if c == "`" then
                if i < n and sql:sub(i+1,i+1) == "`" then emit("`"); emit("`"); i = i + 2
                else emit("`"); i = i + 1; state = "code" end
            else emit(c); i = i + 1 end
        elseif state == "line" then
            emit(c); if c == "\n" then state = "code" end; i = i + 1
        elseif state == "block" then
            emit(c)
            if c2 == "*/" then emit("*"); emit("/"); i = i + 2; state = "code"
            else i = i + 1 end
        end
    end
    -- Trim leading/trailing space if any
    while out[1] == " " do table.remove(out, 1) end
    while out[#out] == " " do out[#out] = nil end
    return table.concat(out)
end
mysql.minify_sql = minify_sql
local function prepare_statement(sql, params, minify)
    assert(type(sql) == "string", "sql must be a string")
    assert(type(params) == "table", "params must be an array-like table")
    if minify then
        return minify_sql(bind_placeholders(sql, params))
    else
        return bind_placeholders(sql, params)
    end
end
mysql.prepare_statement = prepare_statement

--------------------------------------------------------------------------------
-- Common types
--------------------------------------------------------------------------------

---@class Warning
---@field code integer
---@field message string
---@field level "error"|"warning"|"info"

---@class Result : userdata
---@field affected integer               # same as getAffectedItemsCount()
---@field warnings Warning[]
local Result = {}
function Result:getAffectedItemsCount() end

---@class IncrementResult : Result
---@field auto_id? integer               # same as getAutoIncrementValue()
local IncrementResult = {}
function IncrementResult:getAutoIncrementValue() end

---@class RowResult : Result, table<integer, Row>
---@operator len: integer
---@field count integer                  # number of rows (also available via length operator)
local RowResult = {}
function RowResult:fetchOne() end       ---@return Row
function RowResult:fetchAll() end       ---@return Row[]
---@type Row
RowResult.one = nil
---@type Row[]
RowResult.all = nil

---@class IncrementRowResult : RowResult
---@field auto_id? integer               # same as getAutoIncrementValue()
local IncrementRowResult = {}
function IncrementRowResult:getAutoIncrementValue() end

---@class SqlResult : IncrementRowResult
local SqlResult = setmetatable({}, { __index = RowResult })
function SqlResult:hasData() end        ---@return boolean
function SqlResult:nextResult() end     ---@return boolean

---@class DocResult : RowResult
---@field count integer
local DocResult = {}
function DocResult:fetchOne() end       ---@return DbDoc
function DocResult:fetchAll() end       ---@return DbDoc[]
---@type DbDoc
DocResult.one = nil
---@type DbDoc[]
DocResult.all = nil

---@alias MySQLValue nil|boolean|number|string|DbDoc|any[]

--------------------------------------------------------------------------------
-- Session
--------------------------------------------------------------------------------

---@class Session : userdata
local Session = {}

---Execute a raw SQL statement and return all rows.
---@param query string
---@return Row[]
function Session:exec(query) end

---Create a prepared SQL statement object.
---@param query string
---@return SqlStatement
function Session:sql(query) end

---Start a transaction.
function Session:startTransaction() end

---Commit current transaction.
function Session:commit() end

---Rollback current transaction.
function Session:rollback() end

---Create a named savepoint and return its (server) name.
---@param name string
---@return string
function Session:setSavepoint(name) end

---Get a schema by name.
---@param name string
---@return Schema
function Session:getSchema(name) end

---Get the default schema for this session.
---@return Schema
function Session:getDefaultSchema() end

---Create a schema by name (no-op if it exists) and return it.
---@param name string
---@return Schema
function Session:createSchema(name) end

---Drop a schema by name.
---@param name string
function Session:dropSchema(name) end

--------------------------------------------------------------------------------
-- SqlStatement
--------------------------------------------------------------------------------

---@class SqlStatement : userdata
local SqlStatement = {}

---Bind positional parameters.
---@param ... MySQLValue
---@return SqlStatement
function SqlStatement:bind(...) end

---Execute the statement and return all rows.
---@return SqlResult
function SqlStatement:execute() end

--------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------

---@class Schema : userdata
---@field name string
local Schema = {}

---Whether this schema exists in the database.
---@return boolean
function Schema:existsInDatabase() end

---Get a table within this schema.
---@param name string
---@return Table
function Schema:getTable(name) end

--------------------------------------------------------------------------------
-- Table
--------------------------------------------------------------------------------

---@class Table : userdata
---@field name string
local Table = {}

---Build a SELECT operation.
---@param ... string  # column list (default "*")
---@return TableSelect
function Table:select(...) end

---Build an INSERT operation.
---@param ... string  # column list (optional)
---@return TableInsert
function Table:insert(...) end

---Build an UPDATE operation.
---@return TableUpdate
function Table:update() end

---Build a DELETE/REMOVE operation.
---@return TableRemove
function Table:remove() end

--------------------------------------------------------------------------------
-- TableSelect
--------------------------------------------------------------------------------

---@class TableSelect : userdata
local TableSelect = {}

---@param expr string
---@return TableSelect
function TableSelect:where(expr) end

---@param ... string
---@return TableSelect
function TableSelect:groupBy(...) end

---@param expr string
---@return TableSelect
function TableSelect:having(expr) end

---@param ... string
---@return TableSelect
function TableSelect:orderBy(...) end

---@param n integer
---@return TableSelect
function TableSelect:limit(n) end

---@param n integer
---@return TableSelect
function TableSelect:offset(n) end

---Bind positional parameters.
---@param name string
---@param value MySQLValue
---@return TableSelect
function TableSelect:bind(name, value) end

---@return RowResult
function TableSelect:execute() end

--------------------------------------------------------------------------------
-- TableInsert
--------------------------------------------------------------------------------

---@class TableInsert : userdata
local TableInsert = {}

---Provide a row of values (repeatable for multiple rows).
---@param ... MySQLValue
---@return TableInsert
function TableInsert:values(...) end

---@return IncrementResult
function TableInsert:execute() end

--------------------------------------------------------------------------------
-- TableUpdate
--------------------------------------------------------------------------------

---@class TableUpdate : userdata
local TableUpdate = {}

---@param field string
---@param expr string
---@return TableUpdate
function TableUpdate:set(field, expr) end

---@param expr string
---@return TableUpdate
function TableUpdate:where(expr) end

---@param ... string
---@return TableUpdate
function TableUpdate:orderBy(...) end

---@param n integer
---@return TableUpdate
function TableUpdate:limit(n) end

---Bind named parameters as name,value pairs: bind("age", 30, "id", 1)
---@param name string
---@param value MySQLValue
---@return TableUpdate
function TableUpdate:bind(name, value) end

---@return IncrementResult
function TableUpdate:execute() end

--------------------------------------------------------------------------------
-- TableRemove
--------------------------------------------------------------------------------

---@class TableRemove : userdata
local TableRemove = {}

---@param expr string
---@return TableRemove
function TableRemove:where(expr) end

---@param ... string
---@return TableRemove
function TableRemove:orderBy(...) end

---@param n integer
---@return TableRemove
function TableRemove:limit(n) end

---Bind positional parameters.
---@param name string
---@param value MySQLValue
---@return TableRemove
function TableRemove:bind(name, value) end

---@return IncrementResult
function TableRemove:execute() end

--------------------------------------------------------------------------------
-- Row & DbDoc
--------------------------------------------------------------------------------

---@class Row
---@operator len: integer
---@field colCount integer
local Row = {}

---@param index integer  # 1-based
---@return MySQLValue
function Row:get(index) end

---@param index integer
---@return integer  # underlying mysqlx::Value::Type enum value
function Row:type(index) end

---@param index integer
---@return integer  # element count if value is ARRAY/DOCUMENT
function Row:elementCount(index) end

---@param index integer
---@param subindex integer
---@return MySQLValue
function Row:at(index, subindex) end

---@class DbDoc : userdata
local DbDoc = {}

---@param key string
---@return MySQLValue
function DbDoc:get(key) end

return mysql