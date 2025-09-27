#include <luaxe/luaxe.hpp>
#include "mysqlx/xdevapi.h"

// -----------------------------
// Helpers: Value <-> Lua
// -----------------------------
int lua_push_mysql_value(lua_State* L, const mysqlx::Value& v) {
    try {
        switch (v.getType()) {
            case mysqlx::Value::Type::VNULL:
                return 0;
            case mysqlx::Value::Type::STRING: {
                auto str = v.get<std::string>();
                lua_pushlstring(L, str.c_str(), str.length());
            }   return 1;
            case mysqlx::Value::Type::UINT64: {
                char push_buf[255] = {0};
                snprintf(push_buf, sizeof(push_buf), "return %lluULL", v.get<std::uint64_t>());
                if (luaL_dostring(L, push_buf) != LUA_OK) {
                    luaL_error(L, "Failed to push uint64: %s", lua_tostring(L, -1));
                }
            }   return 1;
            case mysqlx::Value::Type::INT64: {
                char push_buf[255] = {0};
                snprintf(push_buf, sizeof(push_buf), "return %lldLL", v.get<std::int64_t>());
                if (luaL_dostring(L, push_buf) != LUA_OK) {
                    luaL_error(L, "Failed to push int64: %s", lua_tostring(L, -1));
                }
            }   return 1;
            case mysqlx::Value::Type::FLOAT:
                lua_pushnumber(L, v.get<float>());
                return 1;
            case mysqlx::Value::Type::DOUBLE:
                lua_pushnumber(L, v.get<double>());
                return 1;
            case mysqlx::Value::Type::BOOL:
                lua_pushboolean(L, v.get<bool>());
                return 1;
            case mysqlx::abi2::r0::Value::RAW: {
                auto &&bytes = v.getRawBytes();
                lua_pushlstring(L, reinterpret_cast<const char *>(bytes.first), bytes.second);
                return 1;
            }
            case mysqlx::abi2::r0::Value::ARRAY: {
                auto&& count = v.elementCount();
                lua_newtable(L);
                for (size_t i = 0; i < count; ++i) {
                    auto&& elem = v[(int)i];
                    lua_push_mysql_value(L, elem);
                    lua_rawseti(L, -2, i + 1);
                }
                return 1;
            }
            case mysqlx::abi2::r0::Value::DOCUMENT: {
                new (lua::alloc<mysqlx::DbDoc>(L)) mysqlx::DbDoc(v.get<mysqlx::DbDoc>());
                return 1;
            }
        }
    } catch (const std::exception& e) {
        luaL_error(L, e.what());
    }
    return 0;
}

static mysqlx::Value lua_to_mysql_value(lua_State* L, int idx) {
    switch (lua_type(L, idx)) {
        case LUA_TNIL: return mysqlx::Value();
        case LUA_TBOOLEAN: return mysqlx::Value((bool)lua_toboolean(L, idx));
        case LUA_TNUMBER: {
            // prefer integer if exactly integral
            lua_Number num = lua_tonumber(L, idx);
            lua_Integer i = lua_tointeger(L, idx);
            if ((lua_Number)i == num) return mysqlx::Value((std::int64_t)i);
            return mysqlx::Value((double)num);
        }
        case LUA_TSTRING: {
            size_t len; const char* s = lua_tolstring(L, idx, &len);
            return mysqlx::Value(std::string(s, len));
        }
        case LUA_TUSERDATA: {
            if (luaL_testudata(L, idx, "DbDoc")) {
                auto* d = lua::check<mysqlx::DbDoc>(L, idx);
                return mysqlx::Value(*d);
            }
            luaL_error(L, "Unsupported userdata for bind value");
            break;
        }
        case 10: { // cdata, probably (u)int64 from LuaJIT
            auto abs_idx = lua_absindex(L, idx);
            lua_getglobal(L, "tostring");
            lua_pushvalue(L, abs_idx);
            lua_call(L, 1, 1);
            std::string_view val = lua_tostring(L, -1);
            lua_pop(L, 1);
            if (val.ends_with("ULL")) {
                val.remove_suffix(3);
                return mysqlx::Value((std::uint64_t)std::stoull(std::string(val)));
            } else if (val.ends_with("LL")) {
                val.remove_suffix(2);
                return mysqlx::Value((std::int64_t)std::stoll(std::string(val)));
            } else {
                luaL_error(L, "Unsupported cdata for bind value: %s", val.data());
            }
        }
    }
    luaL_error(L, "Unsupported Lua type (%d) for bind value", lua_type(L, idx));
    return mysqlx::Value(); // unreachable
}

// -----------------------------
// Helpers: push results
// -----------------------------
static int push_row_table(lua_State* L, const std::vector<mysqlx::Row>& rows) {
    lua_newtable(L);
    for (const auto& row : rows) {
        new (lua::alloc<mysqlx::Row>(L)) mysqlx::Row(row);
        lua_rawseti(L, -2, (int)lua_objlen(L, -2) + 1);
    }
    return 1;
}

static int push_rowresult(lua_State* L, mysqlx::RowResult& rr) {
    auto rows = rr.fetchAll();
    return push_row_table(L, rows);
}

static int push_docresult(lua_State* L, mysqlx::DocResult& dr) {
    auto docs = dr.fetchAll();
    lua_newtable(L);
    for (const auto& d : docs) {
        new (lua::alloc<mysqlx::DbDoc>(L)) mysqlx::DbDoc(d);
        lua_rawseti(L, -2, (int)lua_objlen(L, -2) + 1);
    }
    return 1;
}

static int push_result_info(lua_State* L, mysqlx::Result& r) {
    lua_newtable(L);
    lua_pushinteger(L, (lua_Integer)r.getAffectedItemsCount());
    lua_setfield(L, -2, "affected");
    try {
        std::uint64_t id = r.getAutoIncrementValue();
        lua_pushinteger(L, (lua_Integer)id);
        lua_setfield(L, -2, "auto_id");
    } catch (...) {
    // no auto id
    }
    // warnings
    unsigned wc = r.getWarningsCount();
    lua_newtable(L);
    for (unsigned i = 0; i < wc; ++i) {
        auto w = r.getWarning(i);
        lua_newtable(L);
        lua_pushinteger(L, (lua_Integer)w.getCode());
        lua_setfield(L, -2, "code");
        std::string msg = w.getMessage();
        lua_pushlstring(L, msg.c_str(), msg.size());
        lua_setfield(L, -2, "message");
        auto lvl = w.getLevel();
        lua_pushinteger(L, lvl);
        lua_setfield(L, -2, "level");
        lua_rawseti(L, -2, (int)i + 1);
    }
    lua_setfield(L, -2, "warnings");
    return 1;
}

extern "C" int luaopen_mysql_core(lua_State *L)
{
    using namespace mysqlx;

    lua::bind::add<Session>(L, "Session")
            .fun("sql", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try {
                    const char* query = luaL_checkstring(L, 2);
                    SqlResult r = s->sql(query).execute();
                    if (r.count() == 0) { lua_newtable(L); return 1; }
                    auto rows = r.fetchAll();
                    return push_row_table(L, rows);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("statement", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try {
                    const char* query = luaL_checkstring(L, 2);
                    new (lua::alloc<SqlStatement>(L)) SqlStatement(s->sql(query));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("setSavepoint", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try {
                    const char* nm = luaL_checkstring(L, 2);
                    std::string ret = (std::string)s->setSavepoint(nm);
                    lua_pushlstring(L, ret.c_str(), ret.size());
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("startTransaction", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try { s->startTransaction(); return 0; } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("commit", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try { s->commit(); return 0; } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("rollback", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try { s->rollback(); return 0; } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("getSchema", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try {
                    const char* nm = luaL_checkstring(L, 2);
                    new (lua::alloc<Schema>(L)) Schema(s->getSchema(nm));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("getDefaultSchema", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try { new (lua::alloc<Schema>(L)) Schema(s->getDefaultSchema()); return 1; } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("createSchema", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try {
                    const char* nm = luaL_checkstring(L, 2);
                    new (lua::alloc<Schema>(L)) Schema(s->createSchema(nm));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("dropSchema", [](lua_State* L) -> int {
                auto s = lua::check<Session>(L, 1);
                try {
                    const char* nm = luaL_checkstring(L, 2);
                    s->dropSchema(nm);
                    return 0;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<Row>(L, "Row")
            .fun("get", [](lua_State *L) -> int {
                auto r = lua::check<Row>(L, 1);
                int index = (int)luaL_checkinteger(L, 2) - 1; // 1-based in Lua
                return lua_push_mysql_value(L, (*r)[index]);
            })
            .meta_fun("__cindex", [](lua_State *L) -> int {
                auto r = lua::check<Row>(L, 1);
                if (lua_isnumber(L, 2)) {
                    int index = (int)luaL_checkinteger(L, 2) - 1;
                    return lua_push_mysql_value(L, (*r)[index]);
                }
                return 0;
            })
            .fun("type", [](lua_State *L) -> int {
                auto r = lua::check<Row>(L, 1);
                int index = (int)luaL_checkinteger(L, 2) - 1;
                try {
                    auto&& v = (*r)[index];
                    lua_pushinteger(L, (lua_Integer)v.getType());
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("elementCount", [](lua_State *L) -> int {
                auto r = lua::check<Row>(L, 1);
                int index = (int)luaL_checkinteger(L, 2) - 1;
                try {
                    auto&& v = (*r)[index];
                    lua_pushinteger(L, (lua_Integer)v.elementCount());
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("at", [](lua_State *L) -> int {
                auto r = lua::check<Row>(L, 1);
                int index = (int)luaL_checkinteger(L, 2) - 1;
                int subindex = (int)luaL_checkinteger(L, 3) - 1;
                try {
                    return lua_push_mysql_value(L, (*r)[index][subindex]);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .prop("colCount", [](lua_State *L) -> int {
                auto r = lua::check<Row>(L, 1);
                try {
                    lua_pushinteger(L, (lua_Integer)r->colCount());
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<DbDoc>(L, "DbDoc")
            .fun("get", [](lua_State *L) -> int {
                auto d = lua::check<DbDoc>(L, 1);
                const char* key = luaL_checkstring(L, 2);
                try {
                    return lua_push_mysql_value(L, (*d)[key]);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .meta_fun("__cindex", [](lua_State *L) -> int {
                auto d = lua::check<DbDoc>(L, 1);
                if (lua_isstring(L, 2)) {
                    const char* key = luaL_checkstring(L, 2);
                    try {
                        return lua_push_mysql_value(L, (*d)[key]);
                    } catch (const std::exception& e) { luaL_error(L, e.what()); }
                }
                return 0;
            })
            ;

    lua::bind::add<SqlStatement>(L, "SqlStatement")
            .fun("bind", [](lua_State* L) -> int {
                auto stmt = lua::check<SqlStatement>(L, 1);
                try {
                    int top = lua_gettop(L);
                    for (int i = 2; i <= top; ++i) {
                        mysqlx::Value v = lua_to_mysql_value(L, i);
                        stmt->bind(v);
                    }
                    lua_settop(L, 1);
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("execute", [](lua_State* L) -> int {
                auto stmt = lua::check<SqlStatement>(L, 1);
                try {
                    SqlResult r = stmt->execute();
                    if (r.count() == 0) { lua_newtable(L); return 1; }
                    auto rows = r.fetchAll();
                    return push_row_table(L, rows);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<Schema>(L, "Schema")
            .prop("name", [](lua_State* L) -> int {
                auto sc = lua::check<Schema>(L, 1);
                std::string n = sc->getName();
                lua_pushlstring(L, n.c_str(), n.size());
                return 1;
            })
            .fun("existsInDatabase", [](lua_State* L) -> int {
                auto sc = lua::check<Schema>(L, 1);
                try {
                    lua_pushboolean(L, sc->existsInDatabase());
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("getTable", [](lua_State* L) -> int {
                auto sc = lua::check<Schema>(L, 1);
                const char* name = luaL_checkstring(L, 2);
                try {
                    new (lua::alloc<Table>(L)) Table(sc->getTable(name));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<Table>(L, "Table")
            .prop("name", [](lua_State* L) -> int {
                auto t = lua::check<Table>(L, 1);
                try {
                    std::string n = t->getName();
                    lua_pushlstring(L, n.c_str(), n.size());
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("select", [](lua_State* L) -> int {
                auto t = lua::check<Table>(L, 1);
                int top = lua_gettop(L);
                std::vector<std::string> cols;
                for (int i = 2; i <= top; ++i) cols.emplace_back(luaL_checkstring(L, i));
                try {
                    TableSelect sel = cols.empty() ? t->select("*") : t->select(cols.begin(), cols.end());
                    new (lua::alloc<TableSelect>(L)) TableSelect(std::move(sel));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("insert", [](lua_State* L) -> int {
                auto t = lua::check<Table>(L, 1);
                int top = lua_gettop(L);
                std::vector<std::string> cols;
                for (int i = 2; i <= top; ++i) cols.emplace_back(luaL_checkstring(L, i));
                try {
                    TableInsert ins = cols.empty() ? t->insert() : t->insert(cols.begin(), cols.end());
                    new (lua::alloc<TableInsert>(L)) TableInsert(std::move(ins));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("update", [](lua_State* L) -> int {
                auto t = lua::check<Table>(L, 1);
                try {
                    TableUpdate up = t->update();
                    new (lua::alloc<TableUpdate>(L)) TableUpdate(std::move(up));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            .fun("remove", [](lua_State* L) -> int {
                auto t = lua::check<Table>(L, 1);
                try {
                    TableRemove del = t->remove();
                    new (lua::alloc<TableRemove>(L)) TableRemove(std::move(del));
                    return 1;
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<TableSelect>(L, "TableSelect")
            .fun("where", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                const char* e = luaL_checkstring(L, 2);
                try {
                    s->where(e);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1);
                return 1;
            })
            .fun("groupBy", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                int top = lua_gettop(L); std::vector<std::string> v; for (int i=2;i<=top;++i) v.emplace_back(luaL_checkstring(L,i));
                s->groupBy(v.begin(), v.end()); lua_settop(L,1); return 1;
            })
            .fun("having", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                const char* e = luaL_checkstring(L, 2);
                try {
                    s->having(e);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("orderBy", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                int top = lua_gettop(L);
                std::vector<std::string> v;
                for (int i=2;i<=top;++i) v.emplace_back(luaL_checkstring(L,i));
                try {
                    s->orderBy(v.begin(), v.end());
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("limit", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                std::uint64_t n = (std::uint64_t)luaL_checkinteger(L, 2);
                try {
                    s->limit(n);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("offset", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                std::uint64_t n = (std::uint64_t)luaL_checkinteger(L, 2);
                try {
                    s->offset(n);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("bind", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                auto str = luaL_checkstring(L, 2);
                auto v = lua_to_mysql_value(L, 3);
                try {
                    s->bind(str, v);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("execute", [](lua_State* L) -> int {
                auto s = lua::check<TableSelect>(L, 1);
                try {
                    RowResult rr = s->execute();
                    return push_rowresult(L, rr);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<TableInsert>(L, "TableInsert")
            .fun("values", [](lua_State* L) -> int {
                auto ins = lua::check<TableInsert>(L, 1);
                int top = lua_gettop(L);
                std::vector<mysqlx::Value> vals; vals.reserve(top-1);
                for (int i=2;i<=top;++i) vals.emplace_back(lua_to_mysql_value(L,i));
                ins->values(vals.begin(), vals.end());
                lua_settop(L,1); return 1;
            })
            .fun("execute", [](lua_State* L) -> int {
                auto ins = lua::check<TableInsert>(L, 1);
                try {
                    Result r = ins->execute();
                    return push_result_info(L, r);
                }
                catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<TableUpdate>(L, "TableUpdate")
            .fun("set", [](lua_State* L) -> int {
                auto up = lua::check<TableUpdate>(L, 1);
                const char* f = luaL_checkstring(L,2); const char* e = luaL_checkstring(L,3); up->set(f, e);
                lua_settop(L,1); return 1;
            })
            .fun("where", [](lua_State* L) -> int {
                auto up = lua::check<TableUpdate>(L, 1);
                const char* e = luaL_checkstring(L,2); up->where(e);
                lua_settop(L,1); return 1;
            })
            .fun("orderBy", [](lua_State* L) -> int {
                auto up = lua::check<TableUpdate>(L, 1);
                int top = lua_gettop(L);
                std::vector<std::string> v;
                for (int i=2;i<=top;++i) v.emplace_back(luaL_checkstring(L,i));
                try {
                    up->orderBy(v.begin(), v.end());
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("limit", [](lua_State* L) -> int {
                auto up = lua::check<TableUpdate>(L, 1);
                std::uint64_t n = (std::uint64_t)luaL_checkinteger(L,2);
                try {
                    up->limit(n);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("bind", [](lua_State* L) -> int {
                auto up = lua::check<TableUpdate>(L, 1);
                auto str = luaL_checkstring(L, 2);
                auto v = lua_to_mysql_value(L, 3);
                try {
                    up->bind(str, v);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("execute", [](lua_State* L) -> int {
                auto up = lua::check<TableUpdate>(L, 1);
                try {
                    Result r = up->execute();
                    return push_result_info(L, r);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua::bind::add<TableRemove>(L, "TableDelete")
            .fun("where", [](lua_State* L) -> int {
                auto del = lua::check<TableRemove>(L, 1);
                const char* e = luaL_checkstring(L,2);
                try {
                    del->where(e);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("orderBy", [](lua_State* L) -> int {
                auto del = lua::check<TableRemove>(L, 1);
                int top = lua_gettop(L); std::vector<std::string> v;
                for (int i=2;i<=top;++i) v.emplace_back(luaL_checkstring(L,i));
                try {
                    del->orderBy(v.begin(), v.end());
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("limit", [](lua_State* L) -> int {
                auto del = lua::check<TableRemove>(L, 1);
                std::uint64_t n = (std::uint64_t)luaL_checkinteger(L,2);
                try {
                    del->limit(n);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("bind", [](lua_State* L) -> int {
                auto del = lua::check<TableRemove>(L, 1);
                auto str = luaL_checkstring(L, 2);
                auto v = lua_to_mysql_value(L, 3);
                try {
                    del->bind(str, v);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                lua_settop(L,1); return 1;
            })
            .fun("execute", [](lua_State* L) -> int {
                auto del = lua::check<TableRemove>(L, 1);
                try {
                    Result r = del->execute();
                    return push_result_info(L, r);
                } catch (const std::exception& e) { luaL_error(L, e.what()); }
                return 0;
            })
            ;

    lua_pushcfunction(L, +[](lua_State* L) {
        try {
            auto argn = lua_gettop(L);
            if (argn == 1)
            {
                new (lua::alloc<mysqlx::Session>(L)) mysqlx::Session(lua_tostring(L, 1));
            }
            else if (argn == 4)
            {
                new (lua::alloc<mysqlx::Session>(L)) mysqlx::Session(lua_tostring(L, 1), lua_tostring(L, 2), lua_tostring(L, 3), lua_tostring(L, 4));
            }
            else if (argn == 5)
            {
                new (lua::alloc<mysqlx::Session>(L)) mysqlx::Session(lua_tostring(L, 1), lua_tointeger(L, 2), lua_tostring(L, 3), lua_tostring(L, 4), lua_tostring(L, 5));
            }
            else
                throw std::runtime_error("Invalid number of arguments");
            return 1;
        } catch (const std::exception& e) {
            luaL_error(L, e.what());
        }
        return 0;
    });

    return 1;
}

extern "C" int luaopen_mysql(lua_State *L)
{
    lua_getglobal(L, "require");
    lua_pushstring(L, "mysql.impl");
    lua_call(L, 1, 1);
    return 1;
}