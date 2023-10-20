#include "lua.hpp"
#include "LuaBinding.h"
#include "mysqlx/xdevapi.h"

extern "C" int luaopen_mysql(lua_State *L)
{
    auto State = LuaBinding::State(L);

    State.addClass<mysqlx::Session>("Session")
        .cfun("sql", [](mysqlx::Session* session, LuaBinding::State S) {
            try {
                auto query = S.at(2).as<const char*>();
                mysqlx::SqlResult r = session->sql(query).execute();
                if (r.count() == 0) {
                    lua_newtable(S);
                    return 1;
                }
                auto rowlist = r.fetchAll();
                std::vector<mysqlx::Row> rows;
                for (auto row : rowlist)
                    rows.push_back(row);
                S.push(rows);
                return 1;
            } catch (const std::exception& e) {
                S.error(e.what());
            }
            return 0;
        });

    State.addClass<mysqlx::Row>("Row")
        .cfun("get", [](mysqlx::Row* row, lua_State *L) -> int {
            auto index = luaL_checkinteger(L, 2) - 1;
            auto v = (*row)[index];
            switch (v.getType()) {
                case mysqlx::Value::Type::VNULL:
                    return 0;
                case mysqlx::Value::Type::STRING: {
                    auto str = v.get<std::string>();
                    lua_pushlstring(L, str.c_str(), str.length());
                }   return 1;
                case mysqlx::Value::Type::UINT64:
                    lua_pushinteger(L, v.get<uint64_t>());
                    return 1;
                case mysqlx::Value::Type::INT64:
                    lua_pushinteger(L, v.get<int64_t>());
                    return 1;
                case mysqlx::Value::Type::FLOAT:
                    lua_pushnumber(L, v.get<float>());
                    return 1;
                case mysqlx::Value::Type::DOUBLE:
                    lua_pushnumber(L, v.get<double>());
                    return 1;
                case mysqlx::Value::Type::BOOL:
                    lua_pushboolean(L, v.get<bool>());
                    return 1;
            }
            return 0;
        })
        .meta_cfun("__cindex", [](mysqlx::Row* row, lua_State *L) -> int {
            mysqlx::Value v;
            if (lua_isnumber(L, 2))
            {
                auto index = luaL_checkinteger(L, 2) - 1;
                v = (*row)[index];
            } else
                return 0;
            switch (v.getType()) {
                case mysqlx::Value::Type::VNULL:
                    return 0;
                case mysqlx::Value::Type::STRING: {
                    auto str = v.get<std::string>();
                    lua_pushlstring(L, str.c_str(), str.length());
                }   return 1;
                case mysqlx::Value::Type::UINT64:
                    lua_pushinteger(L, v.get<uint64_t>());
                    return 1;
                case mysqlx::Value::Type::INT64:
                    lua_pushinteger(L, v.get<int64_t>());
                    return 1;
                case mysqlx::Value::Type::FLOAT:
                    lua_pushnumber(L, v.get<float>());
                    return 1;
                case mysqlx::Value::Type::DOUBLE:
                    lua_pushnumber(L, v.get<double>());
                    return 1;
                case mysqlx::Value::Type::BOOL:
                    lua_pushboolean(L, v.get<bool>());
                    return 1;
            }
            return 0;
        })
        .cfun("type", [](mysqlx::Row* row, lua_State *L) -> int {
            auto index = luaL_checkinteger(L, 2);
            auto v = (*row)[index];
            lua_pushinteger(L, v.getType());
            return 1;
        })
        .cfun("elementCount", [](mysqlx::Row* row, lua_State *L) -> int {
            auto index = luaL_checkinteger(L, 2);
            auto v = (*row)[index];
            lua_pushinteger(L, v.elementCount());
            return 1;
        })
        .cfun("at", [](mysqlx::Row* row, lua_State *L) -> int {
            auto index = luaL_checkinteger(L, 2);
            auto subindex = luaL_checkinteger(L, 3);
            auto v = (*row)[index][(int)subindex];
            switch (v.getType()) {
                case mysqlx::Value::Type::VNULL:
                    return 0;
                case mysqlx::Value::Type::STRING: {
                    auto str = v.get<std::string>();
                    lua_pushlstring(L, str.c_str(), str.length());
                }   return 1;
                case mysqlx::Value::Type::UINT64:
                    lua_pushinteger(L, v.get<uint64_t>());
                    return 1;
                case mysqlx::Value::Type::INT64:
                    lua_pushinteger(L, v.get<int64_t>());
                    return 1;
                case mysqlx::Value::Type::FLOAT:
                    lua_pushnumber(L, v.get<float>());
                    return 1;
                case mysqlx::Value::Type::DOUBLE:
                    lua_pushnumber(L, v.get<double>());
                    return 1;
                case mysqlx::Value::Type::BOOL:
                    lua_pushboolean(L, v.get<bool>());
                    return 1;
            }
            return 0;
        })
        .prop_cfun("colCount", [](mysqlx::Row* row, lua_State *L) -> int {
            lua_pushinteger(L, row->colCount());
            return 1;
        })
        ;

    State["Session"] = [](LuaBinding::State State) {
        try {
            auto args = State.args();
            if (args.size() == 1)
            {
                State.alloc<mysqlx::Session>(args[0].as<const char*>());
            }
            else if (args.size() == 4)
            {
                State.alloc<mysqlx::Session>(args[0].as<const char*>(), args[1].as<const char*>(), args[2].as<const char*>(), args[3].as<const char*>());
            }
            else
                throw std::runtime_error("Invalid number of arguments");
            return 1;
        } catch (const std::exception& e) {
            State.error(e.what());
        }
        return 0;
    };

    return 0;
}

int main() {
    auto State = LuaBinding::State(true);
    luaopen_mysql(State);
}