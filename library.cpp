#include "lua.hpp"
#include <string>
#include "mysqlx/xdevapi.h"
#include <shared/bind.h>

extern "C" int luaopen_mysql(lua_State *L)
{
    lua::bind::add<mysqlx::Session>(L, "Session")
        .fun("sql", [](lua_State* L) -> int {
            auto session = lua::check<mysqlx::Session>(L, 1);
            try {
                auto query = luaL_checkstring(L, 2);
                mysqlx::SqlResult r = session->sql(query).execute();
                if (r.count() == 0) {
                    lua_newtable(L);
                    return 1;
                }
                auto rowlist = r.fetchAll();
                lua_newtable(L);
                for (auto row : rowlist) {
                    new (lua::alloc<mysqlx::Row>(L)) mysqlx::Row(row);
                    lua_rawseti(L, -2, lua_objlen(L, -2) + 1);
                }
                return 1;
            } catch (const std::exception& e) {
                luaL_error(L, e.what());
            }
            return 0;
        });

    lua::bind::add<mysqlx::Row>(L, "Row")
        .fun("get", [](lua_State *L) -> int {
            auto row = lua::check<mysqlx::Row>(L, 1);
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
        .meta_fun("__cindex", [](lua_State *L) -> int {
            auto row = lua::check<mysqlx::Row>(L, 1);
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
        .fun("type", [](lua_State *L) -> int {
            auto row = lua::check<mysqlx::Row>(L, 1);
            auto index = luaL_checkinteger(L, 2);
            auto v = (*row)[index];
            lua_pushinteger(L, v.getType());
            return 1;
        })
        .fun("elementCount", [](lua_State *L) -> int {
            auto row = lua::check<mysqlx::Row>(L, 1);
            auto index = luaL_checkinteger(L, 2);
            auto v = (*row)[index];
            lua_pushinteger(L, v.elementCount());
            return 1;
        })
        .fun("at", [](lua_State *L) -> int {
            auto row = lua::check<mysqlx::Row>(L, 1);
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
        .prop("colCount", [](lua_State *L) -> int {
            auto row = lua::check<mysqlx::Row>(L, 1);
            lua_pushinteger(L, row->colCount());
            return 1;
        })
        ;

    lua_pushcfunction(L, +[](lua_State* L) {
        try {
            auto argn = lua_gettop(L);
            if (argn == 1)
            {
                //State.alloc<mysqlx::Session>(args[0].as<const char*>());
                new (lua::alloc<mysqlx::Session>(L)) mysqlx::Session(lua_tostring(L, 1));
            }
            else if (argn == 4)
            {
                //State.alloc<mysqlx::Session>(args[0].as<const char*>(), args[1].as<const char*>(), args[2].as<const char*>(), args[3].as<const char*>());
                new (lua::alloc<mysqlx::Session>(L)) mysqlx::Session(lua_tostring(L, 1), lua_tostring(L, 2), lua_tostring(L, 3), lua_tostring(L, 4));
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