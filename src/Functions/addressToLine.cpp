//#if defined(__ELF__) && !defined(__FreeBSD__)

#include <Common/Dwarf.h>
#include <Common/SymbolIndex.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Arena.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>
#include <Access/AccessFlags.h>
#include <Interpreters/Context.h>

#include <mutex>
#include <filesystem>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class addressToLineWithInlines  : public IFunction
{
public:
    static constexpr auto name = "addressToLineWithInlines";
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToLine);
        return std::make_shared<addressToLineWithInlines>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Function " + getName() + " needs exactly one argument; passed "
            + toString(arguments.size()) + ".", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isUInt64())
            throw Exception("The only argument for function " + getName() + " must be UInt64. Found "
            + type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {

//            const ColumnArray::Offsets & offsets = array->getOffsets();
std::cout << 11111 << std::endl;


        const ColumnPtr & column = arguments[0].column;
        const ColumnUInt64 * column_concrete = checkAndGetColumn<ColumnUInt64>(column.get());
        std::cout << 11111 << std::endl;

        if (!column_concrete)
                throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        auto col_res = ColumnArray::create(ColumnString::create());
        ColumnArray::Offsets & res_offsets = col_res->getOffsets();

        ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
        ColumnString::Chars & res_strings_chars = res_strings.getChars();
        ColumnString::Offsets & res_strings_offsets = res_strings.getOffsets();
        std::cout << 11111 << std::endl;

        const typename ColumnVector<UInt64>::Container & data = column_concrete->getData();
//        auto result_column = ColumnString::create();
        res_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res_strings_offsets.reserve(i+1);
            std::cout << 11111 << std::endl;
            StringRef res_str = implCached(data[i]);
            const auto starting_offset = res_strings_offsets.empty() ? 0 : res_strings_offsets.back();
            std::cout << "*****"
                      << std::endl;

            if (res_str.size == 0)
            {

                std::cout << 121212 << std::endl;

                res_strings_chars.resize(res_strings_chars.size()+1);
                char * out = reinterpret_cast<char *>(res_strings_chars.data() + starting_offset);

                std::cout << 131313
                          << " "
                          << std::endl;

                *out = '\0';
                std::cout << 141414 << std::endl;

                std::cout << 151515 << std::endl;

                std::cout << 161616 << std::endl;

                res_strings_offsets.push_back(res_strings_offsets.back()+1);
                std::cout << 171717 << std::endl;
                res_offsets[i] = 1;
                continue;
            }
            res_strings_chars.resize(res_strings_chars.size()+res_str.size+1);
            char * out = reinterpret_cast<char *>(res_strings_chars.data() + starting_offset);

            std::cout << 22222 << std::endl;

            memcpy(out, res_str.data, res_str.size);
            std::cout << 33333
                      << " "
                      << res_str.size
                      << " "
                      << res_strings_offsets.back()
                      << std::endl;

            ++out;
            std::cout << 313131 << std::endl;

            *out = '\0';
            std::cout << 323232 << std::endl;

            res_strings_offsets.push_back(res_strings_offsets.back()+res_str.size+1);
            std::cout << 44444 << std::endl;
            res_offsets[i] = 1;
//            result_column->insertData(res_str.data, res_str.size);
        }
        std::cout << 55555 << std::endl;
//        res_offsets.reserve(1);
//        res_offsets.push_back(1);
        std::cout << 66666 << std::endl;
        std::cout << 77777
                  << res_strings_chars.size()
                  << res_strings_offsets.back()
                  << "   "
                  << res_offsets.back()
                  << res_strings.size()
                  << "   "
                  << res_strings_offsets.size()
                  << "   "
                  << input_rows_count
                  << std::endl;
        return col_res;
    }

private:
    struct Cache
    {
        std::mutex mutex;
        Arena arena;
        using Map = HashMap<uintptr_t, StringRef>;
        Map map;
        std::unordered_map<std::string, Dwarf> dwarfs;
    };

    mutable Cache cache;

    StringRef impl(uintptr_t addr) const
    {
        auto symbol_index_ptr = SymbolIndex::instance();
        const SymbolIndex & symbol_index = *symbol_index_ptr;

        if (const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr)))
        {
            auto dwarf_it = cache.dwarfs.try_emplace(object->name, object->elf).first;
            if (!std::filesystem::exists(object->name))
                return {};

            Dwarf::LocationInfo location;
            std::vector<Dwarf::SymbolizedFrame> frames;  // NOTE: not used in FAST mode.
            if (dwarf_it->second.findAddress(addr - uintptr_t(object->address_begin), location, Dwarf::LocationInfoMode::FAST, frames))
            {
                const char * arena_begin = nullptr;
                WriteBufferFromArena out(cache.arena, arena_begin);

                writeString(location.file.toString(), out);
                writeChar(':', out);
                writeIntText(location.line, out);

                return out.finish();
            }
            else
            {
                return object->name;
            }
        }
        else
            return {};
    }

    StringRef implCached(uintptr_t addr) const
    {
        Cache::Map::LookupResult it;
        bool inserted;
        std::lock_guard lock(cache.mutex);
        cache.map.emplace(addr, it, inserted);
        if (inserted)
            it->getMapped() = impl(addr);
        return it->getMapped();
    }
};

class FunctionAddressToLine : public IFunction
{
public:
    static constexpr auto name = "addressToLine";
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToLine);
        return std::make_shared<FunctionAddressToLine>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Function " + getName() + " needs exactly one argument; passed "
                + toString(arguments.size()) + ".", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isUInt64())
            throw Exception("The only argument for function " + getName() + " must be UInt64. Found "
                + type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        const ColumnUInt64 * column_concrete = checkAndGetColumn<ColumnUInt64>(column.get());

        if (!column_concrete)
            throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        const typename ColumnVector<UInt64>::Container & data = column_concrete->getData();
        auto result_column = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef res_str = implCached(data[i]);
            result_column->insertData(res_str.data, res_str.size);
        }

        return result_column;
    }

private:
    struct Cache
    {
        std::mutex mutex;
        Arena arena;
        using Map = HashMap<uintptr_t, StringRef>;
        Map map;
        std::unordered_map<std::string, Dwarf> dwarfs;
    };

    mutable Cache cache;

    StringRef impl(uintptr_t addr) const
    {
        auto symbol_index_ptr = SymbolIndex::instance();
        const SymbolIndex & symbol_index = *symbol_index_ptr;

        if (const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr)))
        {
            auto dwarf_it = cache.dwarfs.try_emplace(object->name, object->elf).first;
            if (!std::filesystem::exists(object->name))
                return {};

            Dwarf::LocationInfo location;
            std::vector<Dwarf::SymbolizedFrame> frames;  // NOTE: not used in FAST mode.
            if (dwarf_it->second.findAddress(addr - uintptr_t(object->address_begin), location, Dwarf::LocationInfoMode::FAST, frames))
            {
                const char * arena_begin = nullptr;
                WriteBufferFromArena out(cache.arena, arena_begin);

                writeString(location.file.toString(), out);
                writeChar(':', out);
                writeIntText(location.line, out);

                return out.finish();
            }
            else
            {
                return object->name;
            }
        }
        else
            return {};
    }

    StringRef implCached(uintptr_t addr) const
    {
        Cache::Map::LookupResult it;
        bool inserted;
        std::lock_guard lock(cache.mutex);
        cache.map.emplace(addr, it, inserted);
        if (inserted)
            it->getMapped() = impl(addr);
        return it->getMapped();
    }
};

}

void registerFunctionAddressToLine(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddressToLine>();
    factory.registerFunction<addressToLineWithInlines>();

}

}

//#endif
