#ifndef _TABLE_H
#define _TABLE_H
#include <string>
#include <variant>
#include "env.h"
#include "page.h"

namespace DB::table
{

    using value_t = std::variant<int32_t, std::string>;

    class Table
    {
    public:

        uint32_t size() const;

    private:

        page::TableMetaPage* tablemeta_;

    };


} // end namespace DB::table

#endif // !_TABLE_H
