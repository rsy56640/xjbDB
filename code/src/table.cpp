#include "include/table.h"
#include "include/BplusTree.h"

namespace DB::table
{

    uint32_t Table::size() const { return tablemeta_->bt_->size(); }



} // end namespace DB::table