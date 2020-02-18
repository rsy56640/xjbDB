//
// Created by 刘瑞康 on 2020/2/18.
//

#pragma once

#include "sql_expr.h"

/*
 * this file includes
 *  node structs of ap ast
 *  functions for ap ast
 */

namespace DB::ast{

    /*
     * functions
     *  root type must be comparison
     *  finish constant computation in place
     *  check existence of column
     * throw exception if check fails
     */
    // column_int / column_str / int / str
    // void apCheckVisit


}
