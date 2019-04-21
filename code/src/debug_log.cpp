#include "include/debug_log.h"
#include "include/buffer_pool.h"
#include <iostream>
#include <ostream>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <memory>
#include <filesystem>
using std::printf;
using std::cout;
using std::endl;

namespace DB::debug
{
    /*
    struct clean_output {
        clean_output(std::streambuf* coutbuf, std::ofstream* out)
            :coutbuf_(coutbuf), out_(out) {}
        ~clean_output() {
            std::cout.rdbuf(coutbuf_); //reset to standard output again
            out_->close();
        }
        std::streambuf* coutbuf_;
        std::ofstream* out_;
    };
    auto __ = []()
    {
        std::cout << std::filesystem::current_path() << endl;
        std::ofstream out;
        out.open(debug_output, std::ios::out | std::ios::trunc);
        if (!out.is_open()) {
            cout << "debug file open error" << endl;
        }
        std::streambuf *coutbuf = std::cout.rdbuf(); //save old buf
        std::cout.rdbuf(out.rdbuf()); //redirect std::cout to out.txt!
        return std::make_unique<clean_output>(coutbuf, &out);
    }();
    */

    void debug_page(bool config, const page::page_id_t page_id, buffer::BufferPoolManager* buffer_pool)
    {
        if (!config) return;

        printf("---------------------------------------------------\n");
        printf("[page_id = %d]\n", page_id);
        using namespace page;
        page::Page* node = buffer_pool->FetchPage(page_id);
        if (node == nullptr) {
            printf("page does not exist!!!\n");
            printf("---------------------------------------------------\n");
            return;
        }

        switch (node->get_page_t())
        {
        case page_t_t::INTERNAL:
            debug_internal(static_cast<const InternalPage*>(node));
            break;
        case page_t_t::LEAF:
            debug_leaf(static_cast<const LeafPage*>(node));
            break;
        case page_t_t::VALUE:
            debug_value(static_cast<const ValuePage*>(node));
            break;
        case page_t_t::ROOT_INTERNAL:
        case page_t_t::ROOT_LEAF:
            debug_root(static_cast<const RootPage*>(node));
            break;
        }
        node->unref();
        printf("---------------------------------------------------\n");
    }

    void debug_leaf(const page::LeafPage* leaf)
    {
        printf("[parent_page_id = %d]\n", leaf->get_parent_id());
        printf("[nEntry = %d]\n", leaf->get_nEntry());
        printf("[page_t = LeafPage]\n");
        printf("[ValuePage_id = %d]\n", leaf->value_page_id_);
        printf("[keys]: ");
        std::copy(leaf->keys_, leaf->keys_ + leaf->get_nEntry(),
            std::ostream_iterator<int32_t>(std::cout, " "));
        printf("\n");
        printf("[left = %d], [right = %d]\n", leaf->get_left_leaf(), leaf->get_right_leaf());
    }

    void debug_internal(const page::InternalPage* link)
    {
        printf("[parent_page_id = %d]\n", link->get_parent_id());
        printf("[nEntry = %d]\n", link->get_nEntry());
        printf("[page_t = InternalPage]\n");
        printf("[keys]: ");
        std::copy(link->keys_, link->keys_ + link->get_nEntry(),
            std::ostream_iterator<int32_t>(std::cout, " "));
        printf("\n");
        printf("[branch]: ");
        std::copy(link->branch_, link->branch_ + link->get_nEntry() + 1,
            std::ostream_iterator<int32_t>(std::cout, " "));
        printf("\n");
    }

    void debug_value(const page::ValuePage* value_page)
    {
        printf("[parent_page_id = %d]\n", value_page->parent_id_);
        printf("[nEntry = %d]\n", value_page->nEntry_);
        printf("[page_t = ValuePage]\n");
    }

    void debug_root(const page::RootPage* root)
    {
        printf("[parent_page_id = %d]\n", root->get_parent_id());
        printf("[nEntry = %d]\n", root->get_nEntry());
        if (root->get_page_t() == page::page_t_t::ROOT_INTERNAL)
        {
            printf("[page_t = ROOT_INTERNAL]\n");
            printf("[keys]: ");
            std::copy(root->keys_, root->keys_ + root->get_nEntry(),
                std::ostream_iterator<int32_t>(std::cout, " "));
            printf("\n");
            printf("[branch]: ");
            std::copy(root->branch_, root->branch_ + root->get_nEntry() + 1,
                std::ostream_iterator<int32_t>(std::cout, " "));
            printf("\n");
        }
        else
        {
            printf("[page_t = ROOT_LEAF]\n");
            printf("[keys]: ");
            std::copy(root->keys_, root->keys_ + root->get_nEntry(),
                std::ostream_iterator<int32_t>(std::cout, " "));
            printf("\n");
        }
    }

} // end namespace DB::debug