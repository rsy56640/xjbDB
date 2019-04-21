#include "include/page.h"
#include "include/disk_manager.h"
#include "include/buffer_pool.h"
#include "include/debug_log.h"
#include "include/BplusTree.h"
#include <cstring>
#include <vector>

namespace DB::page
{

    Page* buffer_to_page(buffer::BufferPoolManager* buffer_pool, const char(&buffer)[page::PAGE_SIZE])
    {
        page_t_t type = static_cast<page_t_t>(read_int(buffer + offset::PAGE_T));
        switch (type)
        {
        case page_t_t::DB_META:
            return parse_DBMetaPage(buffer_pool, buffer);
        case page_t_t::TABLE_META:
            return parse_TableMetaPage(buffer_pool, buffer);
        case page_t_t::ROOT_INTERNAL:
        case page_t_t::ROOT_LEAF:
            return parse_RootPage(buffer_pool, buffer);
        case page_t_t::INTERNAL:
            return parse_InternalPage(buffer_pool, buffer);
        case page_t_t::LEAF:
            return parse_LeafPage(buffer_pool, buffer);
        case page_t_t::VALUE:
            return parse_ValuePage(buffer_pool, buffer);
        default:
            debug::ERROR_LOG("UNKNOWN type\n");
            return nullptr;
        }
    }



    DBMetaPage* parse_DBMetaPage(buffer::BufferPoolManager* buffer_pool, const char(&buffer)[page::PAGE_SIZE])
    {
        page_t_t page_t = static_cast<page_t_t>(read_int(buffer + offset::PAGE_T));
        page_id_t page_id = read_int(buffer + offset::PAGE_ID);
        uint32_t cur_page_no = read_int(buffer + offset::CUR_PAGE_NO);
        uint32_t table_num = read_int(buffer + offset::TABLE_NUM);
        DBMetaPage* page = new DBMetaPage(page_id, buffer_pool->disk_manager_,
            false, cur_page_no, table_num);
        std::memcpy(page->get_data(), buffer, page::PAGE_SIZE);
        for (uint32_t i = 0; i < table_num; i++) {
            page->table_page_ids_[i] =
                read_int(buffer + offset::TABLE_PAGEID_NAMEOFFSET_START + 8 * i);
            page->table_name_offset_[i] =
                read_int(buffer + offset::TABLE_PAGEID_NAMEOFFSET_START + 8 * i + 4);
            if (buffer[page->table_name_offset_[i] + DBMetaPage::MAX_TABLE_NAME_STR] == '\0')
                page->table_name2id_[std::string(buffer + page->table_name_offset_[i] + 1)]
                = page->table_page_ids_[i];
            else
                page->table_name2id_[std::string(buffer + page->table_name_offset_[i] + 1, DBMetaPage::MAX_TABLE_NAME_STR)]
                = page->table_page_ids_[i];
        }
        return page;
    }


    TableMetaPage* parse_TableMetaPage(buffer::BufferPoolManager* buffer_pool, const char(&buffer)[page::PAGE_SIZE])
    {
        page_t_t page_t = static_cast<page_t_t>(read_int(buffer + offset::PAGE_T));
        page_id_t page_id = read_int(buffer + offset::PAGE_ID);
        page_id_t BT_root_id = read_int(buffer + offset::BT_ROOT_ID);
        uint32_t col_num = read_int(buffer + offset::COL_NUM);
        page_id_t default_page_id = read_int(buffer + offset::DEFAULT_VALUE_PAGE_ID);

        std::vector<std::string> cols(col_num);
        std::unordered_map<std::string, ColumnInfo*> col_name2col;
        uint32_t pk_col;
        key_t_t key_t;
        uint32_t str_len;

        for (uint32_t i = 0; i < col_num; i++) {
            ColumnInfo* col = new ColumnInfo;
            col->col_name_offset_ =
                read_int(buffer + offset::COLINFO_START + 14 * i);
            col->col_t_ = static_cast<col_t_t>(
                read_short(buffer + offset::COLINFO_START + 14 * i + 4));
            col->str_len_ =
                read_short(buffer + offset::COLINFO_START + 14 * i + 6);
            col->constraint_t_ =
                read_short(buffer + offset::COLINFO_START + 14 * i + 8);
            col->other_value_ =
                read_int(buffer + offset::COLINFO_START + 14 * i + 10);
            if (col->isPK()) {
                pk_col = i;
                key_t = static_cast<key_t_t>(col->col_t_);
                str_len = col->str_len_;
            }
            cols[i] = std::string(buffer + offset::COLUMN_NAME_STR_START + 1, TableMetaPage::MAX_COLUMN_NAME_STR);
            col_name2col[cols[i]] = col;
        }

        TableMetaPage* page = new TableMetaPage(buffer_pool, page_id,
            // `BT_root_id` is needed only when (!init)
            buffer_pool->disk_manager_, false, key_t, str_len, BT_root_id, col_num, default_page_id);
        std::memcpy(page->get_data(), buffer, page::PAGE_SIZE);

        page->pk_col_ = pk_col;
        page->cols_ = std::move(cols);
        page->col_name2col_ = std::move(col_name2col);

        return page;
    }


    InternalPage* parse_InternalPage(buffer::BufferPoolManager* buffer_pool, const char(&buffer)[page::PAGE_SIZE])
    {
        page_t_t page_t = static_cast<page_t_t>(read_int(buffer + offset::PAGE_T));
        page_id_t page_id = read_int(buffer + offset::PAGE_ID);
        page_id_t parent_id = read_int(buffer + offset::PARENT_PAGE_ID);
        uint32_t nEntry = read_int(buffer + offset::NENTRY);
        key_t_t key_t = static_cast<key_t_t>(read_short(buffer + offset::KEY_T));
        uint32_t str_len = read_short(buffer + offset::STR_LEN);
        InternalPage* page = new InternalPage(page_t, page_id, parent_id, nEntry,
            buffer_pool->disk_manager_, key_t, str_len, false);
        std::memcpy(page->get_data(), buffer, page::PAGE_SIZE);
        for (uint32_t i = 0; i < nEntry; i++) {
            page->branch_[i] =
                read_int(buffer + offset::CHILD_START + 8 * i);
            page->keys_[i] =
                read_int(buffer + offset::CHILD_START + 8 * i + 4);
        }
        page->branch_[nEntry] = read_int(buffer + offset::CHILD_START + 8 * nEntry);
        return page;
    }


    ValuePage* parse_ValuePage(buffer::BufferPoolManager* buffer_pool, const char(&buffer)[page::PAGE_SIZE])
    {
        page_t_t page_t = static_cast<page_t_t>(read_int(buffer + offset::PAGE_T));
        page_id_t page_id = read_int(buffer + offset::PAGE_ID);
        page_id_t parent_id = read_int(buffer + offset::PARENT_PAGE_ID);
        uint32_t nEntry = read_int(buffer + offset::NENTRY);
        ValuePage* page = new ValuePage(page_id, parent_id, nEntry,
            buffer_pool->disk_manager_, false);
        std::memcpy(page->get_data(), buffer, page::PAGE_SIZE);
        return page;
    }


    LeafPage* parse_LeafPage(buffer::BufferPoolManager* buffer_pool, const char(&buffer)[page::PAGE_SIZE])
    {
        page_t_t page_t = static_cast<page_t_t>(read_int(buffer + offset::PAGE_T));
        page_id_t page_id = read_int(buffer + offset::PAGE_ID);
        page_id_t parent_id = read_int(buffer + offset::PARENT_PAGE_ID);
        uint32_t nEntry = read_int(buffer + offset::NENTRY);
        key_t_t key_t = static_cast<key_t_t>(read_short(buffer + offset::KEY_T));
        uint32_t str_len = read_short(buffer + offset::STR_LEN);
        page_id_t value_page_id = read_int(buffer + offset::VALUE_PAGE_ID);
        LeafPage* page = new LeafPage(buffer_pool, page_id, parent_id, nEntry,
            buffer_pool->disk_manager_, key_t, str_len, value_page_id, false);
        std::memcpy(page->get_data(), buffer, page::PAGE_SIZE);
        page->previous_page_id_ = read_int(buffer + offset::PREVIOUS_PAGE_ID);
        page->next_page_id_ = read_int(buffer + offset::NEXT_PAGE_ID);
        for (uint32_t i = 0; i < nEntry; i++) {
            page->keys_[i] =
                read_int(buffer + offset::KV_START + 8 * i);
            page->values_[i] =
                read_int(buffer + offset::KV_START + 8 * i + 4);
        }
        return page;
    }


    RootPage* parse_RootPage(buffer::BufferPoolManager* buffer_pool, const char(&buffer)[page::PAGE_SIZE])
    {
        page_t_t page_t = static_cast<page_t_t>(read_int(buffer + offset::PAGE_T));
        page_id_t page_id = read_int(buffer + offset::PAGE_ID);
        page_id_t parent_id = read_int(buffer + offset::PARENT_PAGE_ID);
        uint32_t nEntry = read_int(buffer + offset::NENTRY);
        key_t_t key_t = static_cast<key_t_t>(read_short(buffer + offset::KEY_T));
        uint32_t str_len = read_short(buffer + offset::STR_LEN);
        uint32_t value_page_id = NOT_A_PAGE;
        if (page_t == page_t_t::ROOT_LEAF)
            value_page_id = read_int(buffer + offset::VALUE_PAGE_ID);

        RootPage* page = new RootPage(buffer_pool, page_t, page_id, parent_id, nEntry,
            buffer_pool->disk_manager_, key_t, str_len, value_page_id, false);

        if (page_t == page_t_t::ROOT_LEAF) {
            for (uint32_t i = 0; i < nEntry; i++) {
                page->keys_[i] =
                    read_int(buffer + offset::KV_START + 8 * i);
                page->values_[i] =
                    read_int(buffer + offset::KV_START + 8 * i + 4);
            }
        }
        else {
            for (uint32_t i = 0; i < nEntry; i++) {
                page->branch_[i] =
                    read_int(buffer + offset::CHILD_START + 8 * i);
                page->keys_[i] =
                    read_int(buffer + offset::CHILD_START + 8 * i + 4);
            }
        }

        std::memcpy(page->get_data(), buffer, page::PAGE_SIZE);
        return page;
    }





    // TODO: all ctor/dtor !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    Page::Page(page_t_t page_t, page_id_t page_id,
        disk::DiskManager* disk_manager, bool isInit)
        :
        disk_manager_(disk_manager),
        page_t_(page_t),
        page_id_(page_id),
        ref_count_(0),
        dirty_(false),
        discarded_(false)
    {
        if (!isInit) {
            disk_manager_->ReadPage(page_id_, data_);
            write_int(data_ + offset::PAGE_T, static_cast<uint32_t>(page_t_));
            write_int(data_ + offset::PAGE_ID, page_id_);
            set_dirty();
        }
        else
            std::memset(data_, 0, PAGE_SIZE * sizeof(char));
    }

    Page::~Page() {
        if (dirty_)
            disk_manager_->WritePage(page_id_, data_);
    }

    void Page::ref() {
        debug::DEBUG_LOG(debug::PAGE_REF, "page_id %d ref %d -> %d\n",
            this->get_page_id(), ref_count_.load(), ref_count_.load() + 1);
        ref_count_++;
    }

    void Page::unref() {
        debug::DEBUG_LOG(debug::PAGE_REF, "page_id %d ref %d -> %d\n",
            this->get_page_id(), ref_count_.load(), ref_count_.load() - 1);
        if (--ref_count_ == 0) {
            if (dirty_) { // no contend reading on `dirty_`
                this->update_data();
                flush();
            }
            delete this;
        }
    }

    page_t_t Page::get_page_t() const noexcept {
        return page_t_;
    }

    page_id_t Page::get_page_id() const noexcept {
        return page_id_;
    }

    char* Page::get_data() noexcept {
        return data_;
    }

    void Page::set_dirty() noexcept {
        dirty_ = true;
        // disk_manager_->set_dirty(this->page_id_);
    }

    bool Page::is_dirty() noexcept {
        return dirty_;
    }

    void Page::flush() {
        if (dirty_) {
            update_data();
            disk_manager_->WritePage(page_id_, data_);
            dirty_ = false;
        }
        // UNDONE: maybe dirty-bitmap for log
    }

    bool Page::try_page_read_lock() {
#ifdef SIMPLE_TEST
        return true;
#endif // SIMPLE_TEST
        return rw_page_mutex_.try_lock_shared();
    }
    bool Page::try_page_write_lock() {
#ifdef SIMPLE_TEST
        return true;
#endif // SIMPLE_TEST
        return rw_page_mutex_.try_lock();
    }
    void Page::page_read_lock() {
#ifdef SIMPLE_TEST
        return;
#endif // SIMPLE_TEST
        while (!(try_page_read_lock()));
    }
    void Page::page_write_lock() {
#ifdef SIMPLE_TEST
        return;
#endif // SIMPLE_TEST
        while (!(try_page_write_lock()));
    }
    void Page::page_read_unlock() {
#ifdef SIMPLE_TEST
        return;
#endif // SIMPLE_TEST
        rw_page_mutex_.unlock_shared();
    }
    void Page::page_write_unlock() {
#ifdef SIMPLE_TEST
        return;
#endif // SIMPLE_TEST
        rw_page_mutex_.unlock();
    }


    //
    // DBMetaPage
    //

    DBMetaPage::DBMetaPage(page_id_t page_id,
        disk::DiskManager* disk_manager, bool isInit, uint32_t cur_page_no, uint32_t table_num)
        :
        Page(page_t_t::DB_META, page_id, disk_manager, isInit),
        cur_page_no_(cur_page_no),
        table_num_(table_num)
    {
        table_page_ids_ = new uint32_t[MAX_TABLE_NUM];
        table_name_offset_ = new uint32_t[MAX_TABLE_NUM];
    }

    DBMetaPage::~DBMetaPage() {
        update_data();
        delete[] table_page_ids_;
        delete[] table_name_offset_;
    }

    page_id_t DBMetaPage::find_table(const std::string& table_name) {
        auto it = table_name2id_.find(table_name);
        if (it == table_name2id_.end())
            return NOT_A_PAGE;
        return it->second;
    }

    bool DBMetaPage::insert_table(page_id_t table_page_id, const std::string& table_name)
    {
        if (table_name.size() > MAX_TABLE_NAME_STR)
            return false;

        // find a `OBSOLETE` record.
        uint32_t tableNameIndex = 0;
        for (; tableNameIndex < MAX_TABLE_NUM; tableNameIndex++)
            if (data_[offset::TABLE_NAME_STR_START + tableNameIndex * TABLE_NAME_STR_BLOCK]
                == static_cast<char>(str_state::OBSOLETE))
                break;

        if (tableNameIndex == MAX_TABLE_NUM) {
            debug::ERROR_LOG("`DBMetaPage::create_table()`, might forget erase table_str somewhere");
            return false;
        }

        const uint32_t offset = offset::TABLE_NAME_STR_START + tableNameIndex * TABLE_NAME_STR_BLOCK;

        // set table_page_id
        table_page_ids_[table_num_] = table_page_id;
        table_name_offset_[table_num_] = offset;
        table_num_++;

        // write table_name_str
        data_[offset] = static_cast<char>(str_state::INUSED);
        char buffer[MAX_TABLE_NAME_STR] = { 0 };
        std::memcpy(buffer, table_name.c_str(), table_name.size());
        std::memcpy(data_ + offset + 1, buffer, MAX_TABLE_NAME_STR);

        set_dirty();
        return true;
    }

    bool DBMetaPage::drop_table(const std::string& table_name)
    {
        auto it = table_name2id_.find(table_name);
        if (it == table_name2id_.end())
            return false;

        const page_id_t table_id = it->second;
        uint32_t index = 0;
        for (; index < table_num_; index++)
            if (table_page_ids_[index] == table_id)
                break;

        const uint32_t offset = table_name_offset_[index];
        data_[offset] = static_cast<char>(str_state::OBSOLETE);

        // compact in `data_` will be done in `update_data()`
        table_num_--;
        for (uint32_t i = index; i < table_num_; i++)
        {
            table_page_ids_[i] = table_page_ids_[i + 1];
            table_name_offset_[i] = table_name_offset_[i + 1];
        }

        set_dirty();
        return true;
    }


    void DBMetaPage::update_data()
    {
        const page_id_t cur_page_no_ = disk_manager_->get_cut_page_id();
        write_int(data_ + offset::PAGE_T, static_cast<uint32_t>(page_t_));
        write_int(data_ + offset::PAGE_ID, page_id_);
        write_int(data_ + offset::CUR_PAGE_NO, cur_page_no_);
        write_int(data_ + offset::TABLE_NUM, table_num_);
        for (uint32_t i = 0; i < table_num_; i++)
        {
            write_int(data_ + offset::TABLE_PAGEID_NAMEOFFSET_START + 8 * i,
                table_page_ids_[i]);
            write_int(data_ + offset::TABLE_PAGEID_NAMEOFFSET_START + 8 * i + 4,
                table_name_offset_[i]);
        }
    }



    //
    // TableMetaPage
    //
    TableMetaPage::TableMetaPage(buffer::BufferPoolManager* buffer_pool, page_id_t page_id,
        disk::DiskManager* disk_manager, bool isInit, key_t_t key_t, uint32_t str_len,
        page_id_t BT_root_id, uint32_t col_num, page_id_t default_value_page_id)
        // `BT_root_id` is needed only when (!init)
        :
        Page(page_t_t::TABLE_META, page_id, disk_manager, isInit),
        BT_root_id_(BT_root_id),
        col_num_(col_num),
        default_value_page_id_(default_value_page_id)
    {
        if (!isInit) {
            if (default_value_page_id_ != NOT_A_PAGE) {
                value_page_ = static_cast<ValuePage*>(buffer_pool->FetchPage(default_value_page_id_));
                buffer_pool->DeletePage(default_value_page_id_);
            }
            tree::OpenTableInfo info;
            info.isInit = false;
            info.root_id = BT_root_id_;
            bt_ = new tree::BTree(info, buffer_pool, key_t, str_len);
        }
        else {
            col_num_ = 0;
            default_value_page_id_ = NOT_A_PAGE;
            // create B+Tree
            tree::OpenTableInfo info;
            info.isInit = true;
            bt_ = new tree::BTree(info, buffer_pool, key_t, str_len);
            BT_root_id_ = bt_->get_root_id();
        }
    }


    TableMetaPage::~TableMetaPage()
    {
        update_data();
        if (default_value_page_id_ != NOT_A_PAGE)
            value_page_->unref();
        for (auto[k, v] : col_name2col_)
            delete v;
        delete bt_;
    }


    bool TableMetaPage::is_PK(const std::string& col_name) const {
        auto it = col_name2col_.find(col_name);
        if (it == col_name2col_.end())
            return false;
        return it->second->isPK();
    }
    bool TableMetaPage::is_FK(const std::string& col_name) const {
        auto it = col_name2col_.find(col_name);
        if (it == col_name2col_.end())
            return false;
        return it->second->isFK();
    }
    bool TableMetaPage::is_not_null(const std::string& col_name) const {
        auto it = col_name2col_.find(col_name);
        if (it == col_name2col_.end())
            return false;
        return it->second->isNOT_NULL();
    }
    bool TableMetaPage::is_default_col(const std::string& col_name) const {
        auto it = col_name2col_.find(col_name);
        if (it == col_name2col_.end())
            return false;
        return it->second->isDEFAULT();
    }

    ValueEntry TableMetaPage::get_default_value(const std::string& col_name) const {
        auto it = col_name2col_.find(col_name);
        if (it == col_name2col_.end())
            return {};
        ColumnInfo* col = it->second;
        if (!col->isDEFAULT())
            return {};
        ValueEntry vEntry;
        vEntry.value_state_ = value_state::INUSED;
        value_page_->read_content(col->other_value_, vEntry);
        return vEntry;
    }


    void TableMetaPage::insert_column(const std::string& col_name, ColumnInfo* col) {
        cols_.push_back(col_name);
        col_name2col_[col_name] = col;
        col_num_++;
        if (col_name.size() > MAX_COLUMN_NAME_STR)
        {
            debug::ERROR_LOG("col name size is invalid: \"%s\"\n", col_name.c_str());
            return;
        }
        const uint32_t offset = offset::COLUMN_NAME_STR_START + (col_num_ - 1) * COLUMN_NAME_STR_BLOCK;
        col->col_name_offset_ = offset;
        data_[offset] = static_cast<char>(str_state::INUSED);
        std::memcpy(data_ + offset + 1, col_name.c_str(), col_name.size());
        set_dirty();
    }


    void TableMetaPage::update_data() {
        write_int(data_ + offset::PAGE_T, static_cast<uint32_t>(page_t_));
        write_int(data_ + offset::PAGE_ID, page_id_);
        write_int(data_ + offset::BT_ROOT_ID, BT_root_id_);
        write_int(data_ + offset::COL_NUM, col_num_);
        write_int(data_ + offset::DEFAULT_VALUE_PAGE_ID, default_value_page_id_);
        for (uint32_t i = 0; i < col_num_; i++) {
            ColumnInfo* col = col_name2col_[cols_[i]];
            write_int(data_ + offset::COLINFO_START + 14 * i,
                col->col_name_offset_);
            write_short(data_ + offset::COLINFO_START + 14 * i + 4,
                static_cast<uint16_t>(col->col_t_));
            write_short(data_ + offset::COLINFO_START + 14 * i + 6,
                col->str_len_);
            write_short(data_ + offset::COLINFO_START + 14 * i + 8,
                col->constraint_t_);
            write_int(data_ + offset::COLINFO_START + 14 * i + 10,
                col->other_value_);
        }

        for (uint32_t colNameIndex = 0; colNameIndex < col_num_; colNameIndex++)
        {
            const uint32_t offset = offset::COLUMN_NAME_STR_START + colNameIndex * COLUMN_NAME_STR_BLOCK;
            data_[offset] = static_cast<char>(str_state::INUSED);
            char buffer[MAX_COLUMN_NAME_STR] = { 0 };
            std::memcpy(buffer, cols_[colNameIndex].c_str(), cols_[colNameIndex].size());
            std::memcpy(data_ + offset + 1, buffer, MAX_COLUMN_NAME_STR);
        }
    }


    //////////////////////////////////////////////////////////////////////
    //////////////////////           util           //////////////////////
    //////////////////////////////////////////////////////////////////////

    uint32_t read_int(const char* data) {
        // This prevents buf[i] from being promoted to a signed int.
        uint32_t
            u0 = static_cast<unsigned char>(data[0]),
            u1 = static_cast<unsigned char>(data[1]),
            u2 = static_cast<unsigned char>(data[2]),
            u3 = static_cast<unsigned char>(data[3]);
        uint32_t uval = u0 | (u1 << 8) | (u2 << 16) | (u3 << 24);
        return uval;
    }

    void write_int(char* data, uint32_t value) {
        data[0] = value;
        data[1] = value >> 8;
        data[2] = value >> 16;
        data[3] = value >> 24;
    }

    uint32_t read_short(const char* data) {
        uint32_t
            u0 = static_cast<unsigned char>(data[0]),
            u1 = static_cast<unsigned char>(data[1]);
        uint32_t uval = u0 | (u1 << 8);
        return uval;
    }

    void write_short(char* data, uint16_t value) {
        data[0] = static_cast<char>(value);
        data[1] = static_cast<char>(value >> 8);
    }

    char read_char(const char* s) {
        return s[0];
    }

    void write_char(char* s, char c) {
        s[0] = c;
    }

    //////////////////////////////////////////////////////////////////////
    //////////////////////       B+ Tree Page       //////////////////////
    //////////////////////////////////////////////////////////////////////

    //
    // BTree Page
    //
    BTreePage::BTreePage(page_t_t page_t, page_id_t page_id, page_id_t parent_id, uint32_t nEntry,
        disk::DiskManager* disk_manager, key_t_t key_t, uint32_t str_len, bool isInit)
        :Page(page_t, page_id, disk_manager, isInit),
        parent_id_(parent_id),
        nEntry_(nEntry),
        key_t_(key_t),
        str_len_(str_len)
        //, last_offset_(PAGE_SIZE)
    {
        keys_ = new int32_t[BTNodeKeySize];
        if (!isInit) {
            set_dirty();
        }
    }

    BTreePage::~BTreePage()
    {
        delete[] keys_;
    }

    page_id_t BTreePage::get_parent_id() const noexcept { return parent_id_; }
    uint32_t BTreePage::get_nEntry() const noexcept { return nEntry_; }
    void BTreePage::set_parent_id(page_id_t parent_id) noexcept {
        parent_id_ = parent_id;
        set_dirty();
    }
    key_t_t BTreePage::get_key_t() const noexcept { return key_t_; }
    uint32_t BTreePage::get_str_len() const noexcept { return str_len_; }


    void BTreePage::insert_key(uint32_t index, const KeyEntry& kEntry)
    {
        if (key_t_ == key_t_t::INTEGER)
        {
            keys_[index] = kEntry.key_int;
        }
        else // key is str.
        {
            // find a `OBSOLETE` record.
            uint32_t keyStrIndex = 0;
            for (; keyStrIndex < BTNodeKeySize; keyStrIndex++)
                if (data_[KEY_STR_START + keyStrIndex * KEY_STR_BLOCK]
                    == static_cast<char>(str_state::OBSOLETE))
                    break;

            if (keyStrIndex == BTNodeKeySize)
                debug::ERROR_LOG("`BTreePage::insert_key()`, might forget erase key somewhere");

            const uint32_t offset = KEY_STR_START + keyStrIndex * KEY_STR_BLOCK;
            keys_[index] = offset; // update key-index
            data_[offset] = static_cast<char>(str_state::INUSED);
            const uint32_t min_size =
                MAX_STR_LEN > kEntry.key_str.size() ? kEntry.key_str.size() : MAX_STR_LEN;
            std::memcpy(data_ + offset + 1, kEntry.key_str.c_str(), min_size);
        }
        set_dirty();
    }


    void BTreePage::erase_key(uint32_t index)
    {
        // if key is (VAR)CHAR attached at the end.
        if (key_t_ != key_t_t::INTEGER)
        {
            const uint32_t offset = keys_[index];
            data_[offset] = static_cast<char>(str_state::OBSOLETE);
        }
        set_dirty();
    }


    KeyEntry BTreePage::read_key(uint32_t index) const
    {
        KeyEntry kEntry;
        kEntry.key_t = key_t_;
        if (key_t_ == key_t_t::INTEGER)
        {
            kEntry.key_int = keys_[index];
        }
        else
        {
            const uint32_t offset = keys_[index];
            if (data_[offset + KEY_STR_BLOCK - 1] != '\0')
                kEntry.key_str = std::string(data_ + offset + 1, KEY_STR_BLOCK - 1);
            else
                kEntry.key_str = std::string(data_ + offset + 1);
        }
        return kEntry;
    }


    //
    // InternalPage
    //
    InternalPage::InternalPage(page_t_t page_t, page_id_t page_id, page_id_t parent_id, uint32_t nEntry,
        disk::DiskManager* disk_manager, key_t_t key_t, uint32_t str_len, bool isInit)
        :BTreePage(page_t, page_id, parent_id, nEntry, disk_manager, key_t, str_len, isInit)
    {
        branch_ = new page_id_t[BTNodeBranchSize];
    }

    InternalPage::~InternalPage()
    {
        update_data();
        delete[] branch_;
    }

    void InternalPage::update_data()
    {
        write_int(data_ + offset::PAGE_T, static_cast<uint32_t>(page_t_));
        write_int(data_ + offset::PAGE_ID, page_id_);
        write_int(data_ + offset::PARENT_PAGE_ID, parent_id_);
        write_int(data_ + offset::NENTRY, nEntry_);
        write_short(data_ + offset::KEY_T, static_cast<uint16_t>(key_t_));
        write_short(data_ + offset::STR_LEN, str_len_);
        for (uint32_t i = 0; i < nEntry_; i++) {
            write_int(data_ + offset::CHILD_START + 8 * i,
                branch_[i]);
            write_int(data_ + offset::CHILD_START + 8 * i + 4,
                keys_[i]);
        }
        write_int(data_ + offset::CHILD_START + 8 * nEntry_, branch_[nEntry_]);
    }



    //
    // ValuePage
    //
    ValuePage::ValuePage(page_id_t page_id, page_id_t parent_id, uint32_t nEntry,
        disk::DiskManager* disk_manager, bool isInit)
        :
        Page(page_t_t::VALUE, page_id, disk_manager, isInit),
        parent_id_(parent_id),
        nEntry_(nEntry)
    {
        if (!isInit) {
            set_dirty();
        }
    }

    ValuePage::~ValuePage() {
        update_data();
    }

    void ValuePage::read_content(uint32_t offset, ValueEntry& vEntry) const
    {
        vEntry.value_state_ = static_cast<value_state>(data_[offset]);
        std::memcpy(vEntry.content_, data_ + offset + 1, TUPLE_BLOCK_SIZE - 1);
    }

    void ValuePage::update_content(uint32_t offset, const ValueEntry& vEntry)
    {
        set_dirty();
        if (vEntry.value_state_ != value_state::INUSED)
            debug::ERROR_LOG("`ValuePage::update_content()` input data invalid.");
        data_[offset] = static_cast<char>(vEntry.value_state_);
        std::memcpy(data_ + offset + 1, vEntry.content_, MAX_TUPLE_SIZE);
    }

    uint32_t ValuePage::write_content(const ValueEntry& vEntry)
    {
        nEntry_++;
        set_dirty();
        // find an obsolete record.
        uint32_t index = 0;
        for (; index < BTNodeKeySize; index++)
            if (data_[offset::VALUE_START + index * TUPLE_BLOCK_SIZE] == static_cast<char>(value_state::OBSOLETE))
                break;
        const uint32_t offset = offset::VALUE_START + index * TUPLE_BLOCK_SIZE;
        data_[offset] = static_cast<char>(value_state::INUSED);
        std::memcpy(data_ + offset + 1, vEntry.content_, MAX_TUPLE_SIZE);
        return offset;
    }

    void ValuePage::erase_block(uint32_t offset) {
        nEntry_--;
        data_[offset] = static_cast<char>(value_state::OBSOLETE);
        set_dirty();
    }

    void ValuePage::update_data()
    {
        write_int(data_ + offset::PAGE_T, static_cast<uint32_t>(page_t_));
        write_int(data_ + offset::PAGE_ID, page_id_);
        write_int(data_ + offset::PARENT_PAGE_ID, parent_id_);
        write_int(data_ + offset::NENTRY, nEntry_);
    }



    //
    // LeafPage
    //
    LeafPage::LeafPage(buffer::BufferPoolManager* buffer_pool, page_id_t page_id, page_id_t parent_id, uint32_t nEntry,
        disk::DiskManager* disk_manager, key_t_t key_t, uint32_t str_len, page_id_t value_page_id, bool isInit)
        :BTreePage(page_t_t::LEAF, page_id, parent_id, nEntry,
            disk_manager, key_t, str_len, isInit), value_page_id_(value_page_id)
    {
        values_ = new uint32_t[BTNodeKeySize];
        std::memset(values_, 0, BTNodeKeySize * sizeof(uint32_t));
        if (isInit) // new
        {
            PageInitInfo info;
            info.page_t = page_t_t::VALUE;
            info.parent_id = this->get_page_id();
            value_page_ = static_cast<ValuePage*>(buffer_pool->NewPage(info));
            value_page_id_ = value_page_->get_page_id();
            //buffer_pool->DeletePage(value_page_id_);
            // now value_page has excatly *** 1 ref count ***.
            write_int(data_ + offset::VALUE_PAGE_ID, value_page_id_);
            value_page_->set_dirty();
            set_dirty();
        }
        else // exist
        {
            value_page_ = static_cast<ValuePage*>(buffer_pool->FetchPage(value_page_id_));
            //buffer_pool->DeletePage(value_page_id_);
            // now value_page has excatly *** 1 ref count ***.
        }
    }

    LeafPage::~LeafPage() {
        update_data();
        value_page_->unref(); // ref 1->0
        delete[] values_;
    }

    void LeafPage::read_value(uint32_t index, ValueEntry& vEntry) const
    {
        value_page_->read_content(values_[index], vEntry);
        if (vEntry.value_state_ != value_state::INUSED)
            debug::ERROR_LOG("`LeafPage::read_value()` read corrupted value.");
    }

    void LeafPage::insert_value(uint32_t index, const ValueEntry& vEntry)
    {
        values_[index] = value_page_->write_content(vEntry);
        set_dirty();
    }

    void LeafPage::erase_value(uint32_t index) {
        value_page_->erase_block(values_[index]);
    }

    void LeafPage::update_value(uint32_t index, const ValueEntry& vEntry) {
        value_page_->update_content(values_[index], vEntry);
    }

    void LeafPage::set_left_leaf(page_id_t previous_page_id) { previous_page_id_ = previous_page_id; }
    void LeafPage::set_right_leaf(page_id_t next_page_id) { next_page_id_ = next_page_id; }
    page_id_t LeafPage::get_left_leaf() const { return previous_page_id_; }
    page_id_t LeafPage::get_right_leaf() const { return next_page_id_; }

    void LeafPage::update_data()
    {
        write_int(data_ + offset::PAGE_T, static_cast<uint32_t>(page_t_));
        write_int(data_ + offset::PAGE_ID, page_id_);
        write_int(data_ + offset::PARENT_PAGE_ID, parent_id_);
        write_int(data_ + offset::NENTRY, nEntry_);
        write_short(data_ + offset::KEY_T, static_cast<uint16_t>(key_t_));
        write_short(data_ + offset::STR_LEN, str_len_);
        write_int(data_ + offset::VALUE_PAGE_ID, value_page_id_);
        write_int(data_ + offset::PREVIOUS_PAGE_ID, previous_page_id_);
        write_int(data_ + offset::NEXT_PAGE_ID, next_page_id_);
        for (uint32_t i = 0; i < nEntry_; i++) {
            write_int(data_ + offset::KV_START + 8 * i,
                keys_[i]);
            write_int(data_ + offset::KV_START + 8 * i + 4,
                values_[i]);
        }
    }



    //
    // RootPage
    //
    //(buffer::BufferPoolManager*, page_t_t, page_id_t parent_id, page_id_t,
    //uint32_t nEntry, disk::DiskManager*, key_t_t, uint32_t str_len, page_id_t value_page_id, bool isInit);
    RootPage::RootPage(buffer::BufferPoolManager* buffer_pool, page_t_t page_t, page_id_t page_id, page_id_t parent_id, uint32_t nEntry,
        disk::DiskManager* disk_manager, key_t_t key_t, uint32_t str_len, page_id_t value_page_id, bool isInit)
        :InternalPage(page_t, page_id, parent_id, nEntry,
            disk_manager, key_t, str_len, isInit), value_page_id_(value_page_id)
    {
        values_ = new uint32_t[BTNodeKeySize];
        std::memset(values_, 0, BTNodeKeySize * sizeof(uint32_t));
        if (isInit) // new
        {
            PageInitInfo info;
            info.page_t = page_t_t::VALUE;
            info.parent_id = this->get_page_id();
            value_page_ = static_cast<ValuePage*>(buffer_pool->NewPage(info));
            value_page_id_ = value_page_->get_page_id();
            //buffer_pool->DeletePage(value_page_id_);
            // now value_page has excatly *** 1 ref count ***.
            write_int(data_ + offset::VALUE_PAGE_ID, value_page_id_);
            value_page_->set_dirty();
            set_dirty();
        }
        else // exist
        {
            value_page_ = static_cast<ValuePage*>(buffer_pool->FetchPage(value_page_id_));
            //buffer_pool->DeletePage(value_page_id_);
            // now value_page has excatly *** 1 ref count ***.
            set_dirty();
        }
    }

    RootPage::~RootPage() {
        value_page_->unref(); // ref 1->0
        delete[] values_;
    }

    void RootPage::read_value(uint32_t index, ValueEntry& vEntry) const
    {
        value_page_->read_content(values_[index], vEntry);
        if (vEntry.value_state_ != value_state::INUSED)
            debug::ERROR_LOG("`RootPage::read_value()` read corrupted value.");
    }

    void RootPage::insert_value(uint32_t index, const ValueEntry& vEntry)
    {
        values_[index] = value_page_->write_content(vEntry);
        set_dirty();
    }

    void RootPage::erase_value(uint32_t index) {
        value_page_->erase_block(values_[index]);
    }

    void RootPage::update_value(uint32_t index, const ValueEntry& vEntry) {
        value_page_->update_content(values_[index], vEntry);
    }

    void RootPage::set_left_leaf(page_id_t previous_page_id) { previous_page_id_ = previous_page_id; }
    void RootPage::set_right_leaf(page_id_t next_page_id) { next_page_id_ = next_page_id; }
    page_id_t RootPage::get_left_leaf() const { return previous_page_id_; }
    page_id_t RootPage::get_right_leaf() const { return next_page_id_; }

    void RootPage::update_data()
    {
        if (page_t_ == page_t_t::ROOT_INTERNAL) {
            InternalPage::update_data();
        }
        else { // ROOT_LEAF
            write_int(data_ + offset::PAGE_T, static_cast<uint32_t>(page_t_));
            write_int(data_ + offset::PAGE_ID, page_id_);
            write_int(data_ + offset::PARENT_PAGE_ID, parent_id_);
            write_int(data_ + offset::NENTRY, nEntry_);
            write_short(data_ + offset::KEY_T, static_cast<uint16_t>(key_t_));
            write_short(data_ + offset::STR_LEN, str_len_);
            write_int(data_ + offset::VALUE_PAGE_ID, value_page_id_);
            write_int(data_ + offset::PREVIOUS_PAGE_ID, previous_page_id_);
            write_int(data_ + offset::NEXT_PAGE_ID, next_page_id_);
            for (uint32_t i = 0; i < nEntry_; i++) {
                write_int(data_ + offset::KV_START + 8 * i,
                    keys_[i]);
                write_int(data_ + offset::KV_START + 8 * i + 4,
                    values_[i]);
            }
        }
    }


} // end namespace DB::page