#include "include/disk_manager.h"
#include "include/debug_log.h"
#include "include/vm.h"
#include "include/page.h"
#include <cstring>
#include <mutex>
#include <random>
#include <string>
#include <set>

namespace DB::disk
{

    DiskManager::DiskManager()
        :
        dn_init_(false),
        cur_page_no_(page::NOT_A_PAGE),
        file_name_(db_name),
        log_name_(log_name),
        next_free_page_id_(page::NOT_A_PAGE)
    {
        db_io_.open(db_name,
            std::ios::binary | std::ios::in | std::ios::out);
        // directory or file does not exist
        if (!db_io_.is_open()) {
            dn_init_ = true;
            db_io_.clear();
            // create a new file
            db_io_.open(db_name,
                std::ios::binary | std::ios::trunc | std::ios::out);
            db_io_.close();
            // reopen with original mode
            db_io_.open(db_name,
                std::ios::binary | std::ios::in | std::ios::out);
        }

        log_io_.open(log_name,
            std::ios::binary | std::ios::in | std::ios::out);
        // directory or file does not exist
        if (!log_io_.is_open()) {
            log_io_.clear();
            // create a new file
            log_io_.open(log_name,
                std::ios::binary | std::ios::trunc | std::ios::out);
            log_io_.close();
            // reopen with original mode
            log_io_.open(log_name,
                std::ios::binary | std::ios::in | std::ios::out);
        }

    }


    DiskManager::~DiskManager()
    {
        db_io_.close();
        log_io_.close();
    }

    void DiskManager::set_vm(vm::VM* vm) { vm_ = vm; }

    page_id_t DiskManager::get_cut_page_id() const { return cur_page_no_.load(); }

    void DiskManager::set_cur_page_id(page_id_t page_id) { cur_page_no_ = page_id; }

    void DiskManager::init_set_next_free_page_id(page_id_t next_free_page_id) {
        next_free_page_id_ = next_free_page_id;
    }

    page_id_t DiskManager::set_next_free_page_id(page_id_t next_free_page_id)
    {
#ifdef PAGE_GC
        page_id_t previous_free_head;
        while (next_free_page_id_lock_.test_and_set(std::memory_order_acquire))
            ;
        previous_free_head = next_free_page_id_;
        next_free_page_id_ = next_free_page_id;
        vm_->set_next_free_page_id(next_free_page_id);
        next_free_page_id_lock_.clear(std::memory_order_release);
        debug::DEBUG_LOG(debug::PAGE_GC,
                         "[PAGE_GC] add [next_free_page_id=%d] -> [previous_free_head=%d]\n",
                         next_free_page_id, previous_free_head);
        return previous_free_head;
#else
        return page::NOT_A_PAGE;
#endif
    }

    void DiskManager::WritePage(page_id_t page_id, const char(&page_data)[page::PAGE_SIZE])
    {
        debug::DEBUG_LOG(debug::PAGE_WRITE,
                         "[PAGE_WRITE] DiskManager::WritePage() [page_t=%s] [page_id=%d]\n",
                         page::page_t_str[static_cast<uint32_t>(page::get_page_t(page_data))], page_id);
        db_io_.seekp(page_id * PAGE_SIZE, std::ios_base::beg);
        db_io_.write(page_data, PAGE_SIZE);
        if (db_io_.bad())
        {
            debug::ERROR_LOG("[PAGE_WRITE] DiskManager::WritePage() write error in \"%s\", page_id: %d\n",
                file_name_.c_str(), page_id);
            return;
        }
        db_io_.flush();
        if (db_io_.bad())
        {
            debug::ERROR_LOG("[PAGE_WRITE] DiskManager::WritePage() flush error in \"%s\", page_id: %d\n",
                file_name_.c_str(), page_id);
            return;
        }
    }


    // Concurrency: maybe wait until the page has been flushed.
    bool DiskManager::ReadPage(page_id_t page_id, char(&page_data)[page::PAGE_SIZE])
    {
        auto doReadPage = [this](page_id_t page_id, char(&page_data)[page::PAGE_SIZE])
        {
            db_io_.seekg(page_id * PAGE_SIZE, std::ios_base::beg);
            db_io_.read(page_data, PAGE_SIZE);
            const uint32_t read_count = db_io_.gcount();
            if (read_count < PAGE_SIZE)
            {
                debug::ERROR_LOG("DiskManager::ReadPage() read error in \"%s\" [page_id=%d] [read_count=%d]\n",
                    file_name_.c_str(), page_id, read_count);
                std::memset(page_data + read_count, 0, PAGE_SIZE - read_count);
            }
        };

        debug::DEBUG_LOG(debug::PAGE_READ,
                         "[PAGE_READ] [page_id=%d]\n",
                         page_id);

        doReadPage(page_id, page_data);
        return true;


        /*
        // if the page is not in bufferPool but resides in memory,
        // the page has been in dirty_set at current time.
        if (!is_dirty(page_id)) {
            doReadPage(page_id, page_data);
            return true;
        }

        // maybe wait until the page has been flushed.
        // UNDONE: what if now the buffer has the page ???
        const uint32_t bucket_no = hash(page_id);
        std::unique_lock<std::mutex> wait_ulk{ wait_sets_mtx_[bucket_no] };
        std::unordered_map<page_id_t, WaitInfo*>& wait_set = wait_sets_[bucket_no];
        auto it = wait_sets_->find(page_id);
        WaitInfo* wait_info;
        if (it == wait_sets_->end())
            wait_info = wait_sets_->insert({ page_id, new WaitInfo{} }).first->second;
        else {
            wait_info = it->second;
            wait_info->wait_count_++;
        }

        wait_ulk.unlock();

        std::unique_lock<std::shared_mutex> dirty_ulk{ dirty_page_sets_mtx_[bucket_no] };
        while (wait_set.count(page_id))
            //wait_info->cv_.wait(dirty_ulk);

        */
    }


    page_id_t DiskManager::AllocatePage()
    {
#ifdef PAGE_GC
        bool allocate_free = false;
        page_id_t free_page_id;
        while (next_free_page_id_lock_.test_and_set(std::memory_order_acquire))
            ;
        if (next_free_page_id_ != page::NOT_A_PAGE) {
            allocate_free = true;
            free_page_id = next_free_page_id_;
            // read next free page
            // NB: free page must be not in buffer pool,
            //     since the becoming free page is still not flush,
            //     after flush, the free page is not is buffer pool.
            char buffer[4];
            db_io_.seekg(next_free_page_id_ * PAGE_SIZE + page::offset::FREE_PAGE_ID,
                std::ios_base::beg);
            db_io_.read(buffer, sizeof(uint32_t));

            next_free_page_id_ = page::read_int(buffer);
            debug::DEBUG_LOG(debug::PAGE_GC,
                             "[PAGE_GC] reuse [free_page_id=%d] -> [next_free_page_id=%d]\n",
                             free_page_id, next_free_page_id_);
            vm_->set_next_free_page_id(next_free_page_id_);
        }
        next_free_page_id_lock_.clear(std::memory_order_release);

        if (allocate_free)
            return free_page_id;
        else
#endif // #ifdef PAGE_GC
            return ++cur_page_no_;
    }



    uint32_t DiskManager::hash(page_id_t page_id) const noexcept {
        return page_id & (dirty_hash_bucket - 1);
    }

    bool DiskManager::is_dirty(page_id_t page_id) const {
        const uint32_t bucket_no = hash(page_id);
        std::shared_lock<std::shared_mutex> slk{ dirty_page_sets_mtx_[bucket_no] };
        return dirty_page_sets_[bucket_no].count(page_id);
    }

    void DiskManager::set_dirty(page_id_t page_id) {
        const uint32_t bucket_no = hash(page_id);
        {
            std::shared_lock<std::shared_mutex> slk{ dirty_page_sets_mtx_[bucket_no] };
            if (dirty_page_sets_[bucket_no].count(page_id))
                return;
        }
        std::lock_guard<std::shared_mutex> lg{ dirty_page_sets_mtx_[bucket_no] };
        dirty_page_sets_[bucket_no].insert(page_id);
    }


    //
    // log protocol
    //      if nuance & check-sum fail to match, the state is regarded as OK.
    //      if match, we should undo / redo.
    //
    uint32_t checksum(const char(&buffer)[page::PAGE_SIZE]);
    uint32_t checksum(const char* buffer, const uint32_t len);
    static constexpr uint32_t sql_max_len = 200;
    struct log_offset {
        static const uint32_t

            SQL = 0,

            NUANCE = PAGE_SIZE - 24,

            NUANCE_PLUS_ONE = PAGE_SIZE - 20,

            UNDO_LOG_NUM = PAGE_SIZE - 16,
            UNDO_CHECK = PAGE_SIZE - 12,

            REDO_SQL_LEN = PAGE_SIZE - 8,
            REDO_CHECK = PAGE_SIZE - 4,

            ZERO = 0;
    };




    log_state_t DiskManager::check_log_state()
    {
        char int_buffer[4] = { 0 };
        log_io_.seekg(log_offset::NUANCE, std::ios_base::beg);
        log_io_.read(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("check log state error on NUANCE\n");
            return log_state_t::CORRUPTION;
        }
        const uint32_t nuance = page::read_int(int_buffer);

        log_io_.seekg(log_offset::NUANCE_PLUS_ONE, std::ios_base::beg);
        log_io_.read(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("check log state error on NUANCE_PLUS_ONE\n");
            return log_state_t::CORRUPTION;
        }

        if (nuance == 0 || nuance + 1 != page::read_int(int_buffer)) // fail to match
            return log_state_t::OK;

        // do check, if fail to match, return OK
        bool undo = false;
        bool redo = false;


        log_io_.seekg(log_offset::UNDO_LOG_NUM, std::ios_base::beg);
        log_io_.read(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("check log state error on UNDO_LOG_NUM\n");
            return log_state_t::CORRUPTION;
        }
        const uint32_t undo_log_num = page::read_int(int_buffer);


        log_io_.seekg(log_offset::UNDO_CHECK, std::ios_base::beg);
        log_io_.read(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("check log state error on UNDO_CHECK\n");
            return log_state_t::CORRUPTION;
        }
        const uint32_t undo_check = page::read_int(int_buffer);


        log_io_.seekg(log_offset::REDO_SQL_LEN, std::ios_base::beg);
        log_io_.read(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("check log state error on REDO_SQL_LEN\n");
            return log_state_t::CORRUPTION;
        }
        const uint32_t redo_sql_len = page::read_int(int_buffer);


        log_io_.seekg(log_offset::REDO_CHECK, std::ios_base::beg);
        log_io_.read(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("check log state error on REDO_CHECK\n");
            return log_state_t::CORRUPTION;
        }
        const uint32_t redo_check = page::read_int(int_buffer);

        // check redo
        char sql_buffer[sql_max_len] = { 0 };
        log_io_.seekg(log_offset::SQL, std::ios_base::beg);
        log_io_.read(sql_buffer, 4);
        if (redo_sql_len != 0)
            if (redo_check == checksum(sql_buffer, redo_sql_len) + nuance)
                redo = true;

        // check undo
        char page_data[page::PAGE_SIZE] = { 0 };
        uint32_t check_sum = 0;
        for (uint32_t i = 1; i <= undo_log_num; i++) {
            log_io_.seekg(i*page::PAGE_SIZE, std::ios_base::beg);
            log_io_.read(page_data, page::PAGE_SIZE);
            check_sum += checksum(page_data);
        }
        if (undo_check == check_sum + nuance) // check
            undo = true;

        if (undo) {
            if (redo && redo_sql_len == 0)
                return log_state_t::UNDO;
            else {
                if (redo) {
                    vm_->send_reply_sql(std::string(sql_buffer, redo_sql_len));
                    return log_state_t::REDO;
                }
                else
                    return log_state_t::CORRUPTION;
            }
        }
        else
            return log_state_t::OK;

    } // check_log_state();


    void DiskManager::replay_log(disk::log_state_t)
    {
        char int_buffer[4] = { 0 };
        log_io_.seekg(log_offset::UNDO_LOG_NUM, std::ios_base::beg);
        log_io_.read(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("check log state error on UNDO_LOG_NUM\n");
            return;
        }
        const uint32_t undo_log_num = page::read_int(int_buffer);

        char page_data[page::PAGE_SIZE] = { 0 };
        for (uint32_t i = 1; i <= undo_log_num; i++) {
            log_io_.seekg(1 * page::PAGE_SIZE, std::ios_base::beg);
            log_io_.read(page_data, page::PAGE_SIZE);
            if (log_io_.bad()) {
                debug::ERROR_LOG("read undo log error\n");
                return;
            }

            const page_id_t page_id = page::read_int(page_data + page::offset::FREE_PAGE_ID);

            db_io_.seekp(page_id * PAGE_SIZE, std::ios_base::beg);
            db_io_.write(page_data, PAGE_SIZE);
            if (db_io_.bad()) {
                debug::ERROR_LOG("[RECOVERY] DiskManager::replay_log() write error in undo, page_id: %d\n", page_id);
                return;
            }
            db_io_.flush();
            if (db_io_.bad()) {
                debug::ERROR_LOG("[RECOVERY] DiskManager::replay_log() flush error in undo, page_id: %d\n", page_id);
                return;
            }
        }
    }


    void DiskManager::doWAL(const page_id_t prev_last_page_id, const std::string& sql)
    {
        debug::DEBUG_LOG(debug::WAL,
                         "[WAL] [prev_last_page_id=%d] [sql=\"%s\"]\n",
                         prev_last_page_id, sql.c_str());

        page_id_t cur_page_id = 0;
        char buffer[PAGE_SIZE] = { 0 };
        uint32_t undo_check = 0;
        std::set<page_id_t> ditry_pages;

        // no need to record undo log when init DB
        if (prev_last_page_id != page::NOT_A_PAGE) {
            for (uint32_t i = 0; i < dirty_hash_bucket; i++) {
                std::unordered_set<page_id_t>& dirty_page_set = dirty_page_sets_[i];
                for (page_id_t ditry_page : dirty_page_set)
                    if (ditry_page <= prev_last_page_id)
                        ditry_pages.insert(ditry_page);
                dirty_page_set.clear();
            }
        }

        for (const page_id_t db_page_id : ditry_pages)
        {

            cur_page_id++;

            // HACK: does below disk seekg/seekp happen to the detriment of I/O performance
            // read old db page for undo
            db_io_.seekg(db_page_id * PAGE_SIZE, std::ios_base::beg);
            db_io_.read(buffer, PAGE_SIZE);
            const uint32_t read_count = db_io_.gcount();
            if (read_count < PAGE_SIZE)
            {
                debug::ERROR_LOG("DiskManager::doWAL() read error in \"%s\" [page_id=%d] [read_count=%d]\n",
                    file_name_.c_str(), db_page_id, read_count);
                return;
            }

            undo_check += checksum(buffer);

            // write log
            log_io_.seekp(cur_page_id * PAGE_SIZE, std::ios_base::beg);
            log_io_.write(buffer, PAGE_SIZE);
            if (log_io_.bad()) {
                debug::ERROR_LOG("[WAL] write error in \"%s\", page_id: %d\n",
                    log_name_.c_str(), cur_page_id);
                return;
            }
            log_io_.flush();
            if (log_io_.bad()) {
                debug::ERROR_LOG("[WAL] flush error in \"%s\", page_id: %d\n",
                    log_name_.c_str(), cur_page_id);
                return;
            }
        }

        // nuance
        // undo_log_num
        // undo_check
        // redo_sql_len
        // redo_check

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> dist(0x23333333, 0xffffffff);

        // nuance
        const uint32_t nuance = dist(rng);

        // undo_log_num
        const uint32_t undo_log_num = cur_page_id;

        // undo_check
        undo_check += nuance;

        // redo_sql_len
        uint32_t redo_sql_len = sql.size();
        if (redo_sql_len > sql_max_len)
            redo_sql_len = 0;

        // redo_check
        const uint32_t redo_check = checksum(sql.c_str(), redo_sql_len) + nuance;

        char int_buffer[4] = { 0 };


        /* [depricated]
        log_io_.seekp(log_offset::PREV_LAST_PAGE_ID, std::ios_base::beg);
        page::write_int(int_buffer, prev_last_page_id);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("WAL write error on PREV_LAST_PAGE_ID\n");
            return;
        }
        log_io_.flush();
        */

        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // write nuance as 0
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        log_io_.seekp(log_offset::NUANCE, std::ios_base::beg);
        page::write_int(int_buffer, 0); // here write NUANCE as 0 !!!
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] write error on FAKE NUANCE\n");
            return;
        }
        log_io_.flush();
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] flush error on FAKE NUANCE\n");
            return;
        }


        log_io_.seekp(log_offset::NUANCE_PLUS_ONE, std::ios_base::beg);
        page::write_int(int_buffer, nuance + 1);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] write error on NUANCE_PLUS_ONE\n");
            return;
        }
        log_io_.flush();
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] flush error on NUANCE_PLUS_ONE\n");
            return;
        }


        log_io_.seekp(log_offset::UNDO_LOG_NUM, std::ios_base::beg);
        page::write_int(int_buffer, undo_log_num);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] write error on UNDO_LOG_NUM\n");
            return;
        }
        log_io_.flush();
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] flush error on UNDO_LOG_NUM\n");
            return;
        }


        log_io_.seekp(log_offset::UNDO_CHECK, std::ios_base::beg);
        page::write_int(int_buffer, undo_check);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] write error on UNDO_CHECK\n");
            return;
        }
        log_io_.flush();
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] flush error on UNDO_CHECK\n");
            return;
        }


        if (redo_sql_len > 0) {
            log_io_.seekp(log_offset::SQL, std::ios_base::beg);
            log_io_.write(sql.c_str(), redo_sql_len);
            if (log_io_.bad()) {
                debug::ERROR_LOG("[WAL] write error on SQL\n");
                return;
            }
            log_io_.flush();
            if (log_io_.bad()) {
                debug::ERROR_LOG("[WAL] flush error on SQL\n");
                return;
            }
        }


        log_io_.seekp(log_offset::REDO_SQL_LEN, std::ios_base::beg);
        page::write_int(int_buffer, redo_sql_len);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] write error on REDO_SQL_LEN\n");
            return;
        }
        log_io_.flush();
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] flush error on REDO_SQL_LEN\n");
            return;
        }


        log_io_.seekp(log_offset::REDO_CHECK, std::ios_base::beg);
        page::write_int(int_buffer, redo_check);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] write error on REDO_CHECK\n");
            return;
        }
        log_io_.flush();
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] flush error on REDO_CHECK\n");
            return;
        }


        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // notice that maybe crash during this write
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        log_io_.seekp(log_offset::NUANCE, std::ios_base::beg);
        page::write_int(int_buffer, nuance);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] write error on NUANCE\n");
            return;
        }
        log_io_.flush();
        if (log_io_.bad()) {
            debug::ERROR_LOG("[WAL] flush error on NUANCE\n");
            return;
        }


    } // end doWAL();


    void DiskManager::detroy_log() {
        // write 0 into NUANCE
        char int_buffer[4];
        log_io_.seekp(log_offset::NUANCE, std::ios_base::beg);
        page::write_int(int_buffer, 0);
        log_io_.write(int_buffer, 4);
        if (log_io_.bad()) {
            debug::ERROR_LOG("[DESTROY_LOG] write error on NUANCE = 0\n");
            return;
        }
        log_io_.flush();
    }


    //
    // util
    //

    uint32_t checksum(const char(&buffer)[page::PAGE_SIZE]) {
        uint32_t check_sum = 0;
        for (uint32_t i = 0; i < PAGE_SIZE / 4; i++) {
            check_sum +=
                static_cast<uint32_t>(buffer[4 * i]) +
                (static_cast<uint32_t>(buffer[4 * i + 1])) << 8 +
                (static_cast<uint32_t>(buffer[4 * i + 2])) << 16 +
                (static_cast<uint32_t>(buffer[4 * i + 3])) << 24;
        }
        return check_sum;
    }

    uint32_t checksum(const char* buffer, const uint32_t len) {
        uint32_t check_sum = 0;
        for (uint32_t i = 0; i < len; i++)
            check_sum += static_cast<uint32_t>(buffer[i]) << (i % 4);
        return check_sum;
    }


} // end namespace DB::disk