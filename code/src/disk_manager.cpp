#include "include/disk_manager.h"
#include "include/debug_log.h"
#include <cstring>
#include <mutex>

namespace DB::disk
{

    DiskManager::DiskManager()
        :
        dn_init_(false),
        cur_page_no_(page::NOT_A_PAGE),
        file_name_(db_name),
        log_name_(log_name),
        log_size_(0)
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
            std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
        // directory or file does not exist
        if (!log_io_.is_open()) {
            log_io_.clear();
            // create a new file
            log_io_.open(log_name,
                std::ios::binary | std::ios::trunc | std::ios::app | std::ios::out);
            log_io_.close();
            // reopen with original mode
            log_io_.open(log_name,
                std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
        }

    }


    DiskManager::~DiskManager()
    {
        db_io_.close();
        log_io_.close();
    }


    page_id_t DiskManager::get_cut_page_id() const { return cur_page_no_.load(); }

    void DiskManager::set_cur_page_id(page_id_t page_id) { cur_page_no_ = page_id; }

    void DiskManager::WritePage(page_id_t page_id, const char(&page_data)[page::PAGE_SIZE])
    {
        db_io_.seekp(page_id * PAGE_SIZE, std::ios_base::beg);
        db_io_.write(page_data, PAGE_SIZE);
        if (db_io_.bad())
        {
            debug::ERROR_LOG("I/O writing error in \"%s\", page_id: %d\n",
                file_name_.c_str(), page_id);
            return;
        }
        db_io_.flush();
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
                debug::ERROR_LOG("Read less than PAGE_SIZE in \"%s\", page_id: %d\n",
                    file_name_.c_str(), page_id);
                std::memset(page_data + read_count, 0, PAGE_SIZE - read_count);
            }
        };

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


    void DiskManager::WriteLog(char *log_data, uint32_t size)
    {
        log_io_.write(log_data, size);
        if (db_io_.bad())
        {
            debug::ERROR_LOG("Log writing error in \"%s\"\n",
                file_name_.c_str());
            return;
        }
        log_io_.flush();
        log_size_ += size;
    }


    bool DiskManager::ReadLog(char *log_data, uint32_t offset, uint32_t size)
    {
        log_io_.seekg(offset);
        log_io_.read(log_data, size);
        // if log file ends before reading "size"
        const uint32_t read_count = log_io_.gcount();
        if (read_count < size) {
            log_io_.clear();
            memset(log_data + read_count, 0, size - read_count);
        }
        log_io_.seekp(0, std::ios_base::end);
        return true;
    }

    log_state_t DiskManager::check_log()
    {
        // TODO: 
        return log_state_t::OK;

    }


    page_id_t DiskManager::AllocatePage() { return ++cur_page_no_; }


    uint32_t DiskManager::log_size() const { return log_size_; }



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


} // end namespace DB::disk