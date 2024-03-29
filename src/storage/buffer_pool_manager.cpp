/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    if (!free_list_.empty())
    {
        // If there are free pages available, take one from the free list
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    else
    {
        return replacer_->victim(frame_id);
    }
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    std::scoped_lock lock(latch_);
    if (page->is_dirty())
    {
        // If the page is dirty, write it back to the disk
        flush_page(page->get_page_id());
        page->is_dirty_ = false;
    }
    // Update the page's metadata
    page->id_ = new_page_id;
    page_table_[new_page_id] = new_frame_id;
    page->reset_memory();

}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    std::lock_guard<std::mutex> latch_guard(latch_);

    auto it = page_table_.find(page_id);
    if (it != page_table_.end())
    {
        // Page exists in the buffer pool, pin it
        frame_id_t frame_id = it->second;
        pages_[frame_id].pin_count_++;
        return &pages_[frame_id];
    }

    frame_id_t frame_id;
    if (!find_victim_page(&frame_id))
    {
        return nullptr; // No available pages
    }

    Page &victim_page = pages_[frame_id];
    if (victim_page.is_dirty())
    {
        flush_page(victim_page.get_page_id());
        victim_page.is_dirty_ = false;
    }

    // Delete R from the page table and insert P
    page_table_.erase(victim_page.get_page_id());
    page_table_[page_id] = frame_id;

    // Update P's metadata
    Page &newPage = victim_page;
    victim_page.id_ = page_id;
    disk_manager_->read_page(page_id.fd, page_id.page_no, newPage.data_, PAGE_SIZE);
    victim_page.is_dirty_ = false;
    replacer_->pin(frame_id);

    return &newPage;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    std::lock_guard<std::mutex> latch_guard(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        // Page not found in the page table

        return true;
    }

    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];

    if (page.pin_count_ > 0)
    {
        page.pin_count_--;
        if (is_dirty)
        {
            page.is_dirty_ = true;
        }
        return true;
    }

    // Check if the page can be evicted (pin_count <= 0)
    if (page.pin_count_ <= 0)
    {
        if (is_dirty)
        {
            disk_manager_->write_page(page_id.fd, page_id.page_no, page.data_, PAGE_SIZE);
        }

        // Remove the page from the page table
        page_table_.erase(page_id);

        // Mark the frame as free
        free_list_.push_back(frame_id);
    }
    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    std::lock_guard<std::mutex> latch_guard(latch_);

    auto it = page_table_.find(page_id);
    if (it != page_table_.end())
    {
        frame_id_t frame_id = it->second;
        Page &page = pages_[frame_id];
        disk_manager_->write_page(page_id.fd, page_id.page_no, page.data_, PAGE_SIZE);
        page.is_dirty_ = false;
        return true;
    }

    return false;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    std::lock_guard<std::mutex> latch_guard(latch_);

    page_id->page_no = disk_manager_->allocate_page(page_id->fd);
    if (page_id->page_no == INVALID_PAGE_ID)
    {
        // No new pages could be created
        return nullptr;
    }
    frame_id_t frame_id;
    if (!free_list_.empty())
    {

        frame_id = free_list_.front();
        // std::cout << " frame_id" << frame_id << std::endl;
        free_list_.pop_front();
        for (auto it = page_table_.begin(); it != page_table_.end(); it++)
        {
            if (it->second == frame_id)
            {

                it = page_table_.erase(it);
                break;
            }
        }
        
    }

    else
    {
        if (!replacer_->victim(&frame_id))
        {

            return nullptr;  // No available pages
            printf("fuck");
        }
        if (pages_[frame_id].is_dirty())
        {
            flush_page(pages_[frame_id].get_page_id());
            pages_[frame_id].is_dirty_ = false;
        }
    }

    // Update P's metadata, zero out memory, and add P to the page table
    Page &new_page = pages_[frame_id];
    new_page.id_ = *page_id;

    new_page.reset_memory();
    page_table_[*page_id] = frame_id;
    return &new_page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    std::lock_guard<std::mutex> latch_guard(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        // Page doesn't exist, consider it deleted
        return true;
    }

    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];

    if (page.pin_count_ > 0)
    {
        // Someone is using the page, can't delete it
        return false;
    }

    page_table_.erase(page_id);
    page.reset_memory();
    free_list_.push_back(frame_id);

    // Deallocate the page on disk
    disk_manager_->deallocate_page(page_id.page_no);

    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    std::lock_guard<std::mutex> latch_guard(latch_);
    for (size_t i = 0; i < pool_size_; i++)
    {
        Page *page = &pages_[i];
        if (page->get_page_id().fd == fd && page->get_page_id().page_no != INVALID_PAGE_ID)
        {
            disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}