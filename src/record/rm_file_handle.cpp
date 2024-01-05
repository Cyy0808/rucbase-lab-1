/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    auto page_handle = fetch_page_handle(rid.page_no); // Get Page Handler
    auto rec = std::make_unique<RmRecord>(file_hdr_.record_size); // That's the record.
    if(!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
      throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    memcpy(rec->data, page_handle.get_slot(rid.slot_no), file_hdr_.record_size);
    rec->size = file_hdr_.record_size;
    return rec;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    RmPageHandle page_handle = create_page_handle();
    int free_slot = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    memcpy(page_handle.get_slot(free_slot), buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, free_slot);
    // If full, update the first_free_page_no.
    if(++page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    return Rid{page_handle.page->get_page_id().page_no, free_slot};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    if (rid.page_no < file_hdr_.num_pages) {
        create_new_page_handle();
    }
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    Bitmap::set(pageHandle.bitmap, rid.slot_no);
    pageHandle.page_hdr->num_records++;
    if (pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
    }

    char *slot = pageHandle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->unpin_page(pageHandle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if(!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
      throw PageNotExistError("a`", rid.page_no);
    }
    // Okay, remember modifying the bitmap!
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    if(page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        page_handle.page_hdr->num_records--;
    } else {
        return;
    }
    release_page_handle(page_handle);
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    // bitmap, bitmap, bitmap!
    if(!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw PageNotExistError("a`", rid.page_no);
    }
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    if(page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("a`", page_no);
    }
    return RmPageHandle(&file_hdr_, this->buffer_pool_manager_->fetch_page({this->fd_, page_no}));
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    PageId page_id = {this->fd_, INVALID_PAGE_ID}; // 0 or this->fd_, Not sure.
    Page* neo_page = this->buffer_pool_manager_->new_page(&page_id);
    auto page_handle = RmPageHandle(&file_hdr_, neo_page);
    if(neo_page != nullptr) {
        RmPageHandle page_handle(&file_hdr_, neo_page);
        page_handle.page_hdr->num_records = 0;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
        file_hdr_.num_pages++;
        file_hdr_.first_free_page_no = page_id.page_no;
    }
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    if(this->file_hdr_.first_free_page_no == RM_NO_PAGE){
        return create_new_page_handle();
    }
    return fetch_page_handle(this->file_hdr_.first_free_page_no);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    
}