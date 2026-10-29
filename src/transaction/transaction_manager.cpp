/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

#include <array>
#include <functional>
#include <utility>

namespace {

constexpr size_t kTableVersionCacheSize = 8;

using rmdb::transaction_page_state::BumpVisibilityEpoch;
using rmdb::transaction_page_state::ClearDirtySlotIfEmpty;
using rmdb::transaction_page_state::MarkDirtySlot;
using rmdb::transaction_page_state::AddActiveMeta;
using rmdb::transaction_page_state::AddDeletedMeta;
using rmdb::transaction_page_state::AddUncommittedMeta;
using rmdb::transaction_page_state::AddVersionLink;
using rmdb::transaction_page_state::AdvanceMaxCommittedMetaTs;
using rmdb::transaction_page_state::RemoveActiveMeta;
using rmdb::transaction_page_state::RemoveDeletedMeta;
using rmdb::transaction_page_state::RemoveUncommittedMeta;
using rmdb::transaction_page_state::RemoveVersionLink;

struct TableVersionCacheEntry {
    TransactionManager *manager{nullptr};
    rmdb::u64 manager_id{0};
    rmdb::u64 epoch{0};
    std::string table_name;
    std::shared_ptr<TransactionManager::TableVersionInfo> table_info;
};

thread_local std::array<TableVersionCacheEntry, kTableVersionCacheSize> table_version_cache;

TableVersionCacheEntry &table_version_cache_entry(const std::string &table_name) {
    return table_version_cache[std::hash<std::string>{}(table_name) & (kTableVersionCacheSize - 1)];
}

void init_page_dirty_slots(SmManager *sm_manager, const std::string &tab_name,
                           const std::shared_ptr<TransactionManager::PageVersionInfo> &page_info) {
    if (sm_manager == nullptr || page_info == nullptr) {
        return;
    }
    auto fh_iter = sm_manager->fhs_.find(tab_name);
    if (fh_iter == sm_manager->fhs_.end() || fh_iter->second == nullptr) {
        return;
    }
    int slot_count = fh_iter->second->get_file_hdr().num_records_per_page;
    if (slot_count <= 0) {
        return;
    }
    page_info->InitDirtySlots(static_cast<rmdb::u32>(slot_count));
}
}

bool TransactionManager::UpdateTupleMeta(const std::string &tab_name, Rid rid, std::optional<TupleMeta> meta,
                                         std::function<bool(std::optional<TupleMeta>)> &&check) {
    auto table_info = GetOrCreateTableVersionInfo(tab_name);
    std::shared_ptr<PageVersionInfo> page_info = GetOrCreatePageVersionInfoOnTable(table_info, tab_name, rid.page_no);

    std::unique_lock<std::shared_mutex> page_lock(page_info->mutex_);
    slot_offset_t slot = static_cast<slot_offset_t>(rid.slot_no);
    if (rid.slot_no < 0 || !page_info->CanTrackSlot(slot)) {
        throw InternalError("Tuple metadata slot is out of range");
    }
    std::optional<TupleMeta> current_meta = std::nullopt;
    if (const TupleMeta *stored_meta = page_info->GetTupleMeta(slot); stored_meta != nullptr) {
        current_meta = *stored_meta;
    }
    if (check != nullptr && !check(current_meta)) {
        return false;
    }
    const bool was_deleted = current_meta.has_value() && current_meta->is_deleted_;
    const bool will_be_deleted = meta.has_value() && meta->is_deleted_;
    const bool was_uncommitted = current_meta.has_value() && current_meta->ts_ >= TXN_START_ID;
    const bool will_be_uncommitted = meta.has_value() && meta->ts_ >= TXN_START_ID;
    bool changed = current_meta.has_value() != meta.has_value();
    if (!changed && current_meta.has_value() && meta.has_value()) {
        changed = current_meta->ts_ != meta->ts_ || current_meta->is_deleted_ != meta->is_deleted_;
    }
    if (changed) {
        BumpVisibilityEpoch(page_info);
    }
    if (!current_meta.has_value() && meta.has_value()) {
        AddActiveMeta(page_info);
    } else if (current_meta.has_value() && !meta.has_value()) {
        RemoveActiveMeta(page_info);
    }
    if (!was_uncommitted && will_be_uncommitted) {
        AddUncommittedMeta(page_info);
    } else if (was_uncommitted && !will_be_uncommitted) {
        RemoveUncommittedMeta(page_info);
    }
    if (meta.has_value() && meta->ts_ < TXN_START_ID) {
        AdvanceMaxCommittedMetaTs(page_info, meta->ts_);
    }
    if (!was_deleted && will_be_deleted) {
        AddDeletedMeta(page_info);
    }
    if (meta.has_value()) {
        MarkDirtySlot(table_info, page_info, rid);
        page_info->SetTupleMeta(slot, *meta);
    } else {
        page_info->ClearTupleMeta(slot);
        ClearDirtySlotIfEmpty(table_info, page_info, rid);
        page_info->ReleaseTupleMetaStorageIfEmpty();
    }
    if (was_deleted && !will_be_deleted) {
        RemoveDeletedMeta(page_info);
    }
    if (changed) {
        BumpVisibilityEpoch(page_info);
    }
    return true;
}

std::optional<TupleMeta> TransactionManager::GetTupleMeta(const std::string &tab_name, Rid rid) {
    auto table_info = GetTableVersionInfo(tab_name);
    auto page_info = GetPageVersionInfoOnTable(table_info, rid.page_no);
    if (page_info == nullptr) {
        return std::nullopt;
    }

    std::shared_lock<std::shared_mutex> page_lock(page_info->mutex_);
    if (rid.slot_no < 0) {
        return std::nullopt;
    }
    const TupleMeta *meta = page_info->GetTupleMeta(static_cast<slot_offset_t>(rid.slot_no));
    return meta == nullptr ? std::nullopt : std::optional<TupleMeta>(*meta);
}

TupleMeta TransactionManager::GetTupleMetaOrDefault(const std::string &tab_name, Rid rid) {
    auto meta = GetTupleMeta(tab_name, rid);
    if (meta.has_value()) {
        return *meta;
    }
    return TupleMeta{0, false};
}

TransactionManager::TupleMutationState TransactionManager::GetTupleMutationState(
    const std::string &tab_name, Rid rid) {
    TupleMutationState state;
    auto table_info = GetTableVersionInfo(tab_name);
    auto page_info = GetPageVersionInfoOnTable(table_info, rid.page_no);
    if (page_info == nullptr || rid.slot_no < 0) {
        return state;
    }
    std::shared_lock<std::shared_mutex> page_lock(page_info->mutex_);
    const auto slot = static_cast<slot_offset_t>(rid.slot_no);
    if (const TupleMeta *meta = page_info->GetTupleMeta(slot); meta != nullptr) {
        state.meta = *meta;
    }
    if (const VersionUndoLink *version = page_info->GetVersion(slot); version != nullptr) {
        state.version = *version;
    }
    return state;
}

std::shared_ptr<TransactionManager::PageVersionInfo> TransactionManager::GetPageVersionInfo(
    const std::string &tab_name, page_id_t page_no) {
    return GetPageVersionInfoOnTable(GetTableVersionInfo(tab_name), page_no);
}

std::shared_ptr<TransactionManager::TableVersionInfo> TransactionManager::GetTableVersionInfo(
    const std::string &tab_name) {
    rmdb::u64 epoch = version_state_.epoch.load(std::memory_order_acquire);
    auto &cached = table_version_cache_entry(tab_name);
    if (cached.manager == this && cached.manager_id == version_state_.cache_id && cached.epoch == epoch &&
        cached.table_name == tab_name) {
        return cached.table_info;
    }

    std::shared_lock<std::shared_mutex> version_lock(version_state_.mutex);
    auto table_iter = version_state_.tables.find(tab_name);
    std::shared_ptr<TableVersionInfo> table_info =
        table_iter == version_state_.tables.end() ? nullptr : table_iter->second;
    cached = TableVersionCacheEntry{this, version_state_.cache_id, epoch, tab_name, table_info};
    return table_info;
}

std::shared_ptr<TransactionManager::TableVersionInfo> TransactionManager::GetOrCreateTableVersionInfo(
    const std::string &tab_name) {
    rmdb::u64 epoch = version_state_.epoch.load(std::memory_order_acquire);
    auto &cached = table_version_cache_entry(tab_name);
    if (cached.manager == this && cached.manager_id == version_state_.cache_id && cached.epoch == epoch &&
        cached.table_name == tab_name && cached.table_info != nullptr) {
        return cached.table_info;
    }
    {
        std::shared_lock<std::shared_mutex> version_lock(version_state_.mutex);
        auto table_iter = version_state_.tables.find(tab_name);
        if (table_iter != version_state_.tables.end() && table_iter->second != nullptr) {
            cached = TableVersionCacheEntry{
                this, version_state_.cache_id, epoch, tab_name, table_iter->second};
            return table_iter->second;
        }
    }
    std::unique_lock<std::shared_mutex> version_lock(version_state_.mutex);
    auto &table_info = version_state_.tables[tab_name];
    if (table_info == nullptr) {
        table_info = std::make_shared<TableVersionInfo>();
        epoch = version_state_.epoch.fetch_add(1, std::memory_order_acq_rel) + 1;
    }
    cached = TableVersionCacheEntry{this, version_state_.cache_id, epoch, tab_name, table_info};
    return table_info;
}

std::shared_ptr<TransactionManager::PageVersionInfo> TransactionManager::GetPageVersionInfoOnTable(
    const std::shared_ptr<TableVersionInfo> &table_info, page_id_t page_no) {
    return GetPageVersionInfoOnTable(table_info, page_no, nullptr);
}

std::shared_ptr<TransactionManager::PageVersionInfo> TransactionManager::GetPageVersionInfoOnTable(
    const std::shared_ptr<TableVersionInfo> &table_info, page_id_t page_no, rmdb::u64 *page_map_epoch) {
    if (table_info == nullptr) {
        if (page_map_epoch != nullptr) {
            *page_map_epoch = 0;
        }
        return nullptr;
    }
    if (table_info->tracks_exact_dirty_page(page_no) && !table_info->is_page_dirty_exact(page_no)) {
        if (page_map_epoch != nullptr) {
            *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
        }
        return nullptr;
    }
    if (table_info->tracks_exact_dirty_page(page_no)) {
        auto *dense_page_info = table_info->lookup_page_info_dense(page_no);
        if (dense_page_info != nullptr) {
            if (page_map_epoch != nullptr) {
                *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
            }
            return std::shared_ptr<PageVersionInfo>(table_info, dense_page_info);
        }
    }
    if (!table_info->maybe_has_page_version_info(page_no)) {
        if (page_map_epoch != nullptr) {
            *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
        }
        return nullptr;
    }
    std::shared_lock<std::shared_mutex> table_lock(table_info->mutex_);
    if (page_map_epoch != nullptr) {
        *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
    }
    auto page_iter = table_info->pages_.find(page_no);
    if (page_iter == table_info->pages_.end()) {
        return nullptr;
    }
    return page_iter->second;
}

TransactionManager::PageVersionInfo *TransactionManager::GetPageVersionInfoOnTableRaw(
    const std::shared_ptr<TableVersionInfo> &table_info, page_id_t page_no, rmdb::u64 *page_map_epoch) {
    if (table_info == nullptr) {
        if (page_map_epoch != nullptr) {
            *page_map_epoch = 0;
        }
        return nullptr;
    }
    if (table_info->tracks_exact_dirty_page(page_no) && !table_info->is_page_dirty_exact(page_no)) {
        if (page_map_epoch != nullptr) {
            *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
        }
        return nullptr;
    }
    if (table_info->tracks_exact_dirty_page(page_no)) {
        auto *dense_page_info = table_info->lookup_page_info_dense(page_no);
        if (dense_page_info != nullptr) {
            if (page_map_epoch != nullptr) {
                *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
            }
            return dense_page_info;
        }
    }
    if (!table_info->maybe_has_page_version_info(page_no)) {
        if (page_map_epoch != nullptr) {
            *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
        }
        return nullptr;
    }
    std::shared_lock<std::shared_mutex> table_lock(table_info->mutex_);
    if (page_map_epoch != nullptr) {
        *page_map_epoch = table_info->page_map_epoch_.load(std::memory_order_acquire);
    }
    auto page_iter = table_info->pages_.find(page_no);
    if (page_iter == table_info->pages_.end()) {
        return nullptr;
    }
    return page_iter->second.get();
}

std::shared_ptr<TransactionManager::PageVersionInfo> TransactionManager::GetOrCreatePageVersionInfo(
    const std::string &tab_name, page_id_t page_no) {
    auto table_info = GetOrCreateTableVersionInfo(tab_name);
    return GetOrCreatePageVersionInfoOnTable(table_info, tab_name, page_no);
}

std::shared_ptr<TransactionManager::PageVersionInfo> TransactionManager::GetOrCreatePageVersionInfoOnTable(
    const std::shared_ptr<TableVersionInfo> &table_info, const std::string &tab_name, page_id_t page_no) {
    table_info->mark_page_version_info(page_no);
    if (table_info->tracks_exact_dirty_page(page_no)) {
        if (auto *dense_page_info = table_info->lookup_page_info_dense(page_no); dense_page_info != nullptr) {
            return std::shared_ptr<PageVersionInfo>(table_info, dense_page_info);
        }
    }
    {
        std::shared_lock<std::shared_mutex> table_lock(table_info->mutex_);
        auto page_iter = table_info->pages_.find(page_no);
        if (page_iter != table_info->pages_.end() && page_iter->second != nullptr) {
            return page_iter->second;
        }
    }

    std::unique_lock<std::shared_mutex> table_lock(table_info->mutex_);
    auto &page_info = table_info->pages_[page_no];
    if (page_info == nullptr) {
        auto new_page_info = std::make_shared<PageVersionInfo>();
        init_page_dirty_slots(sm_manager_, tab_name, new_page_info);
        page_info = std::move(new_page_info);
        table_info->publish_page_info_dense(page_no, page_info.get());
        table_info->page_map_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    return page_info;
}

void TransactionManager::RetainUndoReference(const UndoLink &link) {
    if (!link.IsValid()) {
        return;
    }
    transaction_registry_.WithTransactionShared(link.prev_txn_, [&](Transaction *txn) {
        if (txn == nullptr) {
            throw InternalError("Cannot retain missing undo transaction");
        }
        txn->RetainUndoReference();
    });
}

void TransactionManager::ReleaseUndoReference(const UndoLink &link) {
    if (!link.IsValid()) {
        return;
    }
    transaction_registry_.WithTransactionShared(link.prev_txn_, [&](Transaction *txn) {
        if (txn == nullptr) {
            throw InternalError("Cannot release missing undo transaction");
        }
        txn->ReleaseUndoReference();
    });
}

bool TransactionManager::UpdateUndoLink(const std::string &tab_name, Rid rid, std::optional<UndoLink> prev_link,
                                        std::function<bool(std::optional<UndoLink>)> &&check) {
    auto table_info = GetOrCreateTableVersionInfo(tab_name);
    std::shared_ptr<PageVersionInfo> page_info = GetOrCreatePageVersionInfoOnTable(table_info, tab_name, rid.page_no);

    std::unique_lock<std::shared_mutex> page_lock(page_info->mutex_);
    slot_offset_t slot = static_cast<slot_offset_t>(rid.slot_no);
    if (rid.slot_no < 0 || !page_info->CanTrackSlot(slot)) {
        throw InternalError("Version metadata slot is out of range");
    }
    VersionUndoLink *stored_version = page_info->GetMutableVersion(slot);
    std::optional<UndoLink> current_link = std::nullopt;
    if (stored_version != nullptr && stored_version->prev_.IsValid()) {
        current_link = stored_version->prev_;
    }
    if (check != nullptr && !check(current_link)) {
        return false;
    }

    const UndoLink old_link = stored_version == nullptr ? UndoLink{} : stored_version->prev_;
    const UndoLink new_link = prev_link.has_value() ? *prev_link : UndoLink{};
    const bool link_changed = old_link != new_link;
    if (link_changed && new_link.IsValid()) {
        RetainUndoReference(new_link);
    }

    bool changed = false;
    bool mutation_succeeded = false;
    try {
        if (prev_link.has_value()) {
            changed = stored_version == nullptr || stored_version->prev_ != *prev_link;
            if (changed) {
                BumpVisibilityEpoch(page_info);
            }
            MarkDirtySlot(table_info, page_info, rid);
            if (stored_version == nullptr) {
                page_info->SetVersion(slot, VersionUndoLink{*prev_link, false});
                AddVersionLink(page_info);
            } else {
                stored_version->prev_ = *prev_link;
            }
            if (changed) {
                BumpVisibilityEpoch(page_info);
            }
            mutation_succeeded = true;
            if (link_changed && old_link.IsValid()) {
                ReleaseUndoReference(old_link);
            }
            return true;
        }

        if (stored_version == nullptr) {
            mutation_succeeded = true;
            return true;
        }
        if (stored_version->in_progress_) {
            changed = stored_version->prev_.IsValid();
            if (changed) {
                BumpVisibilityEpoch(page_info);
            }
            stored_version->prev_ = UndoLink{};
        } else {
            changed = true;
            BumpVisibilityEpoch(page_info);
            page_info->ClearVersion(slot);
            RemoveVersionLink(page_info);
            ClearDirtySlotIfEmpty(table_info, page_info, rid);
            page_info->ReleaseVersionStorageIfEmpty();
        }
        if (changed) {
            BumpVisibilityEpoch(page_info);
        }
        mutation_succeeded = true;
        if (link_changed && old_link.IsValid()) {
            ReleaseUndoReference(old_link);
        }
        return true;
    } catch (...) {
        if (!mutation_succeeded && link_changed && new_link.IsValid()) {
            ReleaseUndoReference(new_link);
        }
        throw;
    }
}

bool TransactionManager::UpdateVersionLink(const std::string &tab_name, Rid rid,
                                           std::optional<VersionUndoLink> prev_version,
                                           std::function<bool(std::optional<VersionUndoLink>)> &&check) {
    auto table_info = GetOrCreateTableVersionInfo(tab_name);
    std::shared_ptr<PageVersionInfo> page_info = GetOrCreatePageVersionInfoOnTable(table_info, tab_name, rid.page_no);

    std::unique_lock<std::shared_mutex> page_lock(page_info->mutex_);
    slot_offset_t slot = static_cast<slot_offset_t>(rid.slot_no);
    if (rid.slot_no < 0 || !page_info->CanTrackSlot(slot)) {
        throw InternalError("Version metadata slot is out of range");
    }
    std::optional<VersionUndoLink> current_version = std::nullopt;
    VersionUndoLink *stored_version = page_info->GetMutableVersion(slot);
    if (stored_version != nullptr) {
        current_version = *stored_version;
    }
    if (check != nullptr && !check(current_version)) {
        return false;
    }

    const UndoLink old_link = stored_version == nullptr ? UndoLink{} : stored_version->prev_;
    const UndoLink new_link = prev_version.has_value() ? prev_version->prev_ : UndoLink{};
    const bool link_changed = old_link != new_link;
    if (link_changed && new_link.IsValid()) {
        RetainUndoReference(new_link);
    }

    bool changed = false;
    bool mutation_succeeded = false;
    try {
        if (prev_version.has_value()) {
            changed = stored_version == nullptr || *stored_version != *prev_version;
            if (changed) {
                BumpVisibilityEpoch(page_info);
            }
            MarkDirtySlot(table_info, page_info, rid);
            if (stored_version == nullptr) {
                AddVersionLink(page_info);
            }
            page_info->SetVersion(slot, *prev_version);
        } else {
            if (stored_version != nullptr) {
                changed = true;
                BumpVisibilityEpoch(page_info);
                page_info->ClearVersion(slot);
                RemoveVersionLink(page_info);
                ClearDirtySlotIfEmpty(table_info, page_info, rid);
                page_info->ReleaseVersionStorageIfEmpty();
            }
        }
        if (changed) {
            BumpVisibilityEpoch(page_info);
        }
        mutation_succeeded = true;
        if (link_changed && old_link.IsValid()) {
            ReleaseUndoReference(old_link);
        }
        return true;
    } catch (...) {
        if (!mutation_succeeded && link_changed && new_link.IsValid()) {
            ReleaseUndoReference(new_link);
        }
        throw;
    }
}

void TransactionManager::InstallTupleVersion(const std::string &tab_name, Rid rid,
                                              const VersionUndoLink &version, const TupleMeta &meta) {
    auto table_info = GetOrCreateTableVersionInfo(tab_name);
    auto page_info = GetOrCreatePageVersionInfoOnTable(table_info, tab_name, rid.page_no);
    std::unique_lock<std::shared_mutex> page_lock(page_info->mutex_);
    slot_offset_t slot = static_cast<slot_offset_t>(rid.slot_no);
    if (rid.slot_no < 0 || !page_info->CanTrackSlot(slot)) {
        throw InternalError("Version metadata slot is out of range");
    }

    // Allocate both sparse stores before retaining or publishing anything so
    // allocation failure cannot leave a half-installed tuple version.
    page_info->EnsureVersionStorage();
    page_info->EnsureTupleMetaStorage();
    VersionUndoLink *stored_version = page_info->GetMutableVersion(slot);
    const TupleMeta *stored_meta = page_info->GetTupleMeta(slot);
    UndoLink old_link = stored_version == nullptr ? UndoLink{} : stored_version->prev_;
    bool link_changed = old_link != version.prev_;
    if (link_changed && version.prev_.IsValid()) {
        RetainUndoReference(version.prev_);
    }

    BumpVisibilityEpoch(page_info);
    MarkDirtySlot(table_info, page_info, rid);
    if (stored_version == nullptr) {
        AddVersionLink(page_info);
    }
    page_info->SetVersion(slot, version);

    bool had_meta = stored_meta != nullptr;
    bool was_uncommitted = had_meta && stored_meta->ts_ >= TXN_START_ID;
    bool was_deleted = had_meta && stored_meta->is_deleted_;
    bool will_be_uncommitted = meta.ts_ >= TXN_START_ID;
    if (!had_meta) {
        AddActiveMeta(page_info);
    }
    if (!was_uncommitted && will_be_uncommitted) {
        AddUncommittedMeta(page_info);
    } else if (was_uncommitted && !will_be_uncommitted) {
        RemoveUncommittedMeta(page_info);
    }
    if (!was_deleted && meta.is_deleted_) {
        AddDeletedMeta(page_info);
    } else if (was_deleted && !meta.is_deleted_) {
        RemoveDeletedMeta(page_info);
    }
    if (meta.ts_ < TXN_START_ID) {
        AdvanceMaxCommittedMetaTs(page_info, meta.ts_);
    }
    page_info->SetTupleMeta(slot, meta);
    BumpVisibilityEpoch(page_info);
    if (link_changed && old_link.IsValid()) {
        ReleaseUndoReference(old_link);
    }
}

std::optional<UndoLink> TransactionManager::GetUndoLink(const std::string &tab_name, Rid rid) {
    auto version_link = GetVersionLink(tab_name, rid);
    if (!version_link.has_value() || !version_link->prev_.IsValid()) {
        return std::nullopt;
    }
    return version_link->prev_;
}

std::optional<VersionUndoLink> TransactionManager::GetVersionLink(const std::string &tab_name, Rid rid) {
    auto table_info = GetTableVersionInfo(tab_name);
    auto page_info = GetPageVersionInfoOnTable(table_info, rid.page_no);
    if (page_info == nullptr) {
        return std::nullopt;
    }

    std::shared_lock<std::shared_mutex> page_lock(page_info->mutex_);
    if (rid.slot_no < 0) {
        return std::nullopt;
    }
    const VersionUndoLink *version = page_info->GetVersion(static_cast<slot_offset_t>(rid.slot_no));
    return version == nullptr ? std::nullopt : std::optional<VersionUndoLink>(*version);
}

std::optional<UndoLog> TransactionManager::GetUndoLogOptional(UndoLink link) {
    if (!link.IsValid()) {
        return std::nullopt;
    }
    return transaction_registry_.WithTransactionShared(link.prev_txn_, [&](Transaction *txn)
                                                -> std::optional<UndoLog> {
        if (txn == nullptr) {
            return std::nullopt;
        }
        if (link.prev_log_idx_ < 0 || static_cast<size_t>(link.prev_log_idx_) >= txn->GetUndoLogNum()) {
            return std::nullopt;
        }
        return txn->GetUndoLog(link.prev_log_idx_);
    });
}

UndoLink TransactionManager::AppendUndoLog(Transaction *txn, UndoLog log) {
    if (txn == nullptr) {
        throw InternalError("Cannot append undo log without a transaction");
    }
    const UndoLink predecessor = log.prev_version_;
    const bool cross_transaction = predecessor.IsValid() &&
                                   predecessor.prev_txn_ != txn->get_transaction_id();
    if (cross_transaction) {
        RetainUndoReference(predecessor);
    }
    try {
        return txn->AppendUndoLog(std::move(log));
    } catch (...) {
        if (cross_transaction) {
            ReleaseUndoReference(predecessor);
        }
        throw;
    }
}

UndoLog TransactionManager::GetUndoLog(UndoLink link) {
    auto undo_log = GetUndoLogOptional(link);
    if (!undo_log.has_value()) {
        throw InternalError("Undo log not found");
    }
    return *undo_log;
}

timestamp_t TransactionManager::GetWatermark() {
    return lifecycle_state_.running_txns.GetWatermark();
}

void TransactionManager::EraseTableState(const std::string &tab_name) {
    snapshot_index_history_.EraseTable(tab_name);
    std::unique_lock<std::shared_mutex> guard(version_state_.mutex);
    if (version_state_.tables.erase(tab_name) > 0) {
        version_state_.epoch.fetch_add(1, std::memory_order_acq_rel);
    }
}

#include <algorithm>
#include <chrono>
#include <vector>

namespace {

constexpr size_t kTxnGcBatchLimit = 512;
constexpr size_t kRetiredTupleGcBatchLimit = 4096;
constexpr auto kGcTriggerInterval = std::chrono::milliseconds(2);

using rmdb::transaction_page_state::BumpVisibilityEpoch;
using rmdb::transaction_page_state::ClearDirtySlotIfEmpty;
using rmdb::transaction_page_state::RemoveActiveMeta;
using rmdb::transaction_page_state::RemoveDeletedMeta;
using rmdb::transaction_page_state::RemoveVersionLink;
using rmdb::atomic_counter::DecrementPositive;
using rmdb::atomic_counter::Increment;

}  // namespace

void TransactionManager::EnqueueRetiredTuples(timestamp_t commit_ts, std::vector<RetiredTuple> tuples) {
    if (tuples.empty()) {
        return;
    }
    RetiredTransaction retired{commit_ts, std::move(tuples)};
    const size_t tuple_count = retired.tuples.size();
    std::lock_guard<std::mutex> gc_lock(gc_state_.queue_mutex);
    auto insert_pos = std::upper_bound(
        gc_state_.retired_batches.begin(), gc_state_.retired_batches.end(), commit_ts,
        [](timestamp_t ts, const RetiredTransaction &entry) { return ts < entry.commit_ts; });
    gc_state_.retired_batches.insert(insert_pos, std::move(retired));
    Increment(gc_state_.retired_tuple_count, tuple_count, std::memory_order_release);
}

bool TransactionManager::ReclaimRetiredTuple(timestamp_t commit_ts, const RetiredTuple &retired) {
    auto table_info = GetTableVersionInfo(retired.tab_name);
    if (table_info == nullptr) {
        return true;
    }
    auto page_info = GetPageVersionInfoOnTable(table_info, retired.rid.page_no);
    if (page_info == nullptr) {
        return true;
    }

    if (retired.rid.slot_no < 0) {
        return true;
    }
    const slot_offset_t slot = static_cast<slot_offset_t>(retired.rid.slot_no);

    UndoLink released_link{};
    auto clear_retired_state = [&](const VersionUndoLink *stored_version, bool is_deleted) {
        BumpVisibilityEpoch(page_info);
        page_info->ClearTupleMeta(slot);
        RemoveActiveMeta(page_info);
        if (is_deleted) {
            RemoveDeletedMeta(page_info);
        }

        if (stored_version != nullptr) {
            released_link = stored_version->prev_;
            page_info->ClearVersion(slot);
            RemoveVersionLink(page_info);
        }
        ClearDirtySlotIfEmpty(table_info, page_info, retired.rid);
        page_info->ReleaseTupleMetaStorageIfEmpty();
        page_info->ReleaseVersionStorageIfEmpty();
        BumpVisibilityEpoch(page_info);
    };

    bool needs_physical_delete = false;
    {
        std::unique_lock<std::shared_mutex> page_lock(page_info->mutex_);
        const TupleMeta *stored_meta = page_info->GetTupleMeta(slot);
        if (stored_meta == nullptr || stored_meta->ts_ != commit_ts || stored_meta->ts_ >= TXN_START_ID) {
            return true;
        }

        const VersionUndoLink *stored_version = page_info->GetVersion(slot);
        if (stored_version != nullptr && stored_version->in_progress_) {
            return false;
        }

        needs_physical_delete = stored_meta->is_deleted_;
        if (!needs_physical_delete) {
            clear_retired_state(stored_version, false);
        }
    }

    if (needs_physical_delete) {
        // Deleting the heap slot cannot be made atomic with the metadata recheck by
        // dropping page_info->mutex_: another transaction may reuse the RID in that
        // window.  It also cannot wait for the heap write latch while holding the
        // metadata latch because visibility readers take those latches in the reverse
        // order. Reclaim physical storage only at an idle admission boundary; busy
        // workloads leave the tombstone for WAL maintenance to drain as a batch.
        auto drain_guard = TryBlockNewTransactionsIfIdle();
        if (!drain_guard) {
            // Keep the committed DELETE metadata as the authoritative tombstone.
            // WAL reset and clean shutdown already scan and physicalize these rows
            // under a full admission drain, so this retired-queue entry need not be
            // retried on every subsequent commit.
            return true;
        }

        // Visibility readers keep the heap page read-latched while consulting this page's
        // metadata. Never invert that order by waiting for the heap write latch while holding
        // page_info->mutex_: it deadlocks readers and prevents index reader epochs from draining.
        if (sm_manager_ != nullptr) {
            auto fh_iter = sm_manager_->fhs_.find(retired.tab_name);
            if (fh_iter != sm_manager_->fhs_.end() && fh_iter->second != nullptr) {
                try {
                    if (fh_iter->second->is_record(retired.rid)) {
                        // The logical DELETE and its commit record are durable before the tuple is retired.
                        fh_iter->second->delete_record(retired.rid, nullptr);
                    }
                } catch (...) {
                    return false;
                }
            }
        }

        std::unique_lock<std::shared_mutex> page_lock(page_info->mutex_);
        const TupleMeta *stored_meta = page_info->GetTupleMeta(slot);
        if (stored_meta == nullptr || stored_meta->ts_ != commit_ts || stored_meta->ts_ >= TXN_START_ID ||
            !stored_meta->is_deleted_) {
            throw InternalError("Retired tuple metadata changed during physical reclamation");
        }
        const VersionUndoLink *stored_version = page_info->GetVersion(slot);
        if (stored_version != nullptr && stored_version->in_progress_) {
            throw InternalError("Retired tuple became active during physical reclamation");
        }
        clear_retired_state(stored_version, true);
    }

    if (released_link.IsValid()) {
        ReleaseUndoReference(released_link);
    }
    return true;
}

void TransactionManager::QueueFinishedTransactionForGc(Transaction *txn) {
    if (txn == nullptr) {
        return;
    }
    if (!txn->gc_ready() ||
        (txn->get_state() != TransactionState::COMMITTED &&
         txn->get_state() != TransactionState::ABORTED)) {
        return;
    }
    {
        std::lock_guard<std::mutex> gc_lock(gc_state_.queue_mutex);
        gc_state_.finished_transactions.push_back(txn->get_transaction_id());
        // 节流：GC 需持注册表全局排他锁并遍历全部事务；高频提交下逐事务触发会形成
        // 全局串行热点。最多每 kGcTriggerInterval 触发一次，队列达到批量上限时立即触发。
        const auto now = std::chrono::steady_clock::now();
        if (now - gc_state_.last_gc_trigger < kGcTriggerInterval &&
            gc_state_.finished_transactions.size() < kTxnGcBatchLimit) {
            return;
        }
        gc_state_.last_gc_trigger = now;
    }
    GarbageCollection();
}

void TransactionManager::GarbageCollectFinishedTransactions() {
    std::deque<txn_id_t> candidates;
    {
        std::lock_guard<std::mutex> gc_lock(gc_state_.queue_mutex);
        size_t attempts = std::min(gc_state_.finished_transactions.size(), kTxnGcBatchLimit);
        for (size_t i = 0; i < attempts; ++i) {
            candidates.push_back(gc_state_.finished_transactions.front());
            gc_state_.finished_transactions.pop_front();
        }
    }
    if (candidates.empty()) {
        return;
    }

    std::vector<Transaction *> releasable;
    std::deque<txn_id_t> retry;
    transaction_registry_.WithAllExclusive([&](TransactionRegistry::AllView transactions) {
        SerializableMetadataRetention ssi_retention(transactions);

        while (!candidates.empty()) {
            txn_id_t txn_id = candidates.front();
            candidates.pop_front();

            auto *txn = transactions.Find(txn_id);
            if (txn == nullptr) {
                continue;
            }
            if (!txn->gc_ready() ||
                (txn->get_state() != TransactionState::COMMITTED &&
                 txn->get_state() != TransactionState::ABORTED)) {
                continue;
            }

            if (txn->GetUndoReferenceCount() == 0 && txn->GetUndoLogNum() > 0) {
                auto undo_logs = txn->TakeUndoLogs();
                for (const auto &undo_log : undo_logs) {
                    const UndoLink &predecessor = undo_log.prev_version_;
                    if (!predecessor.IsValid() || predecessor.prev_txn_ == txn_id) {
                        continue;
                    }
                    auto *predecessor_txn = transactions.Find(predecessor.prev_txn_);
                    if (predecessor_txn == nullptr) {
                        throw InternalError("Undo predecessor transaction is missing during GC");
                    }
                    predecessor_txn->ReleaseUndoReference();
                }
            }

            ssi_retention.ReleaseIfSafe(txn_id, txn, transactions);

            bool undo_released = txn->GetUndoReferenceCount() == 0 && txn->GetUndoLogNum() == 0;
            if (undo_released && txn->ssi_metadata_released()) {
                releasable.push_back(txn);
                transactions.Erase(txn_id);
            } else {
                retry.push_back(txn_id);
            }
        }
    });

    if (!retry.empty()) {
        std::lock_guard<std::mutex> gc_lock(gc_state_.queue_mutex);
        while (!retry.empty()) {
            gc_state_.finished_transactions.push_back(retry.front());
            retry.pop_front();
        }
    }

    for (auto *txn : releasable) {
        delete txn;
    }
}

void TransactionManager::GarbageCollection() {
    std::unique_lock<std::mutex> run_lock(gc_state_.run_mutex, std::try_to_lock);
    if (!run_lock.owns_lock()) {
        return;
    }

    const timestamp_t watermark = GetWatermark();
    snapshot_index_history_.Purge(watermark);
    std::vector<RetiredTransaction> ready;
    size_t ready_tuple_count = 0;
    {
        std::lock_guard<std::mutex> gc_lock(gc_state_.queue_mutex);
        while (!gc_state_.retired_batches.empty() &&
               gc_state_.retired_batches.front().commit_ts <= watermark) {
            size_t next_count = gc_state_.retired_batches.front().tuples.size();
            if (!ready.empty() && ready_tuple_count + next_count > kRetiredTupleGcBatchLimit) {
                break;
            }
            ready_tuple_count += next_count;
            ready.push_back(std::move(gc_state_.retired_batches.front()));
            gc_state_.retired_batches.pop_front();
            DecrementPositive(gc_state_.retired_tuple_count, "Retired tuple count", next_count,
                              std::memory_order_release, std::memory_order_relaxed);
        }
    }

    std::vector<RetiredTransaction> retry;
    for (auto &retired_txn : ready) {
        RetiredTransaction failed{retired_txn.commit_ts, {}};
        for (const auto &retired : retired_txn.tuples) {
            if (!ReclaimRetiredTuple(retired_txn.commit_ts, retired)) {
                failed.tuples.push_back(retired);
            }
        }
        if (!failed.tuples.empty()) {
            retry.push_back(std::move(failed));
        }
    }
    for (auto &failed : retry) {
        EnqueueRetiredTuples(failed.commit_ts, std::move(failed.tuples));
    }

    GarbageCollectFinishedTransactions();
}

void TransactionManager::PhysicalizeCommittedDeletes() {
    if (sm_manager_ == nullptr) {
        return;
    }

    std::vector<std::pair<std::string, std::shared_ptr<TableVersionInfo>>> tables;
    {
        std::shared_lock<std::shared_mutex> version_lock(version_state_.mutex);
        tables.reserve(version_state_.tables.size());
        for (const auto &table_entry : version_state_.tables) {
            if (table_entry.second != nullptr) {
                tables.push_back(table_entry);
            }
        }
    }

    std::vector<std::pair<std::string, Rid>> deletes;
    for (const auto &table_entry : tables) {
        const auto &tab_name = table_entry.first;
        const auto &table_info = table_entry.second;
        std::shared_lock<std::shared_mutex> table_lock(table_info->mutex_);
        for (const auto &page_entry : table_info->pages_) {
            auto page_info = page_entry.second;
            if (page_info == nullptr) {
                continue;
            }
            std::shared_lock<std::shared_mutex> page_lock(page_info->mutex_);
            // 无已提交删除标记的页面无需逐个槽位扫描（消除 reset 停顿中的全表横扫）
            if (page_info->deleted_count_.load(std::memory_order_acquire) == 0) {
                continue;
            }
            for (slot_offset_t slot = 0; slot < page_info->slot_count_; ++slot) {
                const TupleMeta *meta = page_info->GetTupleMeta(slot);
                if (meta != nullptr && meta->is_deleted_ && meta->ts_ < TXN_START_ID) {
                    deletes.push_back({tab_name, Rid{page_entry.first, static_cast<int>(slot)}});
                }
            }
        }
    }

    for (const auto &entry : deletes) {
        const std::string &tab_name = entry.first;
        Rid rid = entry.second;
        auto fh_iter = sm_manager_->fhs_.find(tab_name);
        if (fh_iter == sm_manager_->fhs_.end() || fh_iter->second == nullptr) {
            throw InternalError("Cannot physicalize DELETE for unopened table " + tab_name);
        }
        if (fh_iter->second->is_record(rid)) {
            fh_iter->second->delete_record(rid, nullptr);
        }
        UpdateTupleMeta(tab_name, rid, std::nullopt);
        UpdateVersionLink(tab_name, rid, std::nullopt);
    }
}


#include "index/ix.h"
#include "common/common.h"
#include "record/rm_file_handle.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using rmdb::transaction_page_state::BumpVisibilityEpoch;
using rmdb::transaction_page_state::MarkDirtySlot;
using rmdb::transaction_page_state::AddActiveMeta;
using rmdb::transaction_page_state::AddDeletedMeta;
using rmdb::transaction_page_state::AdvanceMaxCommittedMetaTs;
using rmdb::transaction_page_state::RemoveDeletedMeta;
using rmdb::transaction_page_state::RemoveUncommittedMeta;

inline void spin_pause() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

void delete_index_entries_bound(const std::vector<rmdb::IndexBinding> &bindings, const RmRecord &record,
                                const Rid &rid, Transaction *txn) {
    std::vector<std::string> key_scratch(bindings.size());
    for (size_t i = 0; i < bindings.size(); ++i) {
        const auto &binding = bindings[i];
        key_scratch[i].resize(binding.meta->col_tot_len);
        char *key = rmdb::build_index_key_into(*binding.meta, record.data, rid, &key_scratch[i]);
        binding.ih->delete_entry(key, txn);
    }
}

void delete_index_entries(SmManager *sm_manager, const TabMeta &tab, const std::string &tab_name,
                          const RmRecord &record, const Rid &rid, Transaction *txn) {
    auto bindings = rmdb::bind_table_indexes(sm_manager, tab_name, tab);
    delete_index_entries_bound(bindings, record, rid, txn);
}

void delete_changed_old_index_entries_bound(const std::vector<rmdb::IndexBinding> &bindings,
                                            const RmRecord &old_record, const RmRecord &new_record, const Rid &rid,
                                            Transaction *txn) {
    std::vector<std::string> old_key_scratch(bindings.size());
    std::vector<std::string> new_key_scratch(bindings.size());
    for (size_t i = 0; i < bindings.size(); ++i) {
        const auto &binding = bindings[i];
        old_key_scratch[i].resize(binding.meta->col_tot_len);
        new_key_scratch[i].resize(binding.meta->col_tot_len);
        char *old_key = rmdb::build_index_key_into(*binding.meta, old_record.data, rid, &old_key_scratch[i]);
        char *new_key = rmdb::build_index_key_into(*binding.meta, new_record.data, rid, &new_key_scratch[i]);
        if (memcmp(old_key, new_key, binding.meta->col_tot_len) != 0) {
            binding.ih->delete_entry(old_key, txn);
        }
    }
}

void delete_changed_old_index_entries(SmManager *sm_manager, const TabMeta &tab, const std::string &tab_name,
                                      const RmRecord &old_record, const RmRecord &new_record, const Rid &rid,
                                      Transaction *txn) {
    auto bindings = rmdb::bind_table_indexes(sm_manager, tab_name, tab);
    delete_changed_old_index_entries_bound(bindings, old_record, new_record, rid, txn);
}

void delete_changed_new_index_entries_bound(const std::vector<rmdb::IndexBinding> &bindings,
                                            const RmRecord &old_record, const RmRecord &new_record, const Rid &rid,
                                            Transaction *txn) {
    std::vector<std::string> old_key_scratch(bindings.size());
    std::vector<std::string> new_key_scratch(bindings.size());
    for (size_t i = 0; i < bindings.size(); ++i) {
        const auto &binding = bindings[i];
        old_key_scratch[i].resize(binding.meta->col_tot_len);
        new_key_scratch[i].resize(binding.meta->col_tot_len);
        char *old_key = rmdb::build_index_key_into(*binding.meta, old_record.data, rid, &old_key_scratch[i]);
        char *new_key = rmdb::build_index_key_into(*binding.meta, new_record.data, rid, &new_key_scratch[i]);
        if (memcmp(old_key, new_key, binding.meta->col_tot_len) != 0) {
            binding.ih->delete_entry(new_key, txn);
        }
    }
}

void release_locks(LockManager *lock_manager, Transaction *txn) {
    if (lock_manager == nullptr || txn == nullptr) {
        return;
    }
    lock_manager->unlock_all(txn);
}

}  // namespace

TransactionDrainGuard::~TransactionDrainGuard() {
    reset();
}

TransactionDrainGuard::TransactionDrainGuard(TransactionDrainGuard &&other) noexcept : manager_(other.manager_) {
    other.manager_ = nullptr;
}

TransactionDrainGuard &TransactionDrainGuard::operator=(TransactionDrainGuard &&other) noexcept {
    if (this != &other) {
        reset();
        manager_ = other.manager_;
        other.manager_ = nullptr;
    }
    return *this;
}

void TransactionDrainGuard::reset() {
    if (manager_ != nullptr) {
        manager_->ReleaseTransactionDrain();
        manager_ = nullptr;
    }
}

Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager,
                                       IsolationLevel isolation_level) {
    static const std::vector<lock_data_key_t> kNoIntents;
    return begin(txn, log_manager, isolation_level, kNoIntents);
}

void TransactionManager::ReserveTransactionAdmission() {
    auto &state = lifecycle_state_;
    // 快路径:先原子递增,再检查阻塞标志。递增若发生在 drain 置位之前,
    // drain 的 count==0 等待会观察到该增量并等待其撤销/结束,不会漏计;
    // 若阻塞已置位,撤销增量并转入持锁慢路径等待。
    state.active_transaction_count.fetch_add(1, std::memory_order_acq_rel);
    if (!state.admission_blocked.load(std::memory_order_acquire)) {
        return;
    }
    state.active_transaction_count.fetch_sub(1, std::memory_order_acq_rel);
    std::unique_lock<std::mutex> admission_lock(state.admission_mutex);
    // 唤醒可能在等待 count==0 的 drain(我们的撤销可能使其归零)。
    state.admission_cv.notify_all();
    state.admission_cv.wait(admission_lock, [&] {
        return !state.admission_blocked.load(std::memory_order_acquire);
    });
    // 持锁递增:drain 的 blocked 置位与 count 检查在同一把锁下,不会交错。
    state.active_transaction_count.fetch_add(1, std::memory_order_acq_rel);
}

Transaction *TransactionManager::begin(Transaction* txn, LogManager* log_manager,
                                       IsolationLevel isolation_level,
                                       const std::vector<lock_data_key_t> &write_intents,
                                       bool write_intents_canonical) {
    ReserveTransactionAdmission();

    std::unique_ptr<Transaction> owned_txn;
    try {
        if (txn == nullptr) {
            owned_txn = std::make_unique<Transaction>(lifecycle_state_.next_txn_id++, isolation_level);
            txn = owned_txn.get();
        } else {
            txn->set_isolation_level(isolation_level);
        }
        txn->set_state(TransactionState::GROWING);
        txn->set_start_ts(lifecycle_state_.next_timestamp++);
        if (lock_manager_ != nullptr && !write_intents.empty()) {
            if (!lock_manager_->acquire_write_intents(
                    txn, write_intents, write_intents_canonical)) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                                AbortSubReason::WRITE_INTENT_CONFLICT);
            }
        }
        txn->set_read_ts(lifecycle_state_.last_commit_ts.load());
        txn->set_commit_ts(INVALID_TS);
        txn->set_watermark_registered(false);
        txn->set_gc_ready(false);
        txn->set_admission_active(true);
        txn->set_ssi_metadata_released(isolation_level != IsolationLevel::SERIALIZABLE);
        lifecycle_state_.running_txns.AddTxn(txn->get_read_ts());
        txn->set_watermark_registered(true);

        if (log_manager != nullptr) {
            BeginLogRecord log_record(txn->get_transaction_id());
            txn->set_prev_lsn(log_manager->add_log_to_buffer(&log_record));
        }
        transaction_registry_.Insert(txn);
        owned_txn.release();
        return txn;
    } catch (...) {
        if (txn != nullptr) {
            if (txn->watermark_registered()) {
                lifecycle_state_.running_txns.RemoveTxn(txn->get_read_ts());
                txn->set_watermark_registered(false);
            }
            if (lock_manager_ != nullptr) {
                lock_manager_->release_write_intents(txn);
            }
            txn->set_admission_active(false);
        }
        CancelTransactionAdmissionReservation();
        throw;
    }
}

Transaction *TransactionManager::begin_read_only(IsolationLevel isolation_level) {
    if (isolation_level != IsolationLevel::SNAPSHOT_ISOLATION) {
        throw InternalError("Lightweight read-only transactions require snapshot isolation");
    }
    ReserveTransactionAdmission();
    std::unique_ptr<Transaction> txn;
    try {
        txn = std::make_unique<Transaction>(INVALID_TXN_ID, isolation_level);
        timestamp_t read_ts = lifecycle_state_.last_commit_ts.load(std::memory_order_acquire);
        txn->set_state(TransactionState::GROWING);
        txn->set_start_ts(read_ts);
        txn->set_read_ts(read_ts);
        txn->set_commit_ts(INVALID_TS);
        txn->set_lightweight_read_only(true);
        txn->set_watermark_registered(true);
        txn->set_gc_ready(false);
        txn->set_admission_active(true);
        txn->set_ssi_metadata_released(true);
        lifecycle_state_.running_txns.AddTxn(read_ts);
        return txn.release();
    } catch (...) {
        CancelTransactionAdmissionReservation();
        throw;
    }
}

void TransactionManager::PromoteToWrite(Transaction *txn, LogManager *log_manager) {
    if (txn == nullptr || !txn->is_lightweight_read_only()) {
        return;
    }
    if (txn->get_isolation_level() != IsolationLevel::SNAPSHOT_ISOLATION ||
        txn->get_state() != TransactionState::GROWING) {
        throw InternalError("Only a growing SI snapshot can be promoted to write");
    }
    txn_id_t txn_id = lifecycle_state_.next_txn_id++;
    lsn_t begin_lsn = INVALID_LSN;
    if (log_manager != nullptr) {
        BeginLogRecord log_record(txn_id);
        begin_lsn = log_manager->add_log_to_buffer(&log_record);
    }
    txn->set_transaction_id(txn_id);
    txn->set_prev_lsn(begin_lsn);
    transaction_registry_.Insert(txn);
    txn->set_lightweight_read_only(false);
}

TransactionDrainGuard TransactionManager::BlockNewTransactionsAndWait() {
    std::unique_lock<std::mutex> admission_lock(lifecycle_state_.admission_mutex);
    lifecycle_state_.admission_cv.wait(admission_lock, [&] {
        return !lifecycle_state_.admission_blocked.load(std::memory_order_acquire);
    });
    // 置位后,快路径 begin 的"递增→复查"会观察到阻塞并撤销/等待;
    // 置位前已递增的活跃事务都会计入下面的 count==0 等待。
    lifecycle_state_.admission_blocked.store(true, std::memory_order_release);
    lifecycle_state_.admission_cv.wait(admission_lock, [&] {
        return lifecycle_state_.active_transaction_count.load(std::memory_order_acquire) == 0;
    });
    return TransactionDrainGuard(this);
}

TransactionDrainGuard TransactionManager::TryBlockNewTransactionsIfIdle() {
    std::lock_guard<std::mutex> admission_lock(lifecycle_state_.admission_mutex);
    if (lifecycle_state_.admission_blocked.load(std::memory_order_acquire) ||
        lifecycle_state_.active_transaction_count.load(std::memory_order_acquire) != 0) {
        return {};
    }
    lifecycle_state_.admission_blocked.store(true, std::memory_order_release);
    return TransactionDrainGuard(this);
}

TransactionDrainGuard TransactionManager::BlockNewTransactionsAndWaitFor(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> admission_lock(lifecycle_state_.admission_mutex);
    if (lifecycle_state_.admission_blocked.load(std::memory_order_acquire)) {
        return {};
    }
    lifecycle_state_.admission_blocked.store(true, std::memory_order_release);
    if (!lifecycle_state_.admission_cv.wait_for(admission_lock, timeout, [&] {
            return lifecycle_state_.active_transaction_count.load(std::memory_order_acquire) == 0;
        })) {
        lifecycle_state_.admission_blocked.store(false, std::memory_order_release);
        admission_lock.unlock();
        lifecycle_state_.admission_cv.notify_all();
        return {};
    }
    return TransactionDrainGuard(this);
}

size_t TransactionManager::ActiveTransactionCount() const {
    return lifecycle_state_.active_transaction_count.load(std::memory_order_acquire);
}

void TransactionManager::FinishTransactionAdmission(Transaction *txn) {
    if (txn == nullptr || !txn->admission_active()) {
        return;
    }
    txn->set_admission_active(false);
    CancelTransactionAdmissionReservation();
}

void TransactionManager::CancelTransactionAdmissionReservation() {
    // 原子递减;仅在 drain 阻塞期间才持锁唤醒等待者。
    size_t prior = lifecycle_state_.active_transaction_count.load(std::memory_order_acquire);
    while (true) {
        if (prior == 0) {
            throw InternalError("Transaction admission count underflow");
        }
        if (lifecycle_state_.active_transaction_count.compare_exchange_weak(
                prior, prior - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
    }
    if (lifecycle_state_.admission_blocked.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> admission_lock(lifecycle_state_.admission_mutex);
        lifecycle_state_.admission_cv.notify_all();
    }
}

void TransactionManager::ReleaseTransactionDrain() {
    std::lock_guard<std::mutex> admission_lock(lifecycle_state_.admission_mutex);
    lifecycle_state_.admission_blocked.store(false, std::memory_order_release);
    lifecycle_state_.admission_cv.notify_all();
}

std::vector<CheckpointTxnInfo> TransactionManager::CollectActiveTxnCheckpointInfo(Transaction *exclude_txn) {
    return transaction_registry_.WithAllShared([&](const TransactionRegistry::AllView &transactions) {
        std::vector<CheckpointTxnInfo> active_txns;
        active_txns.reserve(transactions.Size());
        transactions.ForEach([&](txn_id_t txn_id, Transaction *txn) {
            if (txn == nullptr || txn == exclude_txn || txn->get_state() != TransactionState::GROWING) {
                return;
            }
            active_txns.push_back(CheckpointTxnInfo{txn_id, txn->get_prev_lsn()});
        });
        return active_txns;
    });
}

void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->is_lightweight_read_only()) {
        timestamp_t commit_ts = lifecycle_state_.last_commit_ts.load(std::memory_order_acquire);
        txn->set_commit_ts(commit_ts);
        txn->set_state(TransactionState::COMMITTED);
        if (txn->watermark_registered()) {
            lifecycle_state_.running_txns.FinishTxn(txn->get_read_ts(), commit_ts);
            txn->set_watermark_registered(false);
        }
        FinishTransactionAdmission(txn);
        delete txn;
        return;
    }
    bool admission_finished = false;
    auto completion_guard = rmdb::make_scope_exit([&] {
        if (!admission_finished &&
            (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED)) {
            FinishTransactionAdmission(txn);
        }
    });
    auto &write_set = txn->get_write_set();
    const bool has_writes = !write_set.empty();

    struct PendingCommitTuple {
        std::shared_ptr<const std::string> tab_name;
        Rid rid;
        bool is_deleted;
        bool publish_version;
    };
    struct PendingCommitSlot {
        slot_offset_t slot;
        bool is_deleted;
        bool publish_version;
    };
    struct CommitPageBatch {
        std::shared_ptr<const std::string> tab_name;
        page_id_t page_no;
        std::shared_ptr<TableVersionInfo> table_info;
        std::shared_ptr<PageVersionInfo> page_info;
        std::vector<PendingCommitSlot> slots;
    };
    std::vector<PendingCommitTuple> pending_tuples;
    const auto &write_summary = txn->get_write_summary();
    pending_tuples.reserve(write_summary.size());
    for (const auto &entry : write_summary) {
        pending_tuples.push_back(PendingCommitTuple{
            entry.tab_name, entry.rid, entry.is_deleted, entry.publish_version});
    }
    std::sort(pending_tuples.begin(), pending_tuples.end(),
              [](const PendingCommitTuple &lhs,
                 const PendingCommitTuple &rhs) {
                  if (lhs.tab_name.get() != rhs.tab_name.get()) {
                      return std::less<const std::string *>{}(
                          lhs.tab_name.get(), rhs.tab_name.get());
                  }
                  if (lhs.rid.page_no != rhs.rid.page_no) {
                      return lhs.rid.page_no < rhs.rid.page_no;
                  }
                  return lhs.rid.slot_no < rhs.rid.slot_no;
              });

    std::vector<CommitPageBatch> page_batches;
    page_batches.reserve(pending_tuples.size());
    for (size_t begin = 0; begin < pending_tuples.size();) {
        const auto &first = pending_tuples[begin];
        size_t end = begin + 1;
        while (end < pending_tuples.size() &&
               pending_tuples[end].tab_name.get() == first.tab_name.get() &&
               pending_tuples[end].rid.page_no == first.rid.page_no) {
            ++end;
        }
        auto table_info = GetOrCreateTableVersionInfo(*first.tab_name);
        auto page_info = GetOrCreatePageVersionInfoOnTable(
            table_info, *first.tab_name, first.rid.page_no);
        CommitPageBatch batch{first.tab_name, first.rid.page_no,
                              std::move(table_info), std::move(page_info), {}};
        batch.slots.reserve(end - begin);
        for (size_t pos = begin; pos < end; ++pos) {
            const auto &pending = pending_tuples[pos];
            slot_offset_t slot = static_cast<slot_offset_t>(pending.rid.slot_no);
            if (pending.rid.slot_no < 0 || !batch.page_info->CanTrackSlot(slot)) {
                throw InternalError("Tuple metadata slot is out of range");
            }
            batch.slots.push_back(PendingCommitSlot{
                slot, pending.is_deleted, pending.publish_version});
        }
        page_batches.push_back(std::move(batch));
        begin = end;
    }

    if (log_manager != nullptr && has_writes) {
        CommitLogRecord log_record(txn->get_transaction_id());
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t commit_lsn = log_manager->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(commit_lsn);
        // 必须先持久化提交日志，再发布元组版本；否则崩溃后可能留下磁盘日志无法解释的可见数据。
        log_manager->flush_log_to_disk_until_group(commit_lsn + log_record.log_tot_len_ - 1);
    }

    // 各事务可以并行准备互不相干的页版本，但提交票号负责串行化最终的可见性发布。
    // 只有更早票号都准备完毕后，读事务的快照水位才会前移，从而避免观察到提交序列中的空洞。
    rmdb::u64 publish_ticket = 0;
    timestamp_t commit_version = INVALID_TS;
    {
        // 在同一临界区内分配票号和逻辑时间戳。begin/abort 也可能消耗时间戳，
        // 但真正提交的版本仍须按票号对应的时间戳单调发布。
        std::lock_guard<std::mutex> lock(lifecycle_state_.commit_publish_mutex);
        publish_ticket = lifecycle_state_.next_commit_ticket.fetch_add(1, std::memory_order_relaxed);
        commit_version = lifecycle_state_.next_timestamp++;
    }
    txn->set_commit_ts(commit_version);
    for (auto &batch : page_batches) {
        std::unique_lock<std::shared_mutex> page_lock(batch.page_info->mutex_);
        BumpVisibilityEpoch(batch.page_info);
        for (const auto &[slot, is_deleted, publish_version] : batch.slots) {
            TupleMeta *current_meta = batch.page_info->GetMutableTupleMeta(slot);
            bool had_meta = current_meta != nullptr;
            bool was_uncommitted = had_meta && current_meta->ts_ >= TXN_START_ID;
            bool was_deleted = had_meta && current_meta->is_deleted_;
            if (!had_meta) {
                Rid rid{batch.page_no, static_cast<int>(slot)};
                MarkDirtySlot(batch.table_info, batch.page_info, rid);
                AddActiveMeta(batch.page_info);
            }
            if (was_uncommitted) {
                RemoveUncommittedMeta(batch.page_info);
            }
            if (!was_deleted && is_deleted) {
                AddDeletedMeta(batch.page_info);
            } else if (was_deleted && !is_deleted) {
                RemoveDeletedMeta(batch.page_info);
            }
            batch.page_info->SetTupleMeta(slot, TupleMeta{commit_version, is_deleted});
            if (publish_version) {
                VersionUndoLink *version = batch.page_info->GetMutableVersion(slot);
                if (version == nullptr) {
                    continue;
                }
                version->in_progress_ = false;
            }
        }
        AdvanceMaxCommittedMetaTs(batch.page_info, commit_version);
        BumpVisibilityEpoch(batch.page_info);
    }

    {
        // 快速路径：发布临界区极短（毫秒级 WAL 刷盘已在临界区外完成），等待者先无锁自旋观察
        // 发布序号，避免每次提交 notify_all 惊动所有等待者去抢同一把全局锁。
        constexpr int kPublishSpinIterations = 200;  // ~20-40μs
        rmdb::u64 next = lifecycle_state_.next_publish_ticket.load(std::memory_order_acquire);
        for (int i = 0; i < kPublishSpinIterations && next != publish_ticket; ++i) {
            spin_pause();
            next = lifecycle_state_.next_publish_ticket.load(std::memory_order_acquire);
        }
        std::unique_lock<std::mutex> lock(lifecycle_state_.commit_publish_mutex);
        lifecycle_state_.commit_publish_cv.wait(lock, [&] {
            return publish_ticket ==
                   lifecycle_state_.next_publish_ticket.load(std::memory_order_acquire);
        });
        // 事务对象在稍后的 GC 移交前仍由注册表管理。TransactionState 是原子状态，
        // 因此这里不获取注册表全局锁；SSI 图维护和 GC 可在各自的注册表保护下安全观察完成状态。
        txn->set_state(TransactionState::COMMITTED);
        lifecycle_state_.last_commit_ts.store(commit_version, std::memory_order_release);
        if (txn->watermark_registered()) {
            lifecycle_state_.running_txns.FinishTxn(txn->get_read_ts(), commit_version);
            txn->set_watermark_registered(false);
        }
        lifecycle_state_.next_publish_ticket.store(publish_ticket + 1, std::memory_order_release);
    }
    lifecycle_state_.commit_publish_cv.notify_all();

    for (auto &write_record : write_set) {
        const std::string tab_name = write_record.GetTableName();
        Rid rid = write_record.GetRid();
        if (write_record.GetWriteType() == WType::DELETE_TUPLE) {
            RmRecord &old_record = write_record.GetRecord();
            auto bindings = rmdb::bind_table_indexes(sm_manager_, tab_name, sm_manager_->db_.get_table(tab_name));
            for (const auto &binding : bindings) {
                RecordSnapshotIndexRetirement(tab_name, *binding.meta, rid, txn->get_commit_ts());
            }
            delete_index_entries(sm_manager_, sm_manager_->db_.get_table(tab_name), tab_name, old_record, rid, txn);
        } else if (write_record.GetWriteType() == WType::UPDATE_TUPLE) {
            if (!write_record.IndexKeysChanged()) {
                continue;
            }
            RmRecord &old_record = write_record.GetRecord();
            auto current = sm_manager_->fhs_.at(tab_name)->get_record(rid, nullptr);
            auto bindings = rmdb::bind_table_indexes(sm_manager_, tab_name, sm_manager_->db_.get_table(tab_name));
            std::vector<std::string> old_key_scratch(bindings.size());
            std::vector<std::string> new_key_scratch(bindings.size());
            for (size_t i = 0; i < bindings.size(); ++i) {
                const auto &binding = bindings[i];
                old_key_scratch[i].resize(binding.meta->col_tot_len);
                new_key_scratch[i].resize(binding.meta->col_tot_len);
                char *old_key = rmdb::build_index_key_into(*binding.meta, old_record.data, rid, &old_key_scratch[i]);
                char *new_key = rmdb::build_index_key_into(*binding.meta, current->data, rid, &new_key_scratch[i]);
                if (memcmp(old_key, new_key, binding.meta->col_tot_len) != 0) {
                    RecordSnapshotIndexRetirement(tab_name, *binding.meta, rid, txn->get_commit_ts());
                }
            }
            delete_changed_old_index_entries(sm_manager_, sm_manager_->db_.get_table(tab_name), tab_name,
                                             old_record, *current, rid, txn);
        }
    }

    std::vector<RetiredTuple> retired_tuples;
    retired_tuples.reserve(pending_tuples.size());
    for (const auto &pending : pending_tuples) {
        retired_tuples.push_back(RetiredTuple{*pending.tab_name, pending.rid});
    }
    EnqueueRetiredTuples(txn->get_commit_ts(), std::move(retired_tuples));

    txn->clear_write_records();
    if (txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        txn->clear_serializable_state();
        txn->set_ssi_metadata_released(true);
    }
    release_locks(lock_manager_, txn);
    FinishTransactionAdmission(txn);
    admission_finished = true;
    txn->set_gc_ready(true);
    QueueFinishedTransactionForGc(txn);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->is_lightweight_read_only()) {
        txn->set_state(TransactionState::ABORTED);
        if (txn->watermark_registered()) {
            lifecycle_state_.running_txns.FinishTxn(
                txn->get_read_ts(), lifecycle_state_.last_commit_ts.load(std::memory_order_acquire));
            txn->set_watermark_registered(false);
        }
        FinishTransactionAdmission(txn);
        delete txn;
        return;
    }
    bool admission_finished = false;
    auto completion_guard = rmdb::make_scope_exit([&] {
        if (!admission_finished &&
            (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED)) {
            FinishTransactionAdmission(txn);
        }
    });
    struct AbortTableCache {
        TabMeta *tab = nullptr;
        RmFileHandle *fh = nullptr;
        std::vector<rmdb::IndexBinding> index_bindings;
    };
    std::unordered_map<std::string, AbortTableCache> table_cache;
    auto get_table_cache = [&](const std::string &tab_name) -> AbortTableCache & {
        auto cache_iter = table_cache.find(tab_name);
        if (cache_iter != table_cache.end()) {
            return cache_iter->second;
        }
        AbortTableCache cache;
        cache.tab = &sm_manager_->db_.get_table(tab_name);
        cache.fh = sm_manager_->fhs_.at(tab_name).get();
        cache.index_bindings = rmdb::bind_table_indexes(sm_manager_, tab_name, *cache.tab);
        auto inserted = table_cache.emplace(tab_name, std::move(cache));
        return inserted.first->second;
    };
    auto &write_set = txn->get_write_set();
    while (!write_set.empty()) {
        WriteRecord write_record = std::move(write_set.back());
        write_set.pop_back();
        const std::string tab_name = write_record.GetTableName();
        auto &table = get_table_cache(tab_name);
        Rid rid = write_record.GetRid();

        if (write_record.GetWriteType() == WType::INSERT_TUPLE) {
            auto inserted = table.fh->get_record(rid, nullptr);
            delete_index_entries_bound(table.index_bindings, *inserted, rid, txn);
            table.fh->delete_record(rid, nullptr);
            UpdateTupleMeta(tab_name, rid, std::nullopt);
            UpdateVersionLink(tab_name, rid, std::nullopt);
        } else if (write_record.GetWriteType() == WType::DELETE_TUPLE) {
            auto version_link = GetVersionLink(tab_name, rid);
            if (version_link.has_value() && version_link->prev_.IsValid()) {
                auto undo_log = GetUndoLog(version_link->prev_);
                UpdateTupleMeta(tab_name, rid, TupleMeta{undo_log.ts_, undo_log.is_deleted_});
                UpdateVersionLink(tab_name, rid, VersionUndoLink::FromOptionalUndoLink(
                    undo_log.prev_version_.IsValid() ? std::optional<UndoLink>(undo_log.prev_version_) : std::nullopt));
            } else {
                UpdateTupleMeta(tab_name, rid, std::nullopt);
                UpdateVersionLink(tab_name, rid, std::nullopt);
            }
        } else if (write_record.GetWriteType() == WType::UPDATE_TUPLE) {
            RmRecord &old_record = write_record.GetRecord();
            if (write_record.IndexKeysChanged()) {
                auto current = table.fh->get_record(rid, nullptr);
                delete_changed_new_index_entries_bound(table.index_bindings, old_record, *current, rid, txn);
            }
            table.fh->update_record(rid, old_record.data, nullptr);
            auto version_link = GetVersionLink(tab_name, rid);
            if (version_link.has_value() && version_link->prev_.IsValid()) {
                auto undo_log = GetUndoLog(version_link->prev_);
                UpdateTupleMeta(tab_name, rid, TupleMeta{undo_log.ts_, undo_log.is_deleted_});
                UpdateVersionLink(tab_name, rid, VersionUndoLink::FromOptionalUndoLink(
                    undo_log.prev_version_.IsValid() ? std::optional<UndoLink>(undo_log.prev_version_) : std::nullopt));
            } else {
                UpdateTupleMeta(tab_name, rid, std::nullopt);
                UpdateVersionLink(tab_name, rid, std::nullopt);
            }
        }
    }
    auto finish_abort = [&] {
        txn->set_state(TransactionState::ABORTED);
        if (txn->watermark_registered()) {
            lifecycle_state_.running_txns.FinishTxn(
                txn->get_read_ts(), lifecycle_state_.last_commit_ts.load(std::memory_order_acquire));
            txn->set_watermark_registered(false);
        }
    };
    if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE) {
        transaction_registry_.WithAllExclusive([&](TransactionRegistry::AllView transactions) {
            txn->set_commit_ts(lifecycle_state_.next_timestamp++);
            finish_abort();
            transactions.ForEach([&](txn_id_t, Transaction *other) {
                if (other != nullptr && other != txn) {
                    other->remove_rw_dependency(txn->get_transaction_id());
                }
            });
            txn->clear_serializable_state();
            txn->set_ssi_metadata_released(true);
        });
    } else {
        // SI 事务不参与 SSI 依赖图。其状态为原子变量，检查点、SSI 和 GC 读者可直接观察终止状态，
        // 无须让每次热路径回滚都争用注册表全局写锁。
        txn->set_commit_ts(INVALID_TS);
        finish_abort();
    }
    if (log_manager != nullptr) {
        AbortLogRecord log_record(txn->get_transaction_id());
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t abort_lsn = log_manager->add_log_to_buffer(&log_record);
        log_manager->flush_log_to_disk_until_group(abort_lsn + log_record.log_tot_len_ - 1);
    }
    txn->clear_write_records();
    release_locks(lock_manager_, txn);
    FinishTransactionAdmission(txn);
    admission_finished = true;
    txn->set_gc_ready(true);
    QueueFinishedTransactionForGc(txn);
}


#include <algorithm>
#include <cstring>
#include <memory>

namespace {

bool same_rid(const Rid &lhs, const Rid &rhs) {
    return lhs.page_no == rhs.page_no && lhs.slot_no == rhs.slot_no;
}

bool has_read_record(Transaction *txn, const std::string &tab_name, const Rid &rid) {
    const auto &read_records = txn->get_read_records();
    auto iter = read_records.find(tab_name);
    if (iter == read_records.end()) {
        return false;
    }
    for (const auto &read_rid : iter->second) {
        if (same_rid(read_rid, rid)) {
            return true;
        }
    }
    return false;
}

bool has_written_record(Transaction *txn, const std::string &tab_name, const Rid &rid) {
    if (txn == nullptr) {
        return false;
    }
    for (const auto &entry : txn->get_serializable_writes()) {
        const auto &write_info = entry.second;
        if (write_info.tab_name == tab_name && same_rid(write_info.rid, rid)) {
            return true;
        }
    }
    return false;
}

bool is_overlapping_serializable(Transaction *other, Transaction *txn) {
    if (other == nullptr || other == txn || other->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }
    if (other->get_state() == TransactionState::GROWING) {
        return true;
    }
    return other->get_state() == TransactionState::COMMITTED &&
           other->get_commit_ts() != INVALID_TS &&
           other->get_commit_ts() > txn->get_start_ts();
}

bool writer_invisible_to_reader(Transaction *reader, Transaction *writer) {
    if (reader == nullptr || writer == nullptr || reader == writer) {
        return false;
    }
    if (!is_overlapping_serializable(writer, reader)) {
        return false;
    }
    if (writer->get_state() == TransactionState::GROWING) {
        return true;
    }
    return writer->get_state() == TransactionState::COMMITTED &&
           writer->get_commit_ts() != INVALID_TS &&
           writer->get_commit_ts() > reader->get_read_ts();
}

bool record_matches(const std::vector<ColMeta> &cols, const RmRecord &record, const std::vector<Condition> &conds) {
    auto find_col = [&](const TabCol &target) {
        return std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
    };
    auto compare = [](const char *lhs, const char *rhs, ColType type, int len) {
        if (type == TYPE_INT) {
            int a = rmdb::load_unaligned<int>(lhs);
            int b = rmdb::load_unaligned<int>(rhs);
            return (a > b) - (a < b);
        }
        if (type == TYPE_FLOAT) {
            float a = rmdb::load_unaligned<float>(lhs);
            float b = rmdb::load_unaligned<float>(rhs);
            return (a > b) - (a < b);
        }
        return memcmp(lhs, rhs, len);
    };
    auto pass = [](int cmp, CompOp op) {
        switch (op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
        }
        return false;
    };
    for (const auto &cond : conds) {
        auto lhs_col = find_col(cond.lhs_col);
        if (lhs_col == cols.end()) {
            return false;
        }
        const char *rhs = cond.is_rhs_val ? cond.rhs_val.raw->data : nullptr;
        if (!cond.is_rhs_val) {
            auto rhs_col = find_col(cond.rhs_col);
            if (rhs_col == cols.end()) {
                return false;
            }
            rhs = record.data + rhs_col->offset;
        }
        if (!pass(compare(record.data + lhs_col->offset, rhs, lhs_col->type, lhs_col->len), cond.op)) {
            return false;
        }
    }
    return true;
}

bool commits_before(Transaction *tout, Transaction *tin) {
    return tout != nullptr && tout->get_state() == TransactionState::COMMITTED &&
           tout->get_commit_ts() != INVALID_TS &&
           (tin == nullptr || tin->get_state() != TransactionState::COMMITTED ||
            tout->get_commit_ts() < tin->get_commit_ts());
}

bool has_dangerous_structure_after_new_edge(const TransactionRegistry::AllView &transactions,
                                            Transaction *from, Transaction *to) {
    if (from == nullptr || to == nullptr) {
        return false;
    }

    for (auto next_id : to->get_rw_dependencies()) {
        auto *tout = transactions.Find(next_id);
        if (tout == nullptr || tout->get_state() == TransactionState::ABORTED) {
            continue;
        }
        if (tout == from || commits_before(tout, from)) {
            return true;
        }
    }

    bool dangerous = false;
    transactions.ForEach([&](txn_id_t, Transaction *tin) {
        if (tin == nullptr || tin == from || tin->get_state() == TransactionState::ABORTED) {
            return;
        }
        if (tin->get_rw_dependencies().count(from->get_transaction_id()) == 0) {
            return;
        }
        if (to == tin || commits_before(to, tin)) {
            dangerous = true;
        }
    });
    return dangerous;
}

void add_dependency(const TransactionRegistry::AllView &transactions, Transaction *from, Transaction *to,
                    Transaction *abort_candidate) {
    if (from == nullptr || to == nullptr || from == to) {
        return;
    }
    if (from->get_rw_dependencies().count(to->get_transaction_id()) != 0) {
        return;
    }
    from->add_rw_dependency(to->get_transaction_id());
    if (has_dangerous_structure_after_new_edge(transactions, from, to)) {
        throw TransactionAbortException(abort_candidate->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                        AbortSubReason::SSI_DANGEROUS_STRUCTURE);
    }
}

}  // namespace

void TransactionManager::RecordSerializableRead(Transaction *txn, const std::string &tab_name, const Rid &rid) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return;
    }
    transaction_registry_.WithAllExclusive([&](TransactionRegistry::AllView transactions) {
        txn->add_read_record(tab_name, rid);
        transactions.ForEach([&](txn_id_t, Transaction *other) {
            if (!writer_invisible_to_reader(txn, other)) {
                return;
            }
            if (has_written_record(other, tab_name, rid)) {
                add_dependency(transactions, txn, other, txn);
            }
        });
    });
}

void TransactionManager::RecordSerializablePredicateRead(Transaction *txn, const std::string &tab_name,
                                                          const std::vector<ColMeta> &cols,
                                                          const std::vector<Condition> &conds) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return;
    }
    transaction_registry_.WithAllExclusive([&](TransactionRegistry::AllView transactions) {
        txn->add_predicate_read(tab_name, cols, conds);
        transactions.ForEach([&](txn_id_t, Transaction *other) {
            if (!writer_invisible_to_reader(txn, other)) {
                return;
            }
            for (const auto &write_entry : other->get_serializable_writes()) {
                const auto &write_info = write_entry.second;
                if (write_info.tab_name != tab_name) {
                    continue;
                }
                bool matches_old = write_info.old_record != nullptr &&
                                   record_matches(cols, *write_info.old_record, conds);
                bool matches_new = write_info.new_record != nullptr &&
                                   record_matches(cols, *write_info.new_record, conds);
                if (matches_old || matches_new) {
                    add_dependency(transactions, txn, other, txn);
                    return;
                }
            }
        });
    });
}

void TransactionManager::RecordSerializableWrite(Transaction *txn, const std::string &tab_name, const Rid &rid,
                                                 const RmRecord *old_record, const RmRecord *new_record,
                                                 const std::vector<ColMeta> *cols) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return;
    }
    transaction_registry_.WithAllExclusive([&](TransactionRegistry::AllView transactions) {
        std::shared_ptr<RmRecord> old_copy =
            old_record == nullptr ? nullptr : std::make_shared<RmRecord>(*old_record);
        std::shared_ptr<RmRecord> new_copy =
            new_record == nullptr ? nullptr : std::make_shared<RmRecord>(*new_record);
        txn->upsert_serializable_write(tab_name, rid, cols == nullptr ? std::vector<ColMeta>() : *cols,
                                       std::move(old_copy), std::move(new_copy));

        transactions.ForEach([&](txn_id_t, Transaction *other) {
            if (!is_overlapping_serializable(other, txn)) {
                return;
            }
            if (has_read_record(other, tab_name, rid)) {
                add_dependency(transactions, other, txn, txn);
            }
            if (cols == nullptr) {
                return;
            }
            for (const auto &predicate : other->get_predicate_reads()) {
                bool matches_old = old_record != nullptr && predicate.tab_name == tab_name &&
                                   record_matches(*cols, *old_record, predicate.conds);
                bool matches_new = new_record != nullptr && predicate.tab_name == tab_name &&
                                   record_matches(*cols, *new_record, predicate.conds);
                if (matches_old || matches_new) {
                    add_dependency(transactions, other, txn, txn);
                    return;
                }
            }
        });
    });
}


#include "index/ix.h"
#include "execution/execution_common.h"
#include "record/rm_file_handle.h"
#include "record/rm_scan.h"

#include <algorithm>
#include <vector>

namespace {

rmdb::u64 snapshot_index_id(const IndexMeta &index) {
    if (index.index_id == 0) {
        throw InternalError("Snapshot index history requires a stable index id");
    }
    return index.index_id;
}

std::string index_key(const IndexMeta &index, const char *record_data) {
    std::string key;
    size_t key_size = static_cast<size_t>(index.logical_col_tot_len()) +
                      static_cast<size_t>(index.col_num) * 2;
    for (const auto &col : index.cols) {
        key_size += col.name.size();
    }
    key.reserve(key_size);
    for (int i = 0; i < index.col_num; ++i) {
        const auto &col = index.cols[i];
        key += col.name;
        key.push_back('\x1f');
        key.append(record_data + col.offset, col.len);
        key.push_back('\x1e');
    }
    return key;
}

std::string raw_index_key(const IndexMeta &index, const char *record_data) {
    std::string key(index.logical_col_tot_len(), '\0');
    rmdb::build_index_logical_key_into(index, record_data, key.data());
    return key;
}

const ColMeta *logical_identity_col(const TabMeta &tab) {
    auto id_col = std::find_if(tab.cols.begin(), tab.cols.end(), [](const ColMeta &col) {
        return col.name == "id";
    });
    if (id_col != tab.cols.end()) {
        return &*id_col;
    }
    if (tab.cols.size() == 1) {
        return &tab.cols.front();
    }
    return nullptr;
}

std::string logical_key(const TabMeta &tab, const RmRecord &record) {
    const ColMeta *col = logical_identity_col(tab);
    if (col == nullptr) {
        return {};
    }
    std::string key = "\x1dlogical\x1f";
    key.reserve(key.size() + col->name.size() + 1 + static_cast<size_t>(col->len));
    key += col->name;
    key.push_back('\x1f');
    key.append(record.data + col->offset, col->len);
    return key;
}

bool tuple_version_visible_to_txn(const TupleMeta &meta, Transaction *txn) {
    if (txn == nullptr) {
        return true;
    }
    if (meta.ts_ >= TXN_START_ID) {
        txn_id_t owner = meta.ts_ - TXN_START_ID;
        return owner == txn->get_transaction_id();
    }
    return meta.ts_ <= txn->get_read_ts();
}

bool keys_overlap(const std::vector<std::string> &lhs, const std::vector<std::string> &rhs) {
    for (const auto &key : lhs) {
        if (std::find(rhs.begin(), rhs.end(), key) != rhs.end()) {
            return true;
        }
    }
    return false;
}

bool is_logical_identity_key(const std::string &key) {
    static const std::string prefix = "\x1dlogical\x1f";
    return key.compare(0, prefix.size(), prefix) == 0;
}

bool nonlogical_keys_overlap(const std::vector<std::string> &lhs, const std::vector<std::string> &rhs) {
    for (const auto &key : lhs) {
        if (is_logical_identity_key(key)) {
            continue;
        }
        if (std::find(rhs.begin(), rhs.end(), key) != rhs.end()) {
            return true;
        }
    }
    return false;
}

}  // namespace

std::optional<RmRecord> TransactionManager::GetVisibleTuple(const std::string &tab_name, const Rid &rid,
                                                            Transaction *txn, TupleMeta *visible_meta) {
    RmRecord visible_record;
    if (!GetVisibleTupleInto(tab_name, rid, txn, &visible_record, visible_meta)) {
        return std::nullopt;
    }
    return visible_record;
}

bool TransactionManager::GetVisibleTupleInto(const std::string &tab_name, const Rid &rid, Transaction *txn,
                                             RmRecord *out_record, TupleMeta *visible_meta,
                                             RmRecordPageCursor *page_cursor) {
    return GetVisibleTupleInto(tab_name, GetTableVersionInfo(tab_name), rid, txn, out_record,
                               visible_meta, page_cursor);
}

bool TransactionManager::GetVisibleTupleInto(const std::string &tab_name,
                                             const std::shared_ptr<TableVersionInfo> &table_info,
                                             const Rid &rid, Transaction *txn, RmRecord *out_record,
                                             TupleMeta *visible_meta, RmRecordPageCursor *page_cursor) {
    bool found = page_cursor != nullptr
                     ? page_cursor->read_record(rid, out_record)
                     : sm_manager_->fhs_.at(tab_name)->read_record(rid, out_record, nullptr);
    if (!found) {
        return false;
    }

    TupleMeta base_meta{0, false};
    std::optional<VersionUndoLink> version_link;
    PageVersionInfo *page_info = GetPageVersionInfoOnTableRaw(table_info, rid.page_no);
    if (page_info != nullptr && rid.slot_no >= 0) {
        std::shared_lock<std::shared_mutex> page_lock(page_info->mutex_);
        const auto slot = static_cast<slot_offset_t>(rid.slot_no);
        if (const TupleMeta *stored_meta = page_info->GetTupleMeta(slot); stored_meta != nullptr) {
            base_meta = *stored_meta;
        }
        if (const VersionUndoLink *stored_version = page_info->GetVersion(slot); stored_version != nullptr) {
            version_link = *stored_version;
        }
    }
    if (tuple_version_visible_to_txn(base_meta, txn)) {
        if (visible_meta != nullptr) {
            *visible_meta = base_meta;
        }
        if (base_meta.is_deleted_) {
            return false;
        }
        return true;
    }

    while (version_link.has_value() && version_link->prev_.IsValid()) {
        auto undo_log = GetUndoLogOptional(version_link->prev_);
        if (!undo_log.has_value()) {
            break;
        }
        TupleMeta undo_meta{undo_log->ts_, undo_log->is_deleted_};
        if (tuple_version_visible_to_txn(undo_meta, txn)) {
            if (visible_meta != nullptr) {
                *visible_meta = undo_meta;
            }
            if (undo_meta.is_deleted_) {
                return false;
            }
            if (undo_log->tuple_image_ != nullptr) {
                *out_record = *undo_log->tuple_image_;
                return true;
            }
            auto reconstructed = ReconstructTuple(&sm_manager_->db_.get_table(tab_name), *out_record, base_meta,
                                                  std::vector<UndoLog>{*undo_log});
            if (!reconstructed.has_value()) {
                return false;
            }
            *out_record = std::move(*reconstructed);
            return true;
        }
        if (!undo_log->prev_version_.IsValid()) {
            break;
        }
        version_link = VersionUndoLink::FromOptionalUndoLink(undo_log->prev_version_);
    }
    return false;
}

TransactionManager::IndexEntryVisibilityState TransactionManager::ClassifySnapshotIndexEntryOnPage(
    PageVersionInfo *page_info, const Rid &rid, Transaction *txn, IndexEntryTupleHint *hint) {
    if (hint != nullptr) {
        hint->Reset();
    }
    if (page_info == nullptr) {
        return IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
    }

    TupleMeta meta{INVALID_TS, false};
    bool has_meta = false;
    bool has_version_link = false;
    {
        std::shared_lock<std::shared_mutex> page_lock(page_info->mutex_);
        if (IsSnapshotPageCleanVisible(page_info, txn)) {
            return IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
        }
        const TupleMeta *stored_meta = rid.slot_no < 0
                                           ? nullptr
                                           : page_info->GetTupleMeta(static_cast<slot_offset_t>(rid.slot_no));
        if (stored_meta != nullptr) {
            meta = *stored_meta;
            has_meta = true;
        }
        const VersionUndoLink *stored_version = rid.slot_no < 0
                                                    ? nullptr
                                                    : page_info->GetVersion(static_cast<slot_offset_t>(rid.slot_no));
        has_version_link = stored_version != nullptr;
        if (hint != nullptr) {
            hint->valid = true;
            hint->page_no = rid.page_no;
            hint->slot_no = rid.slot_no;
            hint->page_info = page_info;
            hint->visibility_epoch = page_info->visibility_epoch_.load(std::memory_order_acquire);
            hint->has_meta = has_meta;
            hint->meta = has_meta ? meta : TupleMeta{0, false};
            hint->has_version_link = has_version_link;
            if (has_version_link) {
                hint->version_link = *stored_version;
            }
        }
    }
    return ClassifySnapshotIndexEntryState(has_meta, meta, has_version_link, txn);
}

TransactionManager::IndexEntryVisibilityState TransactionManager::ClassifySnapshotIndexEntryState(
    bool has_meta, const TupleMeta &meta, bool has_version_link, Transaction *txn) const {
    if (!has_meta) {
        return has_version_link ? IndexEntryVisibilityState::NEEDS_HEAP
                                : IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
    }
    if (meta.ts_ >= TXN_START_ID) {
        txn_id_t owner = meta.ts_ - TXN_START_ID;
        if (txn != nullptr && owner == txn->get_transaction_id()) {
            if (meta.is_deleted_) {
                return IndexEntryVisibilityState::INVISIBLE;
            }
            return has_version_link ? IndexEntryVisibilityState::NEEDS_HEAP
                                    : IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
        }
        return has_version_link ? IndexEntryVisibilityState::NEEDS_HEAP
                                : IndexEntryVisibilityState::INVISIBLE;
    }
    timestamp_t read_ts = txn == nullptr ? lifecycle_state_.last_commit_ts.load() : txn->get_read_ts();
    if (meta.ts_ > read_ts) {
        return has_version_link ? IndexEntryVisibilityState::NEEDS_HEAP
                                : IndexEntryVisibilityState::INVISIBLE;
    }
    return meta.is_deleted_ ? IndexEntryVisibilityState::INVISIBLE
                            : IndexEntryVisibilityState::CURRENT_KEY_VISIBLE;
}

bool TransactionManager::IsSnapshotPageCleanVisible(const PageVersionInfo *page_info, Transaction *txn) const {
    if (page_info == nullptr) {
        return true;
    }
    timestamp_t read_ts = txn == nullptr ? lifecycle_state_.last_commit_ts.load() : txn->get_read_ts();
    return page_info->max_committed_meta_ts_.load(std::memory_order_relaxed) <= read_ts &&
           page_info->uncommitted_meta_count_.load(std::memory_order_relaxed) <= 0 &&
           page_info->deleted_count_.load(std::memory_order_relaxed) <= 0;
}

void TransactionManager::EnsureWriteConflictFree(Transaction *txn, const std::string &tab_name, const Rid &rid) {
    if (txn == nullptr) {
        return;
    }
    TupleMeta current_meta = GetTupleMetaOrDefault(tab_name, rid);
    EnsureWriteConflictFree(txn, current_meta);
}

void TransactionManager::EnsureWriteConflictFree(Transaction *txn, const TupleMeta &current_meta) {
    if (txn == nullptr) {
        return;
    }
    if (current_meta.ts_ >= TXN_START_ID &&
        current_meta.ts_ - TXN_START_ID != txn->get_transaction_id()) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                        AbortSubReason::UNCOMMITTED_WRITE_CONFLICT);
    }
    if (IsWriteWriteConflict(current_meta.ts_, txn)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                        AbortSubReason::STALE_SNAPSHOT_WRITE_CONFLICT);
    }
}

void TransactionManager::EnsureKeyConflictFree(Transaction *txn, const std::string &tab_name, const TabMeta &tab,
                                               const RmRecord &record, const Rid *self_rid) {
    if (txn == nullptr || sm_manager_ == nullptr) {
        return;
    }
    auto bindings = rmdb::bind_table_indexes(sm_manager_, tab_name, tab);
    for (const auto &binding : bindings) {
        const auto &index = *binding.meta;
        if (!index.unique) {
            continue;
        }
        std::string candidate_key = index_key(index, record.data);
        std::vector<Rid> matches;
        binding.ih->get_value(candidate_key.data(), &matches, txn);
        auto historical = LookupSnapshotIndexHistory(tab_name, index, txn->get_read_ts());
        matches.insert(matches.end(), historical.begin(), historical.end());
        auto conflict = ClassifyUniqueIndexConflict(txn, tab_name, index, candidate_key.data(), matches, self_rid);
        if (conflict == UniqueKeyConflictResult::ABORT) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                            AbortSubReason::UNIQUE_KEY_CONFLICT);
        }
    }

    const ColMeta *identity_col = logical_identity_col(tab);
    if (identity_col == nullptr) {
        return;
    }
    bool identity_indexed = std::any_of(tab.indexes.begin(), tab.indexes.end(), [&](const IndexMeta &index) {
        return index.unique && index.col_num == 1 && index.cols.front().name == identity_col->name;
    });
    if (identity_indexed) {
        return;
    }

    const std::vector<std::string> candidate_keys{logical_key(tab, record)};
    auto &fh = sm_manager_->fhs_.at(tab_name);
    for (RmScan scan(fh.get()); !scan.is_end(); scan.next()) {
        Rid rid = scan.rid();
        if (self_rid != nullptr && rid == *self_rid) {
            continue;
        }
        auto base_record = fh->get_record(rid, nullptr);
        TupleMeta base_meta = GetTupleMetaOrDefault(tab_name, rid);
        const std::vector<std::string> base_keys{logical_key(tab, *base_record)};
        auto visible_record = GetVisibleTuple(tab_name, rid, txn);
        bool uncommitted_other = base_meta.ts_ >= TXN_START_ID &&
                                 base_meta.ts_ - TXN_START_ID != txn->get_transaction_id();
        bool newer_committed = base_meta.ts_ < TXN_START_ID && base_meta.ts_ > txn->get_read_ts();
        bool base_visible = tuple_version_visible_to_txn(base_meta, txn);

        if (visible_record.has_value()) {
            const std::vector<std::string> visible_keys{logical_key(tab, *visible_record)};
            if (keys_overlap(candidate_keys, visible_keys)) {
                bool self_owned = base_meta.ts_ >= TXN_START_ID &&
                                  base_meta.ts_ - TXN_START_ID == txn->get_transaction_id();
                if (self_owned && !nonlogical_keys_overlap(candidate_keys, visible_keys)) {
                    continue;
                }
                if (!base_visible || uncommitted_other || newer_committed) {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                                    AbortSubReason::UNIQUE_KEY_CONFLICT);
                }
                continue;
            }
            if (keys_overlap(candidate_keys, base_keys) && (uncommitted_other || newer_committed)) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                                AbortSubReason::UNIQUE_KEY_CONFLICT);
            }
            continue;
        }

        if (!keys_overlap(candidate_keys, base_keys)) {
            continue;
        }
        if (uncommitted_other || newer_committed) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION,
                                            AbortSubReason::UNIQUE_KEY_CONFLICT);
        }
    }
}

void TransactionManager::RecordSnapshotIndexRetirement(const std::string &tab_name, const IndexMeta &index,
                                                        const Rid &rid, timestamp_t retire_ts) {
    snapshot_index_history_.Record(tab_name, snapshot_index_id(index), rid, retire_ts);
}

std::vector<Rid> TransactionManager::LookupSnapshotIndexHistory(const std::string &tab_name,
                                                                const IndexMeta &index,
                                                                timestamp_t read_ts) {
    return snapshot_index_history_.Lookup(tab_name, snapshot_index_id(index), read_ts);
}

void TransactionManager::EraseSnapshotIndexHistory(const std::string &tab_name, const IndexMeta &index) {
    snapshot_index_history_.EraseIndex(tab_name, snapshot_index_id(index));
}

void TransactionManager::EraseSnapshotIndexHistory(const std::string &tab_name) {
    snapshot_index_history_.EraseTable(tab_name);
}

void TransactionManager::ClearSnapshotIndexHistory() {
    snapshot_index_history_.Clear();
}

TransactionManager::UniqueKeyConflictResult TransactionManager::ClassifyUniqueIndexConflict(
    Transaction *txn, const std::string &tab_name, const IndexMeta &index, const char *key,
    const std::vector<Rid> &matches, const Rid *self_rid) {
    if (!index.unique) {
        return UniqueKeyConflictResult::NONE;
    }
    if (matches.empty()) {
        return UniqueKeyConflictResult::NONE;
    }
    std::string candidate_key(key, index.col_tot_len);
    if (txn == nullptr || sm_manager_ == nullptr) {
        for (const auto &rid : matches) {
            if (self_rid == nullptr || rid != *self_rid) {
                return UniqueKeyConflictResult::FAILURE;
            }
        }
        return UniqueKeyConflictResult::NONE;
    }

    auto &fh = sm_manager_->fhs_.at(tab_name);
    for (const auto &rid : matches) {
        if (self_rid != nullptr && rid == *self_rid) {
            continue;
        }
        auto base_record = fh->get_record(rid, nullptr);
        TupleMeta base_meta = GetTupleMetaOrDefault(tab_name, rid);
        auto visible_record = GetVisibleTuple(tab_name, rid, txn);

        bool uncommitted_other = base_meta.ts_ >= TXN_START_ID &&
                                 base_meta.ts_ - TXN_START_ID != txn->get_transaction_id();
        bool newer_committed = base_meta.ts_ < TXN_START_ID && base_meta.ts_ > txn->get_read_ts();
        std::string base_key = raw_index_key(index, base_record->data);
        bool base_visible = tuple_version_visible_to_txn(base_meta, txn);

        if (visible_record.has_value()) {
            std::string visible_key = raw_index_key(index, visible_record->data);
            if (visible_key == candidate_key) {
                if (base_visible) {
                    return UniqueKeyConflictResult::FAILURE;
                }
                return UniqueKeyConflictResult::ABORT;
            }
            if (base_key == candidate_key && (uncommitted_other || newer_committed)) {
                return UniqueKeyConflictResult::ABORT;
            }
            continue;
        }

        if (base_key == candidate_key && (uncommitted_other || newer_committed)) {
            return UniqueKeyConflictResult::ABORT;
        }
        return UniqueKeyConflictResult::NONE;
    }
    return UniqueKeyConflictResult::NONE;
}


SerializableMetadataRetention::SerializableMetadataRetention(const TransactionRegistry::AllView &transactions) {
    transactions.ForEach([&](txn_id_t, Transaction *txn) {
        if (txn == nullptr || txn->get_state() != TransactionState::GROWING ||
            txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
            return;
        }
        has_active_serializable_ = true;
        if (oldest_active_start_ts_ == INVALID_TS || txn->get_start_ts() < oldest_active_start_ts_) {
            oldest_active_start_ts_ = txn->get_start_ts();
        }
    });
}

void SerializableMetadataRetention::ReleaseIfSafe(txn_id_t txn_id, Transaction *txn,
                                                  TransactionRegistry::AllView &transactions) const {
    if (txn == nullptr || txn->ssi_metadata_released()) {
        return;
    }
    bool aborted = txn->get_state() == TransactionState::ABORTED;
    bool snapshot_overlap_done = !has_active_serializable_ ||
        (txn->get_commit_ts() != INVALID_TS && txn->get_commit_ts() <= oldest_active_start_ts_);
    if (!aborted && !snapshot_overlap_done) {
        return;
    }
    transactions.ForEach([&](txn_id_t, Transaction *other) {
        if (other != nullptr && other != txn) {
            other->remove_rw_dependency(txn_id);
        }
    });
    txn->clear_serializable_state();
    txn->set_ssi_metadata_released(true);
}


#include <algorithm>
#include <functional>

namespace rmdb {

size_t SnapshotIndexHistoryStore::KeyHash::operator()(const Key &key) const {
    size_t seed = std::hash<std::string>{}(key.table_name);
    size_t value = std::hash<rmdb::u64>{}(key.index_id);
    return seed ^ (value + 0x9e3779b9U + (seed << 6U) + (seed >> 2U));
}

size_t SnapshotIndexHistoryStore::shard_index(const Key &key) const {
    return KeyHash{}(key) % kShardCount;
}

void SnapshotIndexHistoryStore::Record(const std::string &table_name, rmdb::u64 index_id,
                                       const Rid &rid, timestamp_t retire_ts) {
    if (retire_ts == INVALID_TS) {
        return;
    }
    Key key{table_name, index_id};
    Shard &shard = shards_[shard_index(key)];
    std::lock_guard<std::mutex> guard(shard.mutex);
    auto &entries = shard.entries[key];
    auto insert_at = std::upper_bound(entries.begin(), entries.end(), retire_ts,
                                      [](timestamp_t ts, const Entry &entry) { return ts < entry.retire_ts; });
    entries.insert(insert_at, Entry{retire_ts, rid});
}

std::vector<Rid> SnapshotIndexHistoryStore::Lookup(const std::string &table_name,
                                                   rmdb::u64 index_id,
                                                   timestamp_t read_ts) {
    Key key{table_name, index_id};
    Shard &shard = shards_[shard_index(key)];
    std::lock_guard<std::mutex> guard(shard.mutex);
    auto iter = shard.entries.find(key);
    if (iter == shard.entries.end()) {
        return {};
    }
    const auto &entries = iter->second;
    auto first = std::upper_bound(entries.begin(), entries.end(), read_ts,
                                  [](timestamp_t ts, const Entry &entry) { return ts < entry.retire_ts; });
    std::vector<Rid> rids;
    rids.reserve(static_cast<size_t>(entries.end() - first));
    for (; first != entries.end(); ++first) {
        rids.push_back(first->rid);
    }
    return rids;
}

void SnapshotIndexHistoryStore::Purge(timestamp_t watermark) {
    for (auto &shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        for (auto iter = shard.entries.begin(); iter != shard.entries.end();) {
            auto &entries = iter->second;
            auto first_live = std::upper_bound(entries.begin(), entries.end(), watermark,
                                               [](timestamp_t ts, const Entry &entry) {
                                                   return ts < entry.retire_ts;
                                               });
            entries.erase(entries.begin(), first_live);
            if (entries.empty()) {
                iter = shard.entries.erase(iter);
            } else {
                ++iter;
            }
        }
    }
}

void SnapshotIndexHistoryStore::EraseIndex(const std::string &table_name,
                                           rmdb::u64 index_id) {
    Key key{table_name, index_id};
    Shard &shard = shards_[shard_index(key)];
    std::lock_guard<std::mutex> guard(shard.mutex);
    shard.entries.erase(key);
}

void SnapshotIndexHistoryStore::EraseTable(const std::string &table_name) {
    for (auto &shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        for (auto iter = shard.entries.begin(); iter != shard.entries.end();) {
            if (iter->first.table_name == table_name) {
                iter = shard.entries.erase(iter);
            } else {
                ++iter;
            }
        }
    }
}

void SnapshotIndexHistoryStore::Clear() {
    for (auto &shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        shard.entries.clear();
    }
}

size_t SnapshotIndexHistoryStore::EntryCountForTest() {
    size_t count = 0;
    for (auto &shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        for (const auto &entry : shard.entries) {
            count += entry.second.size();
        }
    }
    return count;
}

}  // namespace rmdb

