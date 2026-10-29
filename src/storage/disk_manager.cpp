/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <algorithm>
#include <cerrno>
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>

#include "defs.h"
#include "common/common.h"

namespace {
void write_all_at(int fd, const char *data, size_t num_bytes, off_t offset, const char *context) {
    size_t bytes_written = 0;
    while (bytes_written < num_bytes) {
        ssize_t result = pwrite(fd, data + bytes_written, num_bytes - bytes_written,
                                offset + static_cast<off_t>(bytes_written));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw UnixError();
        }
        if (result == 0) {
            throw InternalError(context);
        }
        bytes_written += static_cast<size_t>(result);
    }
}

size_t read_at_most(int fd, char *data, size_t num_bytes, off_t offset) {
    size_t bytes_read = 0;
    while (bytes_read < num_bytes) {
        ssize_t result = pread(fd, data + bytes_read, num_bytes - bytes_read,
                               offset + static_cast<off_t>(bytes_read));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw UnixError();
        }
        if (result == 0) {
            break;
        }
        bytes_read += static_cast<size_t>(result);
    }
    return bytes_read;
}

void sync_fd(int fd) {
    while (fdatasync(fd) < 0) {
        if (errno == EINTR) {
            continue;
        }
        throw UnixError();
    }
}

void truncate_fd(int fd, off_t size) {
    while (ftruncate(fd, size) < 0) {
        if (errno == EINTR) {
            continue;
        }
        throw UnixError();
    }
}
}  // namespace

DiskManager::DiskManager() {
    for (auto &next_page_no : fd2pageno_) {
        next_page_no.store(0, std::memory_order_relaxed);
    }
}

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    off_t file_offset = static_cast<off_t>(page_no) * PAGE_SIZE;
    write_all_at(fd, offset, num_bytes, file_offset, "DiskManager::write_page Error");
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordDiskWrite(static_cast<size_t>(num_bytes), false);
    }
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    off_t file_offset = static_cast<off_t>(page_no) * PAGE_SIZE;
    size_t bytes_read = read_at_most(fd, offset, static_cast<size_t>(num_bytes), file_offset);
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordDiskRead(bytes_read, false);
    }
    if (bytes_read < static_cast<size_t>(num_bytes)) {
        memset(offset + bytes_read, 0, static_cast<size_t>(num_bytes) - bytes_read);
    }
}

size_t DiskManager::read_file_range(int fd, rmdb::i64 offset, char *data, size_t num_bytes) {
    if (offset < 0) {
        throw InternalError("DiskManager::read_file_range negative offset");
    }
    size_t bytes_read = read_at_most(fd, data, num_bytes, static_cast<off_t>(offset));
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordDiskRead(bytes_read, false);
    }
    return bytes_read;
}

void DiskManager::write_file_range(int fd, rmdb::i64 offset, const char *data, size_t num_bytes) {
    if (offset < 0) {
        throw InternalError("DiskManager::write_file_range negative offset");
    }
    write_all_at(fd, data, num_bytes, static_cast<off_t>(offset),
                 "DiskManager::write_file_range Error");
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordDiskWrite(num_bytes, false);
    }
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    if (fd < 0 || fd >= MAX_FD) {
        throw FileNotOpenError(fd);
    }
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true
 * @param {string} &path 指定路径文件
 */
bool DiskManager::is_file(const std::string &path) {
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string &path) {
    if (is_file(path)) {
        throw FileExistsError(path);
    }
    int fd = open(path.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
    if (fd == -1) {
        throw UnixError();
    }
    if (close(fd) == -1) {
        throw UnixError();
    }
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string &path) {
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    if (path2fd_.count(path)) {
        throw FileNotClosedError(path);
    }
    if (unlink(path.c_str()) == -1) {
        throw UnixError();
    }
}


/**
 * @description: 打开指定路径文件
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string &path) {
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    if (path2fd_.count(path)) {
        throw FileNotClosedError(path);
    }
    int fd = open(path.c_str(), O_RDWR);
    if (fd == -1) {
        throw UnixError();
    }
    if (fd >= MAX_FD) {
        close(fd);
        throw InternalError("DiskManager file descriptor exceeds page allocator capacity");
    }
    path2fd_[path] = fd;
    fd2path_[fd] = path;
    fd2pageno_[fd] = static_cast<page_id_t>((get_file_size(path) + PAGE_SIZE - 1) / PAGE_SIZE);
    return fd;
}

/**
 * @description:用于关闭指定路径文件
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    std::string path = fd2path_[fd];
    if (close(fd) == -1) {
        throw UnixError();
    }
    fd2path_.erase(fd);
    path2fd_.erase(path);
    if (log_fd_ == fd) {
        log_fd_ = -1;
    }
}


/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
rmdb::i64 DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}


/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
size_t DiskManager::read_log(char *log_data, size_t size, lsn_t offset) {
    if (offset < 0) {
        return 0;
    }
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    rmdb::i64 file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return 0;
    }

    size = std::min<size_t>(size, static_cast<size_t>(file_size - offset));
    if (size == 0) return 0;
    size_t bytes_read = read_at_most(log_fd_, log_data, size, static_cast<off_t>(offset));
    if (bytes_read != size) {
        throw InternalError("DiskManager::read_log incomplete read");
    }
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordDiskRead(bytes_read, true);
    }
    return bytes_read;
}


/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(const char *log_data, size_t size, lsn_t offset) {
    if (offset < 0) {
        throw InternalError("DiskManager::write_log negative offset");
    }
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    write_all_at(log_fd_, log_data, size, static_cast<off_t>(offset), "DiskManager::write_log Error");
    if (perf_stats_ != nullptr) {
        perf_stats_->RecordDiskWrite(size, true);
    }
}

int DiskManager::ensure_log_fd() {
    if (log_fd_ == -1) {
        log_fd_ = get_file_fd(LOG_FILE_NAME);
    }
    return log_fd_;
}

void DiskManager::sync_file(int fd) {
    if (fd < 0) {
        throw InternalError("DiskManager::sync_file invalid file descriptor");
    }
    sync_fd(fd);
}

void DiskManager::sync_log() {
    sync_fd(ensure_log_fd());
}

void DiskManager::sync_all_data_files() {
    for (const auto &entry : fd2path_) {
        if (entry.first == log_fd_ || entry.second == LOG_FILE_NAME) {
            continue;
        }
        sync_fd(entry.first);
    }
}

void DiskManager::truncate_file(int fd, rmdb::i64 size) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    if (size < 0) {
        throw InternalError("negative file truncate size");
    }
    truncate_fd(fd, static_cast<off_t>(size));
    fd2pageno_[fd] = static_cast<page_id_t>((size + PAGE_SIZE - 1) / PAGE_SIZE);
}

void DiskManager::truncate_log() {
    truncate_fd(ensure_log_fd(), 0);
}

void DiskManager::remove_file_if_exists(const std::string &path) {
    while (unlink(path.c_str()) < 0) {
        if (errno == EINTR) {
            continue;
        }
        if (errno == ENOENT) {
            return;
        }
        throw UnixError();
    }
}
