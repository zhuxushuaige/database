/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstddef>

#include "common/config.h"

// A replacement policy observes only frame lifecycle events. Concrete
// policies own all queueing and synchronization details.
class Replacer {
   public:
    using frame_type = frame_id_t;
    using size_type = std::size_t;

    virtual ~Replacer() = default;

    Replacer(const Replacer &) = delete;
    Replacer &operator=(const Replacer &) = delete;
    Replacer(Replacer &&) = delete;
    Replacer &operator=(Replacer &&) = delete;

    [[nodiscard]] virtual bool victim(frame_type *result) = 0;
    virtual void pin(frame_type frame) = 0;
    virtual void unpin(frame_type frame) = 0;

    // Policies without a hot/cold distinction retain the ordinary admission
    // behavior through this default implementation.
    virtual void unpin_cold(frame_type frame) { unpin(frame); }

    [[nodiscard]] virtual size_type Size() = 0;

   protected:
    Replacer() = default;
};
