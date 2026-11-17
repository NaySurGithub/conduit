#pragma once
#include <conduit/types.hpp>

#include <array>
#include <functional>
#include <mutex>
#include <vector>

namespace conduit {
    using queue_serials = std::array<uint64_t, queue_count>;

    // released once every queue has finished the work submitted before destroy(); defer() holds a release back
    // until seal() stamps it with the serials of the frame it was destroyed in, so a resource destroyed while
    // a command list that uses it is still being recorded outlives that list
    class retire_queue {
    public:
        void push(const queue_serials& submitted, std::function<void()> release) {
            std::lock_guard lock(mutex_);
            items_.push_back({submitted, std::move(release)});
        }

        void defer(std::function<void()> release) {
            std::lock_guard lock(mutex_);
            open_.push_back(std::move(release));
        }

        void seal(const queue_serials& submitted) {
            std::lock_guard lock(mutex_);
            for (auto& release : open_)
                items_.push_back({submitted, std::move(release)});
            open_.clear();
        }

        void collect(const queue_serials& completed) {
            std::vector<std::function<void()>> running;
            {
                std::lock_guard lock(mutex_);
                if (items_.empty())
                    return;
                ready_.clear();
                size_t kept = 0;
                for (size_t i = 0; i < items_.size(); i++) {
                    if (covered(items_[i].serials, completed)) {
                        ready_.push_back(std::move(items_[i].release));
                    } else {
                        if (kept != i)
                            items_[kept] = std::move(items_[i]);
                        kept++;
                    }
                }
                items_.resize(kept);
                if (ready_.empty())
                    return;
                running.swap(ready_);
            }
            for (auto& fn : running)
                fn();
        }

        void flush_all() {
            std::vector<retire_queue::item> all;
            std::vector<std::function<void()>> open;
            {
                std::lock_guard lock(mutex_);
                all.swap(items_);
                open.swap(open_);
            }
            for (auto& item : all)
                item.release();
            for (auto& release : open)
                release();
        }

        size_t pending() const {
            std::lock_guard lock(mutex_);
            return items_.size() + open_.size();
        }

    private:
        struct item {
            queue_serials serials;
            std::function<void()> release;
        };

        static bool covered(const queue_serials& need, const queue_serials& done) {
            for (size_t i = 0; i < queue_count; i++)
                if (done[i] < need[i])
                    return false;
            return true;
        }

        mutable std::mutex mutex_;
        std::vector<item> items_;
        std::vector<std::function<void()>> open_, ready_;
    };
}
