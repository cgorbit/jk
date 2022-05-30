#include "storage.h"
#include "volume.h"

namespace NJK {

    class TStorage::TImpl {
    public:
        TImpl(std::initializer_list<std::string> volumes)
            : Volume_(*volumes.begin(), {}, true)
        {
        }

        void Set(const std::string& path, const TValue& value) {
            auto inode = ResolvePath(path);
            TVolume::TInodeDataOps ops(*Volume_.MetaGroups_[0]->BlockGroups[0]);
            ops.SetValue(inode, value);
        }

        TValue Get(const std::string& path) {
            auto inode = ResolvePath(path);
            TVolume::TInodeDataOps ops(*Volume_.MetaGroups_[0]->BlockGroups[0]);
            return ops.GetValue(inode);
        }

    private:
        TVolume::TInode ResolvePath(const std::string& path) {
            Y_ENSURE(!path.empty() && path[0] == '/');

            auto cur = Volume_.GetRoot();
            auto start = path.begin();

            while (start != path.end() && *start == '/') {
                ++start;
            }

            // TODO
            TVolume::TInodeDataOps ops(*Volume_.MetaGroups_[0]->BlockGroups[0]);

            while (start != path.end()) {
                auto end = start;
                while (end != path.end() && *end != '/') {
                    ++end;
                }

                Y_ENSURE(start != end);
                const std::string token(start, end);
                cur = ops.EnsureChild(cur, token);

                start = end;
                while (start != path.end() && *start == '/') {
                    ++start;
                }
            }

            return cur;
        }

    private:
        TVolume Volume_;
    };

    TStorage::TStorage(std::initializer_list<std::string> volumes)
        : Impl_(new TImpl(volumes))
    {
    }

    TStorage::~TStorage() = default;

    void TStorage::Set(const std::string& path, const TValue& value) {
        Impl_->Set(path, value);
    }

    TStorage::TValue TStorage::Get(const std::string& path) {
        return Impl_->Get(path);
    }

}