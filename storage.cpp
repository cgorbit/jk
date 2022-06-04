#include "storage.h"
#include "volume.h"

#include <stack>
#include <cassert>
#include <string_view>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>

namespace NJK {

    class TStorage::TImpl {
    public:
        using TInode = TVolume::TInode;

        TImpl(TVolume* rootVolume, const std::string& rootDir) {
            auto tmp = ResolveInVolumePath(rootVolume, rootDir); // FIXME Better?
            // TODO Locking here
            auto& inodeData = InodeCache_[{rootVolume, tmp.Id}];
            inodeData.Inode = std::move(tmp);
            inodeData.Loaded = true;
            inodeData.DentryCount = 1;

            Root_.Volume = rootVolume;
            Root_.Dentry.reset(new TDentry());
            Root_.Dentry->InParentName = "/";
            Root_.Dentry->InodeId = inodeData.Inode.Id;
            Root_.Dentry->InodeData = &inodeData;
        }

        void Set(const std::string& path, const TValue& value, ui32 deadline) {
            (void)deadline; // TODO
            auto node = ResolvePath(path, true);
            Y_VERIFY(node.Dentry);
            EnsureInodeData(node);
            TVolume::TInodeDataOps ops(node.Volume);
            ops.SetValue(node.Dentry->InodeData->Inode, value);
        }

        TValue Get(const std::string& path) {
            auto node = ResolvePath(path, false);
            if (!node.Dentry) {
                return {};
            }
            EnsureInodeData(node);
            TVolume::TInodeDataOps ops(node.Volume);
            return ops.GetValue(node.Dentry->InodeData->Inode);
        }

        void Remove(const std::string& path) {
        }

        void Mount(const std::string& mountPointPath, TVolume* srcVolume, const std::string& srcDir);

    private:
        struct TDentry;

        struct TResolveParams {
            bool Create = false;
            bool MergeIntermediate = false;
        };

        struct TFullInodeId {
            TVolume* Volume{};
            TInode::TId InodeId{};

            bool operator== (const TFullInodeId& other) const {
                return Volume == other.Volume
                    && InodeId == other.InodeId;
            }
        };

        struct TDentryWithVolume {
            TVolume* Volume{};
            TDentry* Dentry{};

            operator TFullInodeId() const {
                return {Volume, Dentry->InodeId};
            }
        };

        TDentryWithVolume ResolvePath(const std::string& path, bool create);
        TDentryWithVolume ResolveDirs(const std::string_view& path, const TResolveParams&);
        TVolume::TInode ResolveInVolumePath(TVolume* volume, const std::string& path);
        TDentry* StepPath(const TDentryWithVolume parent, const std::string& childName, const TResolveParams&);
        void EnsureInodeData(TDentryWithVolume node);

    private:
        struct TInodeData {
            std::shared_mutex Lock;
            std::condition_variable CondVar;
            bool Loaded = false;
            size_t DentryCount = 0;
            TInode Inode;
        };

        struct TMount {
            TVolume* Volume{};
            std::unique_ptr<TDentry> Dentry;

            operator TDentryWithVolume () const {
                return {Volume, Dentry.get()};
            }
        };

        struct TDentry {
            std::shared_mutex Lock;
            std::condition_variable CondVar;
            std::string InParentName; // for debug: remove or replace by unique ptr?
            TInode::TId InodeId{};
            TInodeData* InodeData{};
            std::unique_ptr<std::vector<TMount>> Mounts;
        };

        struct TDentryCacheKey {
            TFullInodeId ParentInode;
            std::string ChildName;

            bool operator== (const TDentryCacheKey& other) const {
                return ParentInode == other.ParentInode
                    && ChildName == other.ChildName;
            }
        };

        // 1. DentryCache -- это перемещение только внутри одного TVolume
        // 2. Нельзя лукапить рутовые dentry через DentryCache
        // 3. нужно смотреть в один и тот же Dentry или Inode*, когда один и тот же inode маунтится в несколько мест
        // 4. 

        struct TFullInodeIdHash {
            size_t operator() (const TFullInodeId& key) const {
                auto h0 = std::hash<TVolume*>{}(key.Volume);
                auto h1 = std::hash<TInode::TId>{}(key.InodeId);
                return h0 ^ (h1 << 1);
            }
        };

        struct TDentryKeyHash {
            size_t operator() (const TDentryCacheKey& key) const {
                auto h0 = TFullInodeIdHash{}(key.ParentInode);
                auto h1 = std::hash<std::string>{}(key.ChildName);
                return h0 ^ (h1 << 1);
            }
        };

        std::shared_mutex DentryCacheLock_;
        TMount Root_;
        std::unordered_map<TDentryCacheKey, TDentry, TDentryKeyHash> DentryCache_;

        std::shared_mutex InodeCacheLock_;
        std::unordered_map<TFullInodeId, TInodeData, TFullInodeIdHash> InodeCache_;
    };

    TStorage::TImpl::TDentryWithVolume TStorage::TImpl::ResolvePath(const std::string& path, bool create) {
        if (path.empty()) {
            throw std::runtime_error("path is empty");
        }
        if (path[0] != '/') {
            throw std::runtime_error("path didn't start from the root");
        }

        std::string_view dirPath;
        std::string keyName;
        {
            auto b = path.rbegin();
            auto e = path.rend();
            while (b != e && *b == '/') {
                ++b;
            }
            if (b == e) {
                throw std::runtime_error("path contains only slash");
            }
            auto keyEnd = b.base();

            while (b != e && *b != '/') {
                ++b;
            }
            assert(b != e);

            //static_assert(std::is_same_v<decltype(path.begin()), std::string::iterator>);

            keyName = {b.base(), keyEnd};
            dirPath = std::string_view(&*path.begin(), std::distance(path.begin(), b.base()));
        }

        auto dir = ResolveDirs(dirPath, {.Create = create});
        if (!dir.Dentry) {
            Y_ENSURE(!create)
            return {};
        }
        if (dir.Dentry->Mounts) {
            //for (const auto& mount : *dir.Dentry->Mounts) { // TODO REVERT ORDER
            const auto& mounts = *dir.Dentry->Mounts;
            for (auto it = mounts.rbegin(); it != mounts.rend(); ++it) {
                const TDentryWithVolume mount = *it;
                auto* dentry = StepPath(mount, keyName, {.Create = false});
                if (dentry) {
                    return {mount.Volume, dentry};
                }
            }

            if (!create) {
                return {};
            }

            const TDentryWithVolume target = dir.Dentry->Mounts->back();
            auto* dentry = StepPath(target, keyName, {.Create = true});
            if (dentry) {
                return {target.Volume, dentry};
            }

            return {};
        }

        auto* dentry = StepPath(dir, keyName, {.Create = create});
        if (!dentry) {
            return {};
        }
        return {dir.Volume, dentry};
    }

    void TStorage::TImpl::EnsureInodeData(TDentryWithVolume node) {
        if (node.Dentry->InodeData) {
            return;
        }
        TInodeData* inodeData = &InodeCache_[node];
        if (!inodeData->Loaded) {
            inodeData->Inode = node.Volume->ReadInode(node.Dentry->InodeId);
            inodeData->Loaded = true;
        }
        node.Dentry->InodeData = inodeData;
    }

    // TODO Rename
    TStorage::TImpl::TDentry* TStorage::TImpl::StepPath(const TDentryWithVolume parent, const std::string& childName, const TResolveParams& params) {
        auto* dentry = parent.Dentry;
        auto* volume = parent.Volume;

        const TDentryCacheKey cacheKey{{volume, dentry->InodeId}, childName};
        if (auto dit = DentryCache_.find(cacheKey); dit != DentryCache_.end()) {
            return &dit->second;
        }

        EnsureInodeData(parent);
        TInodeData* inodeData = dentry->InodeData;

        TInodeData* childInodeData{};

        TVolume::TInodeDataOps ops(volume);
        auto addChildInode = [this, &childInodeData, volume](TInode* inode) {
            TInodeData& inodeData = InodeCache_[{volume, inode->Id}];
            childInodeData = &inodeData;
            inodeData.Loaded = true;
            inodeData.Inode = std::move(*inode);
        };
        if (params.Create) {
            auto childInode = ops.EnsureChild(inodeData->Inode, childName);
            addChildInode(&childInode);
        } else {
            auto childInode = ops.LookupChild(inodeData->Inode, childName);
            if (!childInode) {
                return nullptr;
            }
            addChildInode(&*childInode);
        }

        auto& childDentry = DentryCache_[cacheKey];
        childDentry.InParentName = childName; // DEBUG TODO
        childDentry.InodeId = childInodeData->Inode.Id;
        childDentry.InodeData = childInodeData;

        return &childDentry;
    }

    TStorage::TImpl::TDentryWithVolume TStorage::TImpl::ResolveDirs(const std::string_view& path, const TResolveParams& params) {
        Y_ENSURE(!path.empty() && path[0] == '/');

        auto start = path.begin();
        while (start != path.end() && *start == '/') {
            ++start;
        }

        TDentryWithVolume cur = Root_;

        //TVolume::TInodeDataOps ops(volume);

        while (start != path.end()) {
            // Follow last mount
            if (cur.Dentry->Mounts) {
                cur = cur.Dentry->Mounts->back();
            }

            auto end = start;
            while (end != path.end() && *end != '/') {
                ++end;
            }
            Y_ENSURE(start != end);

            const std::string childName(start, end); // FIXME string_view
            cur.Dentry = StepPath(cur, childName, params);
            if (!cur.Dentry) {
                return {};
            }

            start = end;
            while (start != path.end() && *start == '/') {
                ++start;
            }
        }

        return cur;
    }

    TVolume::TInode TStorage::TImpl::ResolveInVolumePath(TVolume* volume, const std::string& path) {
        Y_ENSURE(!path.empty() && path[0] == '/');

        auto cur = volume->GetRoot();
        auto start = path.begin();

        while (start != path.end() && *start == '/') {
            ++start;
        }

        TVolume::TInodeDataOps ops(volume);

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

    void TStorage::TImpl::Mount(const std::string& mountPointPath, TVolume* srcVolume, const std::string& srcDir) {
        // XXX It's okay to ignore caches in ResolveInVolumePath if we mount all before user mutating requests
        auto srcInode = ResolveInVolumePath(srcVolume, srcDir);

        auto mountPoint = ResolveDirs(mountPointPath, {.Create = true});
        if (!mountPoint.Dentry->Mounts) {
            mountPoint.Dentry->Mounts.reset(new std::vector<TMount>());
        }
        auto& mount = mountPoint.Dentry->Mounts->emplace_back();
        mount.Volume = srcVolume;
        mount.Dentry = std::make_unique<TDentry>();
        mount.Dentry->InParentName = "this is mount"; // TODO More info
        mount.Dentry->InodeId = srcInode.Id;

        // TODO FIXME Update InodeCache here
    }

    TStorage::TStorage(TVolume* rootVolume, const std::string& rootDir)
        : Impl_(new TImpl(rootVolume, rootDir))
    {
    }

    TStorage::~TStorage() = default;

    void TStorage::Set(const std::string& path, const TValue& value, ui32 deadline) {
        Impl_->Set(path, value, deadline);
    }

    void TStorage::Mount(const std::string& mountPoint, TVolume* src, const std::string& srcDir) {
        Impl_->Mount(mountPoint, src, srcDir);
    }

    TStorage::TValue TStorage::Get(const std::string& path) {
        return Impl_->Get(path);
    }

}