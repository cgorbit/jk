#include "storage.h"
#include "hash_map.h"
#include "volume.h"
#include "volume/ops.h"

#include <stack>
#include <cassert>
#include <string_view>
#include <sstream>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>

template <typename T>
T CombineHashes(T l, T r) {
    return std::hash<T>{}(l) ^ r;
    //return l ^ (r << 1);
}

namespace NJK {

    using NVolume::TInodeDataOps;

    class TStorage::TImpl {
    public:
        using TInode = TVolume::TInode;

        TImpl(TVolume* rootVolume, const std::string& rootDir) {
            Root_.Volume = rootVolume;
            Root_.Dentry = EnsureMountedInode(rootVolume, rootDir);
        }

        void Set(const std::string& path, const TValue& value, ui32 deadline) {
            (void)deadline; // TODO
            auto node = ResolvePath(path, true);
            Y_VERIFY(node.Dentry);
            node.Dentry->SetValue(value, deadline);
        }

        TValue Get(const std::string& path) {
            auto node = ResolvePath(path, false);
            if (!node.Dentry) {
                return {};
            }
            return node.Dentry->GetValue();
        }

        void Erase(const std::string& path) {
            auto node = ResolvePath(path, false);
            if (!node.Dentry) {
                return;
            }
            return node.Dentry->UnsetValue();
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

    private:
        //struct TInodeData {
        //    std::atomic<size_t> Ready{0};
        //    std::shared_mutex Lock;
        //    //std::condition_variable CondVar;
        //    size_t DentryCount = 0;
        //    TInode Inode;
        //};

        struct TMount {
            TVolume* Volume{};
            TDentry* Dentry{};
        };

        class TChildNameLockGuard {
        public:
            TChildNameLockGuard(TDentry* dentry, std::string name);
            TChildNameLockGuard(TChildNameLockGuard&) = delete;
            TChildNameLockGuard(TChildNameLockGuard&&) noexcept;
            ~TChildNameLockGuard();

            TChildNameLockGuard& operator= (const TChildNameLockGuard&) = delete;
            TChildNameLockGuard& operator= (TChildNameLockGuard&&) noexcept;

            const std::string& Name() const {
                return Name_;
            }

            const TDentry* Dentry() const {
                return Dentry_;
            }

            void Swap(TChildNameLockGuard& other) noexcept;

        private:
            TDentry* Dentry_{};
            std::string Name_;
        };

        struct TDentry {
            enum class EState {
                Uninitialized,
                Exists,
                NotExists,
            };
                
            TNaiveSpinLock Lock;

            EState State = EState::Uninitialized;
            TCondVar InitCondVar;

            bool CreateLocked = false;
            TCondVar CreateCondVar;

            size_t DirReadLocked = 0;
            // Serialize concurrent directory structure modification
            // TODO DirWriteLocked is to slow if we wait each modification to disk write
            bool DirWriteLocked = false;
            TCondVar DirWriteUnlockedCondVar;

            size_t ValueReadLocked = 0;
            size_t ValueWriteLocked = 0;
            TCondVar ValueWriteUnlockedCondVar;

            // Prevent from Exists to NotExists
            size_t PreventRemoval = 0;
            TCondVar PreventRemovalCondVar;

            TVolume* Volume{};

            //TDentry* Parent{};
            std::string InParentName; // for debug

            std::vector<std::string> ChildrenLocks;
            TCondVar ChildrenLocksCondVar;

            //TInode::TId InodeId{};
            std::unique_ptr<TInode> Inode;

            std::unique_ptr<std::vector<TMount>> Mounts;

            [[nodiscard]] auto LockGuard() {
                return MakeGuard(Lock);
            }

            void WaitInitialized() {
                auto g = LockGuard();
                while (State == EState::Uninitialized) {
                    InitCondVar.Wait(Lock);
                }
            }

            // returns guard
            [[nodiscard]] TChildNameLockGuard LockChild(const std::string& name);
            void UnlockChild(const std::string& name);

            std::unique_ptr<TInode> EnsureChild(const std::string& name, const TChildNameLockGuard& g) {
                Y_ENSURE(g.Name() == name && g.Dentry() == this);

                auto inode = LookupChild(name, g);
                if (inode) {
                    return inode;
                }

                TInode ret;
                {
                    TODO("Write Data into new Data Block and then Swap fast")

                    LockDirForWrite();
                    Y_DEFER([this] {
                        UnlockDirForWrite();
                    });

TODO_BETTER_CONCURRENCY
                    auto g = LockGuard();
                    TInodeDataOps ops(Volume);
                    ret = ops.EnsureChild(*Inode, name);
                }
                return std::make_unique<TInode>(std::move(ret));
            }

            std::unique_ptr<TInode> LookupChild(const std::string& name, const TChildNameLockGuard& g) {
                Y_ENSURE(g.Name() == name && g.Dentry() == this);
                
                LockDirForRead();
                Y_DEFER([this] {
                    UnlockDirForRead();
                });

                std::optional<TInode> ret;
                {
TODO_BETTER_CONCURRENCY
                    auto g = LockGuard();
                    TInodeDataOps ops(Volume);
                    ret = ops.LookupChild(*Inode, name);
                    if (!ret) {
                        return {};
                    }
                }
                return std::make_unique<TInode>(std::move(*ret));
            }

            /////////////////////////////////////////////////////////////////
            TODO("1. Combine Dir and Value in TInode.Data")
            TODO("2. Don't RdWrLock all this. Write Dir/Value in new Data Blocks and then replace")
            TODO("3. Store small TValue in TDentry itself")
            /////////////////////////////////////////////////////////////////

            void LockDirForRead() {
                auto g = LockGuard();
                while (DirWriteLocked) {
                    DirWriteUnlockedCondVar.Wait(Lock);
                }
                ++DirReadLocked;
            }

            void UnlockDirForRead() {
                bool notify = false;
                {
                    auto g = LockGuard();
                    if (--DirReadLocked == 0) {
                        notify = true;
                    }
                }
                DirWriteUnlockedCondVar.NotifyAll(); // FIXME
            }

            void LockDirForWrite() {
                TODO("Write Data into new Data Block and then Swap fast")

                auto g = LockGuard();
                while (DirReadLocked || DirWriteLocked) {
                    DirWriteUnlockedCondVar.Wait(Lock);
                }
                DirWriteLocked = true;
            }

            void UnlockDirForWrite() {
                {
                    auto g = LockGuard();
                    DirWriteLocked = false;
                }
                DirWriteUnlockedCondVar.NotifyAll(); // FIXME
            }

            /////////////////////////////////////////////////////////////////

            void LockValueForRead() {
                auto g = LockGuard();
                while (ValueWriteLocked) {
                    ValueWriteUnlockedCondVar.Wait(Lock);
                }
                ++ValueReadLocked;
            }

            void UnlockValueForRead() {
                bool notify = false;
                {
                    auto g = LockGuard();
                    if (--ValueReadLocked == 0) {
                        notify = true;
                    }
                }
                ValueWriteUnlockedCondVar.NotifyAll(); // FIXME
            }

            void LockValueForWrite() {
                TODO("Write Data into new Data Block and then Swap fast")

                auto g = LockGuard();
                while (ValueReadLocked || ValueWriteLocked) {
                    ValueWriteUnlockedCondVar.Wait(Lock);
                }
                ValueWriteLocked = true;
            }

            void UnlockValueForWrite() {
                {
                    auto g = LockGuard();
                    ValueWriteLocked = false;
                }
                ValueWriteUnlockedCondVar.NotifyAll(); // FIXME
            }

            /////////////////////////////////////////////////////////////////

            void SetValue(const TValue& value, ui32 deadline) {
                LockValueForWrite();
                Y_DEFER([this] {
                    UnlockValueForWrite();
                });

TODO_BETTER_CONCURRENCY
                auto g = LockGuard();
                TInodeDataOps ops(Volume);
                ops.SetValue(*Inode, value, deadline);
            }

            void UnsetValue() {
                LockValueForWrite();
                Y_DEFER([this] {
                    UnlockValueForWrite();
                });

TODO_BETTER_CONCURRENCY
                auto g = LockGuard();
                TInodeDataOps ops(Volume);
                ops.UnsetValue(*Inode);
            }

            TValue GetValue() {
                LockValueForRead();
                Y_DEFER([this] {
                    UnlockValueForRead();
                });

TODO_BETTER_CONCURRENCY
                auto g = LockGuard();
                TInodeDataOps ops(Volume);
                return ops.GetValue(*Inode);
            }
        };

        struct TDentryCacheKey {
            TFullInodeId ParentInode;
            std::string ChildName;

            bool operator== (const TDentryCacheKey& other) const {
                return ParentInode == other.ParentInode
                    && ChildName == other.ChildName;
            }
        };

        struct TFullInodeIdHash {
            size_t operator() (const TFullInodeId& key) const {
                auto h0 = std::hash<TVolume*>{}(key.Volume);
                auto h1 = std::hash<TInode::TId>{}(key.InodeId);
                return CombineHashes(h0, h1);
            }
        };

        struct TDentryKeyHash {
            size_t operator() (const TDentryCacheKey& key) const {
                auto h0 = TFullInodeIdHash{}(key.ParentInode);
                auto h1 = std::hash<std::string>{}(key.ChildName);
                return CombineHashes(h0, h1);
            }
        };

        using TDentryCache = THashMap<TDentryCacheKey, TDentry, TDentryKeyHash>;
        using TDentryFromCache = TDentryCache::TValuePtr;

        // TDentry that release some in dtor
        class TDentryWithGuards {
        public:
            explicit TDentryWithGuards(TDentry* ptr)
                : Ptr_(ptr)
            {
            }

            explicit TDentryWithGuards(TDentryFromCache holder)
                : Ptr_(holder.Ptr())
                , Holder_(std::move(holder))
            {
            }

            TDentryWithGuards() = default;

            TDentryWithGuards(const TDentryWithGuards&) = delete;

            TDentryWithGuards(TDentryWithGuards&& other) noexcept {
                Swap(other);
            }

            ~TDentryWithGuards() {
                if (Ptr_) {
                    if (PreventRemoval_) {
                        auto g = Ptr_->LockGuard(); // Y_TODO("May be locked already by same thread")
                        --Ptr_->PreventRemoval;
                    }
                }
            }

            TDentryWithGuards& operator=(const TDentryWithGuards&) = delete;

            TDentryWithGuards& operator=(TDentryWithGuards&& other) noexcept {
                TDentryWithGuards tmp(std::move(other));
                Swap(tmp);
                return *this;
            }

            explicit operator bool () const {
                return (bool)Ptr_;
            }

            // FIXME
            void PreventRemoval() {
                PreventRemoval_ = true;
            }

            TDentry* operator-> () const {
                return Ptr_;
            }

            TDentry& operator* () const {
                return *Ptr_;
            }

            void Swap(TDentryWithGuards& other) {
                std::swap(Ptr_, other.Ptr_);
                Holder_.Swap(other.Holder_);
                std::swap(PreventRemoval_, other.PreventRemoval_);
            }

        private:
            TDentry* Ptr_ = nullptr;
            TDentryFromCache Holder_;
            bool PreventRemoval_ = false;
        };

        struct TDentryWithVolume {
            TVolume* Volume{};
            TDentryWithGuards Dentry{};

            operator TFullInodeId() const {
                return {Volume, Dentry->Inode->Id};
            }

            static TDentryWithVolume FromMount(const TMount& mount) {
                return TDentryWithVolume{mount.Volume, TDentryWithGuards(mount.Dentry)};
            }
        };

        TDentryWithGuards Wrap(TDentryCache::TValuePtr dentry) {
            return TDentryWithGuards{std::move(dentry)};
        }

        TDentry* EnsureMountedInode(TVolume* srcVolume, const std::string& srcDir);
        TDentryWithVolume ResolvePath(const std::string& path, bool create);
        TDentryWithVolume ResolveDirs(const std::string_view& path, const TResolveParams&);
        TVolume::TInode ResolveInVolumePath(TVolume* volume, const std::string& path);
        TDentryWithGuards StepPath(const TDentryWithVolume& parent, const std::string& childName, const TResolveParams&);
        void EnsureInodeData(TDentryWithVolume node);

    private:
        TMount Root_;
        TDentryCache DentryCache_;
        std::unordered_map<TFullInodeId, TDentry, TFullInodeIdHash> Mounted_;
    };

    [[nodiscard]] TStorage::TImpl::TChildNameLockGuard TStorage::TImpl::TDentry::LockChild(const std::string& name) {
        auto copy = name;
        {
            auto g = LockGuard();
            while (true) {
                if (std::none_of(ChildrenLocks.begin(), ChildrenLocks.end(), [&](const std::string& other) { return other == name; })) {
                    break;
                }
                ChildrenLocksCondVar.Wait(Lock);
            }
            ChildrenLocks.emplace_back(std::move(copy));
        }
        return TChildNameLockGuard(this, name);
    }

    void TStorage::TImpl::TDentry::UnlockChild(const std::string& name) {
        {
            auto g = LockGuard();
            const size_t size0 = ChildrenLocks.size();
            Y_VERIFY(size0 > 0);
            std::erase(ChildrenLocks, name);
            Y_VERIFY(ChildrenLocks.size() == size0 - 1);
        }
        ChildrenLocksCondVar.NotifyAll(); // FIXME One
}

    TStorage::TImpl::TChildNameLockGuard::TChildNameLockGuard(TDentry* dentry, std::string name)
        : Dentry_(dentry)
        , Name_(std::move(name))
    {
    }

    TStorage::TImpl::TChildNameLockGuard::TChildNameLockGuard(TChildNameLockGuard&& other) noexcept {
        Swap(other);
    }

    TStorage::TImpl::TChildNameLockGuard::~TChildNameLockGuard() {
        if (Dentry_) {
            Dentry_->UnlockChild(Name_);
        }
    }

    void TStorage::TImpl::TChildNameLockGuard::Swap(TChildNameLockGuard& other) noexcept {
        std::swap(Dentry_, other.Dentry_);
        std::swap(Name_, other.Name_);
    }

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

            keyName = {b.base(), keyEnd};
            dirPath = std::string_view(&*path.begin(), std::distance(path.begin(), b.base()));
        }

        auto dir = ResolveDirs(dirPath, {.Create = create});
        if (!dir.Dentry) {
            Y_ENSURE(!create)
            return {};
        }

        if (dir.Dentry->Mounts) {
            const auto& mounts = *dir.Dentry->Mounts;
            for (auto it = mounts.rbegin(); it != mounts.rend(); ++it) {
                const auto mount = TDentryWithVolume::FromMount(*it);
                auto dentry = StepPath(mount, keyName, {.Create = false});
                if (dentry) {
                    return {mount.Volume, std::move(dentry)};
                }
            }

            if (!create) {
                return {};
            }

            const auto target = TDentryWithVolume::FromMount(dir.Dentry->Mounts->back());
            auto dentry = StepPath(target, keyName, {.Create = true});
            if (dentry) {
                return {target.Volume, std::move(dentry)};
            }

            return {};
        }

        auto dentry = StepPath(dir, keyName, {.Create = create});
        if (!dentry) {
            return {};
        }
        return {dir.Volume, std::move(dentry)};
    }

    //void TStorage::TImpl::EnsureInodeData(TDentryWithVolume node) {
    //    if (node.Dentry->InodeData) {
    //        return;
    //    }
    //    TInodeData* inodeData = &InodeCache_[node];
    //    if (!inodeData->Loaded) {
    //        inodeData->Inode = node.Volume->ReadInode(node.Dentry->InodeId);
    //        inodeData->Loaded = true;
    //    }
    //    node.Dentry->InodeData = inodeData;
    //}

    /*
        EVENTS

        1. x.ModifyValue
        2. p.ModifyChilds = p.RemoveChild(x) | p.AddChild(x)
        3. RemoveFromCache(x)
        4. AddToCache
        5. FlushCacheItem
        6. p.LookupChildInode(x)
        7. p.EnsureChildInode(x)
        
        I. x.ModifyValue
            Race:
                p.RemoveChild(x)
                x.ModifyValue
                RemoveFromCache(x)
                FlushCacheItem(x)

        II. x.RemoveChild(c) | x.AddChild(c)
            Race:
                p.RemoveChild(x)
                x.RemoveChild(...)
                x.AddChild(...)
                RemoveFromCache(x)
                RemoveFromCache(c)
                FlushCacheItem(c)

        III. AddToCache(c)


    */

    TStorage::TImpl::TDentryWithGuards TStorage::TImpl::StepPath(const TDentryWithVolume& parentExt, const std::string& childName, const TResolveParams& params) {
        using TInodePtr = std::unique_ptr<TInode>;

        auto* volume = parentExt.Volume;
        auto& parent = parentExt.Dentry;

        // 1. parent ++ref_count to prevent cache removal (inside hashmap impl)
        // 2. parent can be in NotExists state, so lock this 

        const TDentryCacheKey childCacheKey{{volume, parent->Inode->Id}, childName};
        auto emplaceResult = DentryCache_.emplace_key(childCacheKey);
        auto child = Wrap(std::move(emplaceResult.Obj));

        if (emplaceResult.Created) {
            auto childGuard = parent->LockChild(childName);
            {
                Y_DEFER([&](){
                    child->InitCondVar.NotifyAll();
                });

                TInodePtr childInode;
                if (params.Create) {
                    childInode = parent->EnsureChild(childName, childGuard);
                } else {
                    childInode = parent->LookupChild(childName, childGuard);
                }

                if (!childInode) {
                    {
                        auto g = child->LockGuard();
                        child->State = TDentry::EState::NotExists;
                        child->InParentName = std::move(childName);
                        child->Volume = volume;
                    }
                    return {};
                }

                {
                    auto g = child->LockGuard();
                    child->Inode = std::move(childInode);
                    child->State = TDentry::EState::Exists;
                    child->InParentName = std::move(childName);
                    child->Volume = volume;
                    ++child->PreventRemoval;
                    child.PreventRemoval();
                }
            }
            return child;
        } else {
            child->WaitInitialized(); // TODO Don't take lock twice

            auto childGuard = parent->LockChild(childName);

            {
                auto g = child->LockGuard();
                while (true) {
                    if (child->State == TDentry::EState::Exists) {
                        ++child->PreventRemoval;
                        child.PreventRemoval();
                        return child;
                    } else if (!params.Create) {
                        return {};
                    }

                    while (child->CreateLocked) {
                        child->CreateCondVar.Wait(child->Lock);
                    }
                    if (child->State == TDentry::EState::NotExists) {
                        child->CreateLocked = true;
                        break;
                    }
                }
            }

            TInodePtr childInode = parent->EnsureChild(childName, childGuard);
            {
                auto g = child->LockGuard();
                child->Inode = std::move(childInode);
                child->State = TDentry::EState::Exists;
                ++child->PreventRemoval;
                child.PreventRemoval();
                child->CreateLocked = false;
            }
            child->CreateCondVar.NotifyAll(); // FIXME

            return child;
        }

        Y_UNREACHABLE;
        return {};
    }

    TStorage::TImpl::TDentryWithVolume TStorage::TImpl::ResolveDirs(const std::string_view& path, const TResolveParams& params) {
        Y_ENSURE(!path.empty() && path[0] == '/');

        auto start = path.begin();
        while (start != path.end() && *start == '/') {
            ++start;
        }

        auto cur = TDentryWithVolume::FromMount(Root_);

        while (start != path.end()) {
            // Follow last mount
            if (cur.Dentry->Mounts) {
                cur = TDentryWithVolume::FromMount(cur.Dentry->Mounts->back());
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

        TInodeDataOps ops(volume);

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

    TStorage::TImpl::TDentry* TStorage::TImpl::EnsureMountedInode(TVolume* srcVolume, const std::string& srcDir) {
        auto srcInode = ResolveInVolumePath(srcVolume, srcDir);
        auto& dentry = Mounted_[{srcVolume, srcInode.Id}];

        auto g = dentry.LockGuard();
        if (dentry.State == TDentry::EState::Uninitialized) {
            std::stringstream s;
            s << (void*)srcVolume << srcVolume->GetFsDir() << '@' << srcDir;
            dentry.InParentName = s.str();
            dentry.Volume = srcVolume;
            // TODO Better
            dentry.Inode.reset(new TInode());
            *dentry.Inode = std::move(srcInode);
            dentry.State = TDentry::EState::Exists;
        }

        return &dentry;
    }

    void TStorage::TImpl::Mount(const std::string& mountPointPath, TVolume* srcVolume, const std::string& srcDir) {
        auto mountPoint = ResolveDirs(mountPointPath, {.Create = true});
        if (!mountPoint.Dentry->Mounts) {
            mountPoint.Dentry->Mounts.reset(new std::vector<TMount>());
        }
        auto& mount = mountPoint.Dentry->Mounts->emplace_back();
        mount.Volume = srcVolume;
        mount.Dentry = EnsureMountedInode(srcVolume, srcDir);
    }

    TStorage::TStorage(TVolume* rootVolume, const std::string& rootDir)
        : Impl_(new TImpl(rootVolume, rootDir))
    {
    }

    TStorage::TStorage(TStorage&& other) noexcept
        : Impl_(std::move(other.Impl_))
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

    void TStorage::Erase(const std::string& path) {
        Impl_->Erase(path);
    }

}