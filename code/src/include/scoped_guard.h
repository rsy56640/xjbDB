#ifndef _SCOPED_GUARD_H
#define _SCOPED_GUARD_H
#include <type_traits>

#define COMBINE1(X,Y) X##Y  // helper macro
#define COMBINE(X,Y) COMBINE1(X,Y)

#define defer ::ScopedGuard COMBINE(defer_obj, __LINE__) = 

    namespace {
        template<typename F>
        class ScopedGuard {
        public:
            ScopedGuard(F&& func) :func_(std::forward<F>(func)) {}
            ~ScopedGuard() { func_(); }
            ScopedGuard(const ScopedGuard&) = delete;
            ScopedGuard(ScopedGuard&&) = delete;
            ScopedGuard& operator=(const ScopedGuard&) = delete;
            ScopedGuard& operator=(ScopedGuard&&) = delete;
        private:
            F && func_;
        };
    }

#endif // !_SCOPED_GUARD_H
