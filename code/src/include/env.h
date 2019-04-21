#ifndef _ENV_H
#define _ENV_H

namespace DB
{

#ifdef _xjbDB_MSVC_
	using int8_t = __int8;
	using uint8_t = unsigned __int8;
	using int16_t = __int16;
	using uint16_t = unsigned __int16;
	using int32_t = __int32;
	using uint32_t = unsigned __int32;
	using int64_t = __int64;
	using uint64_t = unsigned __int64;
	using float32 = float;
	using float64 = double;
	static_assert(sizeof(float) == sizeof(int32_t));
	static_assert(sizeof(double) == sizeof(int64_t));
#endif // _xjbDB_MSVC_

#ifdef _xjbDB_GCC_



#endif // _xjbDB_GCC_

#ifdef _xjbDB_Clang_



#endif // _xjbDB_Clang_




} // end namespace DB

#endif // !_ENV_H
