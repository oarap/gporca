//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCacheFactory.cpp
//
//	@doc:
//		 Function implementation of CCacheFactory
//---------------------------------------------------------------------------


#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCache.h"
#include "gpos/memory/CCacheFactory.h"

using namespace gpos;

// global instance of cache factory
CCacheFactory *CCacheFactory::m_factory = NULL;

//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::CCacheFactory
//
//	@doc:
//		Ctor;
//
//---------------------------------------------------------------------------
CCacheFactory::CCacheFactory
	(
		IMemoryPool *memory_pool
	)
	:
	m_memory_pool(memory_pool)
{

}


//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::Pmp
//
//	@doc:
//		Returns a pointer to allocated memory pool
//
//---------------------------------------------------------------------------
IMemoryPool *
CCacheFactory::Pmp() const
{
	return m_memory_pool;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::Init
//
//	@doc:
//		Initializes global instance
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheFactory::Init()
{
	GPOS_ASSERT(NULL == GetFactory() &&
			    "Cache factory was already initialized");

	GPOS_RESULT res = GPOS_OK;

	// create cache factory memory pool
	IMemoryPool *memory_pool = CMemoryPoolManager::GetMemoryPoolMgr()->Create
							(
							CMemoryPoolManager::EatTracker,
							true /*fThreadSafe*/,
							gpos::ullong_max
							);
	GPOS_TRY
	{
		// create cache factory instance
		CCacheFactory::m_factory = GPOS_NEW(memory_pool) CCacheFactory(memory_pool);
	}
	GPOS_CATCH_EX(ex)
	{
		// destroy memory pool if global instance was not created
		CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(memory_pool);

		CCacheFactory::m_factory = NULL;

		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			res = GPOS_OOM;
		}
		else
		{
			res = GPOS_FAILED;
		}
	}
	GPOS_CATCH_END;
	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::Shutdown
//
//	@doc:
//		Cleans up allocated memory pool
//
//---------------------------------------------------------------------------
void
CCacheFactory::Shutdown()
{
	CCacheFactory *factory = CCacheFactory::GetFactory();

	GPOS_ASSERT(NULL != factory &&
			    "Cache factory has not been initialized");

	IMemoryPool *memory_pool = factory->m_memory_pool;

	// destroy cache factory
	CCacheFactory::m_factory = NULL;
	GPOS_DELETE(factory);

	// release allocated memory pool
	CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(memory_pool);
}
// EOF
