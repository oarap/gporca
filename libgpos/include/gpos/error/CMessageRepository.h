//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010, Greenplum, Inc.
//
//	@filename:
//		CMessageRepository.h
//
//	@doc:
//		Error message repository; 
//---------------------------------------------------------------------------
#ifndef GPOS_CMessageRepository_H
#define GPOS_CMessageRepository_H

#include "gpos/sync/CSpinlock.h"
#include "gpos/error/CMessageTable.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMessageRepository
	//
	//	@doc:
	//		Stores and loads all error messages by locale
	//
	//---------------------------------------------------------------------------
	class CMessageRepository
	{
		private:
		
			// global singleton
			static
			CMessageRepository *m_repository;
			
			// memory pool
			IMemoryPool *m_memory_pool;

			// short hand for Table of Message Tables (TMT)
			typedef CSyncHashtable<
						CMessageTable, 
						ELocale, 
						CSpinlockOS> TMT;

			// short hand for TMT accessor
			typedef CSyncHashtableAccessByKey<
						CMessageTable, 
						ELocale, 
						CSpinlockOS> TMTAccessor;
		
			// basic hash table
			TMT m_hash_table;
			
			// init basic directory
			void InitDirectory(IMemoryPool *memory_pool);
			
			// install message table for a given locale
			void AddMessageTable(ELocale locale);
		
			// pre-load standard messages
			void LoadStandardMessages();

		public:

			// ctor
			CMessageRepository(IMemoryPool *memory_pool);

			// dtor
			~CMessageRepository();

			// lookup message by error/local
			CMessage *LookupMessage(CException exc, ELocale locale);

			// add individual message
			void AddMessage(ELocale locale, CMessage *msg);

			// initializer for global singleton
			static
			GPOS_RESULT Init();

			// accessor for global singleton
			static 
			CMessageRepository* GetMessageRepository();

			void Shutdown();

	}; // class CMessageRepository
}

#endif // !GPOS_CMessageRepository_H

// EOF

