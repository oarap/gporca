//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CIndexDescriptor.h
//
//	@doc:
//		Base class for index descriptor
//---------------------------------------------------------------------------
#ifndef GPOPT_CIndexDescriptor_H
#define GPOPT_CIndexDescriptor_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDIndex.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CColumnDescriptor.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CIndexDescriptor
	//
	//	@doc:
	//		Base class for index descriptor
	//
	//---------------------------------------------------------------------------
	class CIndexDescriptor : public CRefCount
	{
		private:

			// mdid of the index
			IMDId *m_pmdidIndex;

			// name of index
			CName m_name;

			// array of index key columns
			DrgPcoldesc *m_pdrgpcoldescKeyCols;

			// array of index included columns
			DrgPcoldesc *m_pdrgpcoldescIncludedCols;

			// clustered index
			BOOL m_clustered;

			// private copy ctor
			CIndexDescriptor(const CIndexDescriptor &);

		public:

			// ctor
			CIndexDescriptor
				(
				IMemoryPool *memory_pool,
				IMDId *pmdidIndex,
				const CName &name,
				DrgPcoldesc *pdrgcoldescKeyCols,
				DrgPcoldesc *pdrgcoldescIncludedCols,
				BOOL is_clustered
				);

			// dtor
			virtual
			~CIndexDescriptor();

			// number of key columns
			ULONG Keys() const;

			// number of included columns
			ULONG UlIncludedColumns() const;

			// index mdid accessor
			IMDId *MDId() const
			{
				return m_pmdidIndex;
			}

			// index name
			const CName &Name() const
			{
				return m_name;
			}

			// key column descriptors
			DrgPcoldesc *PdrgpcoldescKey() const
			{
				return m_pdrgpcoldescKeyCols;
			}

			// included column descriptors
			DrgPcoldesc *PdrgpcoldescIncluded() const
			{
				return m_pdrgpcoldescIncludedCols;
			}
			
			// is index clustered
			BOOL IsClustered() const
			{
				return m_clustered;
			}

			// create an index descriptor
			static CIndexDescriptor *Pindexdesc
				(
				IMemoryPool *memory_pool,
				const CTableDescriptor *ptabdesc,
				const IMDIndex *pmdindex
				);

#ifdef GPOS_DEBUG
			IOstream &OsPrint(IOstream &) const;
#endif // GPOS_DEBUG

	}; // class CIndexDescriptor
}

#endif // !GPOPT_CIndexDescriptor_H

// EOF
