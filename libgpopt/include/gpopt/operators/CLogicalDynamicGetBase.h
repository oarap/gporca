//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalDynamicGetBase.h
//
//	@doc:
//		Base class for dynamic table accessors for partitioned tables
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalDynamicGetBase_H
#define GPOPT_CLogicalDynamicGetBase_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	
	// fwd declarations
	class CTableDescriptor;
	class CName;
	class CColRefSet;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalDynamicGetBase
	//
	//	@doc:
	//		Dynamic table accessor base class
	//
	//---------------------------------------------------------------------------
	class CLogicalDynamicGetBase : public CLogical
	{

		protected:

			// alias for table
			const CName *m_pnameAlias;

			// table descriptor
			CTableDescriptor *m_ptabdesc;
			
			// dynamic scan id
			ULONG m_scan_id;
			
			// output columns
			ColRefArray *m_pdrgpcrOutput;
			
			// partition keys
			ColRefArrays *m_pdrgpdrgpcrPart;

			// secondary scan id in case of a partial scan
			ULONG m_ulSecondaryScanId;
			
			// is scan partial -- only used with heterogeneous indexes defined on a subset of partitions
			BOOL m_is_partial;
			
			// dynamic get part constraint
			CPartConstraint *m_part_constraint;
			
			// relation part constraint
			CPartConstraint *m_ppartcnstrRel;
			
			// distribution columns (empty for master only tables)
			CColRefSet *m_pcrsDist;

			// private copy ctor
			CLogicalDynamicGetBase(const CLogicalDynamicGetBase &);

			// given a colrefset from a table, get colids and attno
			void
			ExtractColIdsAttno(IMemoryPool *memory_pool, CColRefSet *pcrs, ULongPtrArray *col_ids, ULongPtrArray *pdrgpulPos) const;

			// derive stats from base table using filters on partition and/or index columns
			IStatistics *PstatsDeriveFilter(IMemoryPool *memory_pool, CExpressionHandle &exprhdl, CExpression *pexprFilter) const;

		public:
		
			// ctors
			explicit
			CLogicalDynamicGetBase(IMemoryPool *memory_pool);

			CLogicalDynamicGetBase
				(
				IMemoryPool *memory_pool,
				const CName *pnameAlias,
				CTableDescriptor *ptabdesc,
				ULONG scan_id,
				ColRefArray *colref_array,
				ColRefArrays *pdrgpdrgpcrPart,
				ULONG ulSecondaryScanId,
				BOOL is_partial,
				CPartConstraint *ppartcnstr, 
				CPartConstraint *ppartcnstrRel
				);
			
			CLogicalDynamicGetBase
				(
				IMemoryPool *memory_pool,
				const CName *pnameAlias,
				CTableDescriptor *ptabdesc,
				ULONG scan_id
				);

			// dtor
			virtual 
			~CLogicalDynamicGetBase();

			// accessors
			virtual
			ColRefArray *PdrgpcrOutput() const
			{
				return m_pdrgpcrOutput;
			}
			
			// return table's name
			virtual
			const CName &Name() const
			{
				return *m_pnameAlias;
			}
			
			// distribution columns
			virtual
			const CColRefSet *PcrsDist() const
			{
				return m_pcrsDist;
			}

			// return table's descriptor
			virtual
			CTableDescriptor *Ptabdesc() const
			{
				return m_ptabdesc;
			}
			
			// return scan id
			virtual
			ULONG ScanId() const
			{
				return m_scan_id;
			}
			
			// return the partition columns
			virtual
			ColRefArrays *PdrgpdrgpcrPart() const
			{
				return m_pdrgpdrgpcrPart;
			}

			// return secondary scan id
			virtual
			ULONG UlSecondaryScanId() const
			{
				return m_ulSecondaryScanId;
			}

			// is this a partial scan -- true if the scan operator corresponds to heterogeneous index
			virtual
			BOOL IsPartial() const
			{
				return m_is_partial;
			}
			
			// return dynamic get part constraint
			virtual
			CPartConstraint *Ppartcnstr() const
			{
				return m_part_constraint;
			}

			// return relation part constraint
			virtual
			CPartConstraint *PpartcnstrRel() const
			{
				return m_ppartcnstrRel;
			}
			
			// set part constraint
			virtual
			void SetPartConstraint(CPartConstraint *ppartcnstr);
			
			// set secondary scan id
			virtual
			void SetSecondaryScanId(ULONG scan_id);
			
			// set scan to partial
			virtual
			void SetPartial();

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *, CExpressionHandle &);

			// derive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;
			
			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;
		
			// derive join depth
			virtual
			ULONG JoinDepth
				(
				IMemoryPool *, // memory_pool
				CExpressionHandle & // exprhdl
				)
				const
			{
				return 1;
			}
			

	}; // class CLogicalDynamicGetBase

}


#endif // !GPOPT_CLogicalDynamicGetBase_H

// EOF
