//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColRefSet.h
//
//	@doc:
//		Implementation of column reference sets based on bitset
//---------------------------------------------------------------------------
#ifndef GPOS_CColRefSet_H
#define GPOS_CColRefSet_H

#include "gpos/base.h"

#include "gpos/common/CBitSet.h"
#include "gpopt/base/CColRef.h"


#define GPOPT_COLREFSET_SIZE 1024

namespace gpopt
{

	// fwd decl
	class CColRefSet;
	
	// short hand for colref set array
	typedef CDynamicPtrArray<CColRefSet, CleanupRelease> ColRefSetArray;

	// hash map mapping CColRef -> CColRefSet
	typedef CHashMap<CColRef, CColRefSet, gpos::HashValue<CColRef>, gpos::Equals<CColRef>,
					CleanupNULL<CColRef>, CleanupRelease<CColRefSet> > ColRefToColRefSetMap;

	// hash map mapping INT -> CColRef
	typedef CHashMap<INT, CColRef, gpos::HashValue<INT>, gpos::Equals<INT>,
					CleanupDelete<INT>, CleanupNULL<CColRef> > HMICr;

	//---------------------------------------------------------------------------
	//	@class:
	//		CColRefSet
	//
	//	@doc:
	//		Column reference sets based on bitsets
	//
	//		Redefine accessors by bit index to be private to make super class' 
	//		member functions inaccessible
	//
	//---------------------------------------------------------------------------
	class CColRefSet : public CBitSet
	{
		// bitset iter needs to access internals
		friend class CColRefSetIter;
			
		private:
						
			// determine if bit is set
			BOOL Get(ULONG ulBit) const;
			
			// set given bit; return previous m_bytearray_value
			BOOL ExchangeSet(ULONG ulBit);
						
			// clear given bit; return previous m_bytearray_value
			BOOL ExchangeClear(ULONG ulBit);

		public:
				
			// ctor
			explicit
			CColRefSet(IMemoryPool *memory_pool, ULONG ulSizeBits = GPOPT_COLREFSET_SIZE);

			explicit
			CColRefSet(IMemoryPool *memory_pool, const CColRefSet &);
			
			// ctor, copy from col refs array
			CColRefSet(IMemoryPool *memory_pool, const ColRefArray *colref_array, ULONG ulSizeBits = GPOPT_COLREFSET_SIZE);

			// dtor
			~CColRefSet();
			
			// determine if bit is set
			BOOL FMember(const CColRef *colref) const;
			
			// return random member
			CColRef *PcrAny() const;
			
			// return first member
			CColRef *PcrFirst() const;

			// include column
			void Include(const CColRef *colref);

			// include column array
			void Include(const ColRefArray *colref_array);

			// include column set
			void Include(const CColRefSet *pcrs);
						
			// remove column
			void Exclude(const CColRef *colref);
			
			// remove column array
			void Exclude(const ColRefArray *colref_array);

			// remove column set
			void Exclude(const CColRefSet *pcrs);
			
			// replace column with another column
			void Replace(const CColRef *pcrOut, const CColRef *pcrIn);

			// replace column array with another column array
			void Replace(const ColRefArray *pdrgpcrOut, const ColRefArray *pdrgpcrIn);

			// check if the current colrefset is a subset of any of the colrefsets
			// in the given array
			BOOL FContained(const ColRefSetArray *pdrgpcrs);

			// convert to array
			ColRefArray *Pdrgpcr(IMemoryPool *memory_pool) const;

			// hash function
			ULONG HashValue();	

			// debug print
			IOstream &
			OsPrint(IOstream &os, ULONG ulLenMax = gpos::ulong_max) const;

			// extract all column ids
			void ExtractColIds(IMemoryPool *memory_pool, ULongPtrArray *col_ids) const;

			// are the columns in the column reference set covered by the array of column ref sets
			static
			BOOL FCovered(ColRefSetArray *pdrgpcrs, CColRefSet *pcrs);

	}; // class CColRefSet


	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CColRefSet &crf)
	{
		return crf.OsPrint(os);
	}

}

#endif // !GPOS_CColRefSet_H


// EOF
