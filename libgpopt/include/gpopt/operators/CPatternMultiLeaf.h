//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPatternMultiLeaf.h
//
//	@doc:
//		Pattern that matches a variable number of leaves
//---------------------------------------------------------------------------
#ifndef GPOPT_CPatternMultiLeaf_H
#define GPOPT_CPatternMultiLeaf_H

#include "gpos/base.h"
#include "gpopt/operators/CPattern.h"

namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPatternMultiLeaf
	//
	//	@doc:
	//		Pattern that matches a variable number of expressions, eg inputs to
	//		union operator
	//
	//---------------------------------------------------------------------------
	class CPatternMultiLeaf : public CPattern
	{

		private:

			// private copy ctor
			CPatternMultiLeaf(const CPatternMultiLeaf &);

		public:
		
			// ctor
			explicit
			CPatternMultiLeaf
				(
				IMemoryPool *memory_pool
				)
				: 
				CPattern(memory_pool)
			{}

			// dtor
			virtual 
			~CPatternMultiLeaf() {}
			
			// check if operator is a pattern leaf
			virtual
			BOOL FLeaf() const
			{
				return true;
			}

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopPatternMultiLeaf;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CPatternMultiLeaf";
			}		

	}; // class CPatternMultiLeaf

}


#endif // !GPOPT_CPatternMultiLeaf_H

// EOF
