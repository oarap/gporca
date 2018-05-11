//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CPhysicalLeftAntiSemiHashJoinNotIn.h
//
//	@doc:
//		Left anti semi hash join operator with NotIn semantics
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftAntiSemiHashJoinNotIn_H
#define GPOPT_CPhysicalLeftAntiSemiHashJoinNotIn_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalLeftAntiSemiHashJoin.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalLeftAntiSemiHashJoinNotIn
	//
	//	@doc:
	//		Left anti semi hash join operator with NotIn semantics
	//
	//---------------------------------------------------------------------------
	class CPhysicalLeftAntiSemiHashJoinNotIn : public CPhysicalLeftAntiSemiHashJoin
	{

		private:

			// private copy ctor
			CPhysicalLeftAntiSemiHashJoinNotIn(const CPhysicalLeftAntiSemiHashJoinNotIn &);

		public:

			// ctor
			CPhysicalLeftAntiSemiHashJoinNotIn(IMemoryPool *memory_pool, DrgPexpr *pdrgpexprOuterKeys, DrgPexpr *pdrgpexprInnerKeys);

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalLeftAntiSemiHashJoinNotIn;
			}

			 // return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalLeftAntiSemiHashJoinNotIn";
			}

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required distribution of the n-th child
			virtual
			CDistributionSpec *PdsRequired
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsRequired,
				ULONG child_index,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CPhysicalLeftAntiSemiHashJoinNotIn *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(EopPhysicalLeftAntiSemiHashJoinNotIn == pop->Eopid());

				return dynamic_cast<CPhysicalLeftAntiSemiHashJoinNotIn*>(pop);
			}

	}; // class CPhysicalLeftAntiSemiHashJoinNotIn

}

#endif // !GPOPT_CPhysicalLeftAntiSemiHashJoinNotIn_H

// EOF
