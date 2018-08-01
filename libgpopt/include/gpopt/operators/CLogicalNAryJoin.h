//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalNAryJoin.h
//
//	@doc:
//		N-ary inner join operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalNAryJoin_H
#define GPOS_CLogicalNAryJoin_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalJoin.h"

namespace gpopt
{	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalNAryJoin
	//
	//	@doc:
	//		N-ary inner join operator
	//
	//---------------------------------------------------------------------------
	class CLogicalNAryJoin : public CLogicalJoin
	{
		private:

			// private copy ctor
			CLogicalNAryJoin(const CLogicalNAryJoin &);

		public:

			// ctor
			explicit
			CLogicalNAryJoin(IMemoryPool *memory_pool);

			// dtor
			virtual
			~CLogicalNAryJoin() 
			{}

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalNAryJoin;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalNAryJoin";
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive not nullable columns
			virtual
			CColRefSet *PcrsDeriveNotNull
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PcrsDeriveNotNullCombineLogical(memory_pool, exprhdl);
			}

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl
				) 
				const
			{
				return PpartinfoDeriveCombine(memory_pool, exprhdl);
			}

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintFromPredicates(memory_pool, exprhdl);
			}

			//-------------------------------------------------------------------------------------
			// Derived Stats
			//-------------------------------------------------------------------------------------

			// promise level for stat derivation
			virtual
			EStatPromise Esp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				// we should use the expanded join order for stat derivation
				return EspLow;
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *memory_pool) const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalNAryJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalNAryJoin == pop->Eopid());
				
				return dynamic_cast<CLogicalNAryJoin*>(pop);
			}

	}; // class CLogicalNAryJoin

}


#endif // !GPOS_CLogicalNAryJoin_H

// EOF
