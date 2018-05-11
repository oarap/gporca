//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin.h
//
//	@doc:
//		Turn LS apply with external correlations apply into inner join;
//		external correlations are correlations in the inner child of LSA
//		that use columns not defined by the outer child of LSA
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiApplyWithExternalCorrs2InnerJoin_H
#define GPOPT_CXformLeftSemiApplyWithExternalCorrs2InnerJoin_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformApply2Join.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/operators/ops.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformLeftSemiApplyWithExternalCorrs2InnerJoin
	//
	//	@doc:
	//		Transform Apply into Join by decorrelating the inner side
	//
	//---------------------------------------------------------------------------
	class CXformLeftSemiApplyWithExternalCorrs2InnerJoin : public CXformApply2Join<CLogicalLeftSemiApply, CLogicalInnerJoin>
	{

		private:

			// private copy ctor
			CXformLeftSemiApplyWithExternalCorrs2InnerJoin(const CXformLeftSemiApplyWithExternalCorrs2InnerJoin &);

			// helper for splitting correlations into external and residual
			static
			BOOL FSplitCorrelations
				(
				IMemoryPool *memory_pool,
				CExpression *pexprOuter,
				CExpression *pexprInner,
				DrgPexpr *pdrgpexprAllCorr,
				DrgPexpr **ppdrgpexprExternal,
				DrgPexpr **ppdrgpexprResidual,
				CColRefSet **ppcrsInnerUsed
				);

			// helper for collecting correlations
			static
			BOOL FDecorrelate
				(
				IMemoryPool *memory_pool,
				CExpression *pexpr,
				CExpression **ppexprInnerNew,
				DrgPexpr **ppdrgpexprCorr
				);

			// decorrelate semi apply with external correlations
			static
			CExpression *PexprDecorrelate
				(
				IMemoryPool *memory_pool,
				CExpression *pexpr
				);

		public:

			// ctor
			explicit
			CXformLeftSemiApplyWithExternalCorrs2InnerJoin
				(
				IMemoryPool *memory_pool
				)
				:
				CXformApply2Join<CLogicalLeftSemiApply, CLogicalInnerJoin>(memory_pool, true /*fDeepTree*/)
			{}

			// ctor with a passed pattern
			CXformLeftSemiApplyWithExternalCorrs2InnerJoin
				(
				IMemoryPool *memory_pool,
				CExpression *pexprPattern
				)
				:
				CXformApply2Join<CLogicalLeftSemiApply, CLogicalInnerJoin>(memory_pool, pexprPattern)
			{}

			// dtor
			virtual
			~CXformLeftSemiApplyWithExternalCorrs2InnerJoin()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLeftSemiApplyWithExternalCorrs2InnerJoin;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformLeftSemiApplyWithExternalCorrs2InnerJoin";
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// actual transform
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformLeftSemiApplyWithExternalCorrs2InnerJoin

}

#endif // !GPOPT_CXformLeftSemiApplyWithExternalCorrs2InnerJoin_H

// EOF

