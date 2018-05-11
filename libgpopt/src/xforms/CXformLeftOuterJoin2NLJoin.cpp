//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformLeftOuterJoin2NLJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformLeftOuterJoin2NLJoin.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2NLJoin::CXformLeftOuterJoin2NLJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftOuterJoin2NLJoin::CXformLeftOuterJoin2NLJoin
	(
	IMemoryPool *memory_pool
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(memory_pool) CExpression
					(memory_pool, 
					 GPOS_NEW(memory_pool) CLogicalLeftOuterJoin(memory_pool),
					 GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // left child
					 GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // right child
					 GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool))) // predicate
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2NLJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftOuterJoin2NLJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2NLJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftOuterJoin2NLJoin::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementNLJoin<CPhysicalLeftOuterNLJoin>(pxfctxt, pxfres, pexpr);
}
	

// EOF

