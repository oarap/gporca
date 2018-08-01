//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftOuterJoin2HashJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformLeftOuterJoin2HashJoin.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2HashJoin::CXformLeftOuterJoin2HashJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftOuterJoin2HashJoin::CXformLeftOuterJoin2HashJoin
	(
	IMemoryPool *memory_pool
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(memory_pool) CExpression
					(
					memory_pool,
					GPOS_NEW(memory_pool) CLogicalLeftOuterJoin(memory_pool),
					GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // left child
					GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // right child
					GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternTree(memory_pool))  // predicate
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2HashJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftOuterJoin2HashJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2HashJoin::Transform
//
//	@doc:
//		actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftOuterJoin2HashJoin::Transform
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

	CXformUtils::ImplementHashJoin<CPhysicalLeftOuterHashJoin>(pxfctxt, pxfres, pexpr);
}


// EOF
