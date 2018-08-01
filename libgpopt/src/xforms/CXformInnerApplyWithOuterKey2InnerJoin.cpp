//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInnerApplyWithOuterKey2InnerJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/xforms/CXformInnerApplyWithOuterKey2InnerJoin.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::CXformInnerApplyWithOuterKey2InnerJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerApplyWithOuterKey2InnerJoin::CXformInnerApplyWithOuterKey2InnerJoin
	(
	IMemoryPool *memory_pool
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(memory_pool) CExpression
				(
				memory_pool,
				GPOS_NEW(memory_pool) CLogicalInnerApply(memory_pool),
				GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // left child
				GPOS_NEW(memory_pool) CExpression
					(
					memory_pool,
					GPOS_NEW(memory_pool) CLogicalGbAgg(memory_pool),
					GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternTree(memory_pool)), // relational child of Gb
					GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)) // scalar project list of Gb
					), // right child
				GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool))  // Apply predicate
				)
		)
{}



//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerApplyWithOuterKey2InnerJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// check if outer child has key and inner child has outer references
	if (NULL == exprhdl.GetRelationalProperties(0)->Pkc() ||
		0 == exprhdl.GetRelationalProperties(1)->PcrsOuter()->Size())
	{
		return ExfpNone;
	}

	return ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerApplyWithOuterKey2InnerJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerApplyWithOuterKey2InnerJoin::Transform
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

	IMemoryPool *memory_pool = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprGb = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	if (0 < CLogicalGbAgg::PopConvert(pexprGb->Pop())->Pdrgpcr()->Size())
	{
		// xform is not applicable if inner Gb has grouping columns
		return;
	}

	if (CUtils::FHasSubqueryOrApply((*pexprGb)[0]))
	{
		// Subquery/Apply must be unnested before reaching here
		return;
	}

	// decorrelate Gb's relational child
	(*pexprGb)[0]->ResetDerivedProperties();
	CExpression *pexprInner = NULL;
	DrgPexpr *pdrgpexpr = GPOS_NEW(memory_pool) DrgPexpr(memory_pool);
	if (!CDecorrelator::FProcess(memory_pool, (*pexprGb)[0], false /*fEqualityOnly*/, &pexprInner, pdrgpexpr))
	{
		pdrgpexpr->Release();
		return;
	}

	GPOS_ASSERT(NULL != pexprInner);
	CExpression *pexprPredicate = CPredicateUtils::PexprConjunction(memory_pool, pdrgpexpr);

	// join outer child with Gb's decorrelated child
	pexprOuter->AddRef();
	CExpression *pexprInnerJoin =
		GPOS_NEW(memory_pool) CExpression
			(
			memory_pool,
			GPOS_NEW(memory_pool) CLogicalInnerJoin(memory_pool),
			pexprOuter,
			pexprInner,
			pexprPredicate
			);

	// create grouping columns from the output of outer child
	DrgPcr *pdrgpcrKey = NULL;
	DrgPcr *colref_array = CUtils::PdrgpcrGroupingKey(memory_pool, pexprOuter, &pdrgpcrKey);
	pdrgpcrKey->Release();  // key is not used here

	CLogicalGbAgg *popGbAgg = GPOS_NEW(memory_pool) CLogicalGbAgg(memory_pool, colref_array, COperator::EgbaggtypeGlobal /*egbaggtype*/);
	CExpression *pexprPrjList = (*pexprGb)[1];
	pexprPrjList->AddRef();
	CExpression *pexprNewGb = GPOS_NEW(memory_pool) CExpression (memory_pool, popGbAgg, pexprInnerJoin, pexprPrjList);

	// add Apply predicate in a top Select node
	pexprScalar->AddRef();
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(memory_pool, pexprNewGb, pexprScalar);

	pxfres->Add(pexprSelect);
}


// EOF

