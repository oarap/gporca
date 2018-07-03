//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSubqJoin2Apply.cpp
//
//	@doc:
//		Implementation of Inner Join to Apply transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CSubqueryHandler.h"
#include "gpopt/xforms/CXformSubqJoin2Apply.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::CXformSubqJoin2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSubqJoin2Apply::CXformSubqJoin2Apply
	(
	IMemoryPool *mp
	)
	:
	// pattern
	CXformSubqueryUnnest
		(
		GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CLogicalInnerJoin(mp),
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // relational child
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // relational child
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
// 		if subqueries exist in the scalar predicate, we must have an
// 		equivalent logical Apply expression created during exploration;
// 		no need for generating a Join expression here
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSubqJoin2Apply::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.GetDrvdScalarProps(exprhdl.Arity() - 1)->FHasSubquery())
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::CollectSubqueries
//
//	@doc:
//		Collect subqueries that exclusively use columns from one join child
//
//---------------------------------------------------------------------------
void
CXformSubqJoin2Apply::CollectSubqueries
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	ColRefSetArray *pdrgpcrs,
	ExpressionArrays *pdrgpdrgpexprSubqs // array-of-arrays indexed on join child index.
									//  i^{th} entry is an array corresponding to subqueries collected for join child #i
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdrgpcrs);
	GPOS_ASSERT(NULL != pdrgpdrgpexprSubqs);

	COperator *pop = pexpr->Pop();
	if (CUtils::FSubquery(pop))
	{
		// extract outer references below subquery
		CColRefSet *outer_refs = GPOS_NEW(mp) CColRefSet(mp, *CDrvdPropRelational::GetRelationalProperties((*pexpr)[0]->PdpDerive())->PcrsOuter());

		// add columns used by subquery
		outer_refs->Union(CDrvdPropScalar::GetDrvdScalarProps(pexpr->PdpDerive())->PcrsUsed());

		ULONG child_index = gpos::ulong_max;
		const ULONG size = pdrgpcrs->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			CColRefSet *pcrsOutput = (*pdrgpcrs)[ul];
			if (pcrsOutput->ContainsAll(outer_refs))
			{
				// outer columns all come from the same join child, break here
				child_index = ul;
				break;
			}
		}

		if (gpos::ulong_max != child_index)
		{
			pexpr->AddRef();
			(*pdrgpdrgpexprSubqs)[child_index]->Append(pexpr);
		}

		outer_refs->Release();
		return;
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		CollectSubqueries(mp, pexprChild, pdrgpcrs, pdrgpdrgpexprSubqs);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::PexprReplaceSubqueries
//
//	@doc:
//		Replace subqueries with scalar identifiers based on given map
//
//---------------------------------------------------------------------------
CExpression *
CXformSubqJoin2Apply::PexprReplaceSubqueries
	(
	IMemoryPool *mp,
	CExpression *pexprScalar,
	ExprToColRefMap *phmexprcr
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != phmexprcr);

	CColRef *colref = phmexprcr->Find(pexprScalar);
	if (NULL != colref)
	{
		// look-up succeeded on root operator, we return here
		return CUtils::PexprScalarIdent(mp, colref);
	}

	// recursively process children
	const ULONG arity = pexprScalar->Arity();
	ExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) ExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprReplaceSubqueries(mp, (*pexprScalar)[ul], phmexprcr);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexprScalar->Pop();
	pop->AddRef();

	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::PexprSubqueryPushdown
//
//	@doc:
//		Push down subquery below join
//
//---------------------------------------------------------------------------
CExpression *
CXformSubqJoin2Apply::PexprSubqueryPushDown
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	BOOL fEnforceCorrelatedApply
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalSelect == pexpr->Pop()->Eopid());

	CExpression *pexprJoin = (*pexpr)[0];
	const ULONG arity = pexprJoin->Arity();
	CExpression *pexprScalar = (*pexpr)[1];
	CExpression *join_pred_expr = (*pexprJoin)[arity - 1];

	// collect output columns of all logical children
	ColRefSetArray *pdrgpcrs = GPOS_NEW(mp) ColRefSetArray(mp);
	ExpressionArrays *pdrgpdrgpexprSubqs = GPOS_NEW(mp) ExpressionArrays(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CExpression *pexprChild = (*pexprJoin)[ul];
		CColRefSet *pcrsOutput = CDrvdPropRelational::GetRelationalProperties(pexprChild->PdpDerive())->PcrsOutput();
		pcrsOutput->AddRef();
		pdrgpcrs->Append(pcrsOutput);

		pdrgpdrgpexprSubqs->Append(GPOS_NEW(mp) ExpressionArray(mp));
	}

	// collect subqueries that exclusively use columns from each join child
	CollectSubqueries(mp, pexprScalar, pdrgpcrs, pdrgpdrgpexprSubqs);

	// create new join children by pushing subqueries to Project nodes on top
	// of corresponding join children
	ExpressionArray *pdrgpexprNewChildren = GPOS_NEW(mp) ExpressionArray(mp);
	ExprToColRefMap *phmexprcr = GPOS_NEW(mp) ExprToColRefMap(mp);
	for (ULONG ulChild = 0; ulChild < arity - 1; ulChild++)
	{
		CExpression *pexprChild = (*pexprJoin)[ulChild];
		pexprChild->AddRef();
		CExpression *pexprNewChild = pexprChild;

		ExpressionArray *pdrgpexprSubqs = (*pdrgpdrgpexprSubqs)[ulChild];
		const ULONG ulSubqs = pdrgpexprSubqs->Size();
		if (0 < ulSubqs)
		{
			// join child has pushable subqueries
			pexprNewChild = CUtils::PexprAddProjection(mp, pexprChild, pdrgpexprSubqs);
			CExpression *pexprPrjList = (*pexprNewChild)[1];

			// add pushed subqueries to map
			for (ULONG ulSubq = 0; ulSubq < ulSubqs; ulSubq++)
			{
				CExpression *pexprSubq = (*pdrgpexprSubqs)[ulSubq];
				pexprSubq->AddRef();
				CColRef *colref = CScalarProjectElement::PopConvert((*pexprPrjList)[ulSubq]->Pop())->Pcr();
	#ifdef GPOS_DEBUG
				BOOL fInserted =
	#endif // GPOS_DEBUG
					phmexprcr->Insert(pexprSubq, colref);
				GPOS_ASSERT(fInserted);
			}

			// unnest subqueries in newly created child
			CExpression *pexprUnnested = PexprSubqueryUnnest(mp, pexprNewChild, fEnforceCorrelatedApply);
			if (NULL != pexprUnnested)
			{
				pexprNewChild->Release();
				pexprNewChild = pexprUnnested;
			}
		}

		pdrgpexprNewChildren->Append(pexprNewChild);
	}

	join_pred_expr->AddRef();
	pdrgpexprNewChildren->Append(join_pred_expr);

	// replace subqueries in the original scalar expression with
	// scalar identifiers based on constructed map
	CExpression *pexprNewScalar = PexprReplaceSubqueries(mp, pexprScalar, phmexprcr);

	phmexprcr->Release();
	pdrgpcrs->Release();
	pdrgpdrgpexprSubqs->Release();

	// build the new join expression
	COperator *pop = pexprJoin->Pop();
	pop->AddRef();
	CExpression *pexprNewJoin = GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprNewChildren);

	// return a new Select expression
	pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pexprNewJoin, pexprNewScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::Transform
//
//	@doc:
//		Helper of transformation function
//
//---------------------------------------------------------------------------
void
CXformSubqJoin2Apply::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr,
	BOOL fEnforceCorrelatedApply
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *mp = pxfctxt->Pmp();
	CExpression *pexprSelect = CXformUtils::PexprSeparateSubqueryPreds(mp, pexpr);

	// attempt pushing subqueries to join children,
	// this optimization may not always succeed since unnested subqueries below joins
	// could hide columns needed to evaluate join condition
	CExpression *pexprSubqsPushedDown = PexprSubqueryPushDown(mp, pexprSelect, fEnforceCorrelatedApply);

	// check if join columns in join condition are still accessible after subquery pushdown
	CExpression *pexprJoin = (*pexprSubqsPushedDown)[0];
	CExpression *pexprJoinCondition = (*pexprJoin)[pexprJoin->Arity() - 1];
	CColRefSet *pcrsUsed = CDrvdPropScalar::GetDrvdScalarProps(pexprJoinCondition->PdpDerive())->PcrsUsed();
	CColRefSet *pcrsJoinOutput = CDrvdPropRelational::GetRelationalProperties(pexprJoin->PdpDerive())->PcrsOutput();
	if (!pcrsJoinOutput->ContainsAll(pcrsUsed))
	{
		// discard expression after subquery push down
		pexprSubqsPushedDown->Release();
		pexprSelect->AddRef();
		pexprSubqsPushedDown = pexprSelect;
	}

	pexprSelect->Release();

	CExpression *pexprResult = NULL;
	BOOL fHasSubquery = CDrvdPropScalar::GetDrvdScalarProps((*pexprSubqsPushedDown)[1]->PdpDerive())->FHasSubquery();
	if (fHasSubquery)
	{
		// unnest subqueries remaining in the top Select expression
		pexprResult = PexprSubqueryUnnest(mp, pexprSubqsPushedDown, fEnforceCorrelatedApply);
		pexprSubqsPushedDown->Release();
	}
	else
	{
		pexprResult = pexprSubqsPushedDown;
	}

	if (NULL == pexprResult)
	{
		// unnesting failed, return here
		return;
	}

	// normalize resulting expression and add it to xform results container
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(mp, pexprResult);
	pexprResult->Release();
	pxfres->Add(pexprNormalized);
}


// EOF

