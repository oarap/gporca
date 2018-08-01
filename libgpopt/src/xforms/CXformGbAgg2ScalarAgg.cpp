//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformGbAgg2ScalarAgg.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CXformGbAgg2ScalarAgg.h"
#include "gpopt/xforms/CXformGbAgg2HashAgg.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ScalarAgg::CXformGbAgg2ScalarAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2ScalarAgg::CXformGbAgg2ScalarAgg
	(
	IMemoryPool *memory_pool
	)
	:
	CXformImplementation
		(
		 // pattern
		GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CLogicalGbAgg(memory_pool),
							 GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)),
							 GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)))
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ScalarAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		grouping columns must be empty
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2ScalarAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (0 < CLogicalGbAgg::PopConvert(exprhdl.Pop())->Pdrgpcr()->Size() ||
		exprhdl.GetDrvdScalarProps(1 /*child_index*/)->FHasSubquery())
	{
		// GbAgg has grouping columns, or agg functions use subquery arguments
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2ScalarAgg::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformGbAgg2ScalarAgg::Transform
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

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	IMemoryPool *memory_pool = pxfctxt->Pmp();
	ColRefArray *colref_array = popAgg->Pdrgpcr();
	colref_array->AddRef();
	
	// extract components
	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref children
	pexprRel->AddRef();
	pexprScalar->AddRef();

	ColRefArray *pdrgpcrArgDQA = popAgg->PdrgpcrArgDQA();
	if (pdrgpcrArgDQA != NULL && 0 != pdrgpcrArgDQA->Size())
	{
		pdrgpcrArgDQA->AddRef();
	}

	// create alternative expression
	CExpression *pexprAlt = 
		GPOS_NEW(memory_pool) CExpression
			(
			memory_pool,
			GPOS_NEW(memory_pool) CPhysicalScalarAgg
						(
						memory_pool,
						colref_array,
						popAgg->PdrgpcrMinimal(),
						popAgg->Egbaggtype(),
						popAgg->FGeneratesDuplicates(),
						pdrgpcrArgDQA,
						CXformUtils::FMultiStageAgg(pexpr)
						),
			pexprRel,
			pexprScalar
			);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

