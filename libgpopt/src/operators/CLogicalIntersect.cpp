//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIntersect.cpp
//
//	@doc:
//		Implementation of Intersect operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/CLogicalIntersect.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalIntersectAll.h"
#include "gpopt/operators/CLogicalGbAgg.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::CLogicalIntersect
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalIntersect::CLogicalIntersect
	(
	IMemoryPool *mp
	)
	:
	CLogicalSetOp(mp)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::CLogicalIntersect
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalIntersect::CLogicalIntersect
	(
	IMemoryPool *mp,
	CColRefArray *pdrgpcrOutput,
	ColRefArrays *pdrgpdrgpcrInput
	)
	:
	CLogicalSetOp(mp, pdrgpcrOutput, pdrgpdrgpcrInput)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::~CLogicalIntersect
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalIntersect::~CLogicalIntersect()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalIntersect::Maxcard
	(
	IMemoryPool *, // mp
	CExpressionHandle &exprhdl
	)
	const
{
	// contradictions produce no rows
	if (CDrvdPropRelational::GetRelationalProperties(exprhdl.Pdp())->Ppc()->FContradiction())
	{
		return CMaxCard(0 /*ull*/);
	}

	CMaxCard maxcardL = exprhdl.GetRelationalProperties(0)->Maxcard();
	CMaxCard maxcardR = exprhdl.GetRelationalProperties(1)->Maxcard();

	if (maxcardL <= maxcardR)
	{
		return maxcardL;
	}

	return maxcardR;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalIntersect::PopCopyWithRemappedColumns
	(
	IMemoryPool *mp,
	UlongToColRefMap *colref_mapping,
	BOOL must_exist
	)
{
	CColRefArray *pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput, colref_mapping, must_exist);
	ColRefArrays *pdrgpdrgpcrInput = CUtils::PdrgpdrgpcrRemap(mp, m_pdrgpdrgpcrInput, colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalIntersect(mp, pdrgpcrOutput, pdrgpdrgpcrInput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalIntersect::PxfsCandidates
	(
	IMemoryPool *mp
	)
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfIntersect2Join);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalIntersect::PstatsDerive
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	IStatsArray * // not used
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);

	// intersect is transformed into a group by over an intersect all
	// we follow the same route to compute statistics

	CColRefSetArray *output_colrefsets = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG size = m_pdrgpdrgpcrInput->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, (*m_pdrgpdrgpcrInput)[ul]);
		output_colrefsets->Append(pcrs);
	}

	IStatistics *pstatsIntersectAll =
			CLogicalIntersectAll::PstatsDerive(mp, exprhdl, m_pdrgpdrgpcrInput, output_colrefsets);

	// computed columns
	ULongPtrArray *pdrgpulComputedCols = GPOS_NEW(mp) ULongPtrArray(mp);

	IStatistics *stats = CLogicalGbAgg::PstatsDerive
											(
											mp,
											pstatsIntersectAll,
											(*m_pdrgpdrgpcrInput)[0], // we group by the columns of the first child
											pdrgpulComputedCols, // no computed columns for set ops
											NULL // no keys, use all grouping cols
											);
	// clean up
	pdrgpulComputedCols->Release();
	pstatsIntersectAll->Release();
	output_colrefsets->Release();

	return stats;
}

// EOF
