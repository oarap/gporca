//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc..
//
//	@filename:
//		CLogicalMaxOneRow.cpp
//
//	@doc:
//		Implementation of logical MaxOneRow operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalMaxOneRow.h"

#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CLogicalMaxOneRow::Esp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// low promise for stat derivation if logical expression has outer-refs
	// or is part of an Apply expression
	if (exprhdl.HasOuterRefs() ||
		 (NULL != exprhdl.Pgexpr() &&
			CXformUtils::FGenerateApply(exprhdl.Pgexpr()->ExfidOrigin()))
		)
	{
		 return EspLow;
	}

	return EspHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PcrsStat
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalMaxOneRow::PcrsStat
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsInput,
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(memory_pool) CColRefSet(memory_pool);
	pcrs->Union(pcrsInput);

	// intersect with the output columns of relational child
	pcrs->Intersection(exprhdl.GetRelationalProperties(child_index)->PcrsOutput());

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PxfsCandidates
//
//	@doc:
//		Compute candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalMaxOneRow::PxfsCandidates
	(
	IMemoryPool *memory_pool
	)
	const
{
	CXformSet *xform_set = GPOS_NEW(memory_pool) CXformSet(memory_pool);
	(void) xform_set->ExchangeSet(CXform::ExfMaxOneRow2Assert);
	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalMaxOneRow::PstatsDerive
			(
			IMemoryPool *memory_pool,
			CExpressionHandle &exprhdl,
			StatsArray * // stats_ctxt
			)
			const
{
	// no more than one row can be produced by operator, scale down input statistics accordingly
	IStatistics *stats = exprhdl.Pstats(0);
	return  stats->ScaleStats(memory_pool, CDouble(1.0 / stats->Rows()));
}


// EOF

