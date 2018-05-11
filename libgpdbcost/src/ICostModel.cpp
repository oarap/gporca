//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		ICostModel.cpp
//
//	@doc:
//		Cost model implementation
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"
#include "gpdbcost/CCostModelGPDBLegacy.h"

using namespace gpopt;
using namespace gpdbcost;

// default number segments for the cost model
#define GPOPT_DEFAULT_SEGMENT_COUNT 2 

//---------------------------------------------------------------------------
//	@function:
//		ICostModel::PcmDefault
//
//	@doc:
//		Create default cost model
//
//---------------------------------------------------------------------------
ICostModel *
ICostModel::PcmDefault
	(
	IMemoryPool *memory_pool
	)
{
	return GPOS_NEW(memory_pool) CCostModelGPDBLegacy(memory_pool, GPOPT_DEFAULT_SEGMENT_COUNT);
}


//---------------------------------------------------------------------------
//	@function:
//		ICostModel::SetParams
//
//	@doc:
//		Set cost model params
//
//---------------------------------------------------------------------------
void
ICostModel::SetParams
	(
	DrgPcp *pdrgpcp
	)
{
	if (NULL == pdrgpcp)
	{
		return;
	}

	// overwrite default values of cost model parameters
	const ULONG size = pdrgpcp->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		ICostModelParams::SCostParam *pcp = (*pdrgpcp)[ul];
		GetCostModelParams()->SetParam(pcp->Id(), pcp->Get(), pcp->GetLowerBoundVal(), pcp->GetUpperBoundVal());
	}
}


// EOF
