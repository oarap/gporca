//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotionGather.cpp
//
//	@doc:
//		Implementation of gather motion operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotionGather.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::CPhysicalMotionGather
//
//	@doc:
//		Ctor: create a non order-preserving motion
//
//---------------------------------------------------------------------------
CPhysicalMotionGather::CPhysicalMotionGather
	(
	IMemoryPool *memory_pool,
	CDistributionSpecSingleton::ESegmentType est
	)
	:
	CPhysicalMotion(memory_pool),
	m_pdssSingeton(NULL),
	m_pcrsSort(NULL)
{
	GPOS_ASSERT(CDistributionSpecSingleton::EstSentinel != est);

	m_pdssSingeton = GPOS_NEW(memory_pool) CDistributionSpecSingleton(est);
	m_pos = GPOS_NEW(m_memory_pool) COrderSpec(m_memory_pool);
	m_pcrsSort = m_pos->PcrsUsed(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::CPhysicalMotionGather
//
//	@doc:
//		Ctor: create an order-preserving motion
//
//---------------------------------------------------------------------------
CPhysicalMotionGather::CPhysicalMotionGather
	(
	IMemoryPool *memory_pool,
	CDistributionSpecSingleton::ESegmentType est,
	COrderSpec *pos
	)
	:
	CPhysicalMotion(memory_pool),
	m_pdssSingeton(NULL),
	m_pos(pos),
	m_pcrsSort(NULL)
{
	GPOS_ASSERT(CDistributionSpecSingleton::EstSentinel != est);
	GPOS_ASSERT(NULL != pos);

	m_pdssSingeton = GPOS_NEW(memory_pool) CDistributionSpecSingleton(est);
	m_pcrsSort = m_pos->PcrsUsed(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::~CPhysicalMotionGather
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionGather::~CPhysicalMotionGather()
{
	m_pos->Release();
	m_pdssSingeton->Release();
	m_pcrsSort->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionGather::Matches
	(
	COperator *pop
	)
	const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}
	
	CPhysicalMotionGather *popGather = CPhysicalMotionGather::PopConvert(pop);
	
	return Est() == popGather->Est() && m_pos->Matches(popGather->Pos());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionGather::PcrsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG child_index,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(memory_pool) CColRefSet(memory_pool, *m_pcrsSort);
	pcrs->Union(pcrsRequired);

	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(memory_pool, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionGather::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionGather::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	if (!FOrderPreserving())
	{
		return CEnfdProp::EpetRequired;
	}
	
	if (peo->FCompatible(m_pos))
	{
		// required order is already established by gather merge operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required order is incompatible with the order established by the
	// gather merge operator, prohibit adding another sort operator on top
	return CEnfdProp::EpetProhibited;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionGather::PosRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	COrderSpec *,//posInput,
	ULONG child_index,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == child_index);

	if (FOrderPreserving())
	{
		return PosPassThru(memory_pool, exprhdl, m_pos, child_index);
	}

	return GPOS_NEW(memory_pool) COrderSpec(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionGather::PosDerive
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle & // exprhdl
	)
	const
{
	m_pos->AddRef();
	return m_pos;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionGather::OsPrint
	(
	IOstream &os
	)
	const
{
	const CHAR *szLocation = FOnMaster() ? "(master)" : "(segment)";
	os	<< SzId() << szLocation;
	
	if (FOrderPreserving())
	{
		Pos()->OsPrint(os);
	}
	
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionGather *
CPhysicalMotionGather::PopConvert
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopPhysicalMotionGather == pop->Eopid());
	
	return dynamic_cast<CPhysicalMotionGather*>(pop);
}			

// EOF

