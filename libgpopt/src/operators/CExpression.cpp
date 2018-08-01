//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CExpression.cpp
//
//	@doc:
//		Implementation of expressions
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CWorker.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/exception.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDrvdPropCtxtRelational.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CPrintPrefix.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/search/CGroupExpression.h"
#include "naucrates/traceflags/traceflags.h"


using namespace gpnaucrates;
using namespace gpopt;

static CHAR szExprLevelWS[] =		"   ";
static CHAR szExprBarLevelWS[] =	"|  ";
static CHAR szExprBarOpPrefix[] =	"|--";
static CHAR szExprPlusOpPrefix[] =	"+--";


//---------------------------------------------------------------------------
//	@function:
//		CExpression::CExpression
//
//	@doc:
//		Ctor for leaf nodes
//
//---------------------------------------------------------------------------
CExpression::CExpression
	(
	IMemoryPool *memory_pool,
	COperator *pop,
	CGroupExpression *pgexpr
	)
	:
	m_memory_pool(memory_pool),
	m_pop(pop),
	m_pdrgpexpr(NULL),
	m_pdprel(NULL),
	m_pstats(NULL),
	m_prpp(NULL),
	m_pdpplan(NULL),
	m_pdpscalar(NULL),
	m_pgexpr(pgexpr),
	m_cost(GPOPT_INVALID_COST),
	m_ulOriginGrpId(gpos::ulong_max),
	m_ulOriginGrpExprId(gpos::ulong_max)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pop);

	if (NULL != pgexpr)
	{
		CopyGroupPropsAndStats(NULL /*input_stats*/);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::CExpression
//
//	@doc:
//		Ctor, unary
//
//---------------------------------------------------------------------------
CExpression::CExpression
	(
	IMemoryPool *memory_pool,
	COperator *pop,
	CExpression *pexpr
	)
	:
	m_memory_pool(memory_pool),
	m_pop(pop),
	m_pdrgpexpr(NULL),
	m_pdprel(NULL),
	m_pstats(NULL),
	m_prpp(NULL),
	m_pdpplan(NULL),
	m_pdpscalar(NULL),
	m_pgexpr(NULL),
	m_cost(GPOPT_INVALID_COST),
	m_ulOriginGrpId(gpos::ulong_max),
	m_ulOriginGrpExprId(gpos::ulong_max)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(NULL != pexpr);

	m_pdrgpexpr = GPOS_NEW(memory_pool) DrgPexpr(memory_pool, 1);
	m_pdrgpexpr->Append(pexpr);

	GPOS_ASSERT(m_pdrgpexpr->Size() == 1);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::CExpression
//
//	@doc:
//		Ctor, binary
//
//---------------------------------------------------------------------------
CExpression::CExpression
	(
	IMemoryPool *memory_pool,
	COperator *pop,
	CExpression *pexprChildFirst,
	CExpression *pexprChildSecond
	)
	:
	m_memory_pool(memory_pool),
	m_pop(pop),
	m_pdrgpexpr(NULL),
	m_pdprel(NULL),
	m_pstats(NULL),
	m_prpp(NULL),
	m_pdpplan(NULL),
	m_pdpscalar(NULL),
	m_pgexpr(NULL),
	m_cost(GPOPT_INVALID_COST),
	m_ulOriginGrpId(gpos::ulong_max),
	m_ulOriginGrpExprId(gpos::ulong_max)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pop);

	GPOS_ASSERT(NULL != pexprChildFirst);
	GPOS_ASSERT(NULL != pexprChildSecond);

	m_pdrgpexpr = GPOS_NEW(memory_pool) DrgPexpr(memory_pool, 2);
	m_pdrgpexpr->Append(pexprChildFirst);
	m_pdrgpexpr->Append(pexprChildSecond);

	GPOS_ASSERT(m_pdrgpexpr->Size() == 2);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::CExpression
//
//	@doc:
//		Ctor, ternary
//
//---------------------------------------------------------------------------
CExpression::CExpression
	(
	IMemoryPool *memory_pool,
	COperator *pop,
	CExpression *pexprChildFirst,
	CExpression *pexprChildSecond,
	CExpression *pexprChildThird
	)
	:
	m_memory_pool(memory_pool),
	m_pop(pop),
	m_pdrgpexpr(NULL),
	m_pdprel(NULL),
	m_pstats(NULL),
	m_prpp(NULL),
	m_pdpplan(NULL),
	m_pdpscalar(NULL),
	m_pgexpr(NULL),
	m_cost(GPOPT_INVALID_COST),
	m_ulOriginGrpId(gpos::ulong_max),
	m_ulOriginGrpExprId(gpos::ulong_max)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pop);

	GPOS_ASSERT(NULL != pexprChildFirst);
	GPOS_ASSERT(NULL != pexprChildSecond);
	GPOS_ASSERT(NULL != pexprChildThird);

	m_pdrgpexpr = GPOS_NEW(memory_pool) DrgPexpr(memory_pool, 3);
	m_pdrgpexpr->Append(pexprChildFirst);
	m_pdrgpexpr->Append(pexprChildSecond);
	m_pdrgpexpr->Append(pexprChildThird);

	GPOS_ASSERT(m_pdrgpexpr->Size() == 3);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::CExpression
//
//	@doc:
//		Ctor, generic n-ary
//
//---------------------------------------------------------------------------
CExpression::CExpression
	(
	IMemoryPool *memory_pool,
	COperator *pop,
	DrgPexpr *pdrgpexpr
	)
	:
	m_memory_pool(memory_pool),
	m_pop(pop),
	m_pdrgpexpr(pdrgpexpr),
	m_pdprel(NULL),
	m_pstats(NULL),
	m_prpp(NULL),
	m_pdpplan(NULL),
	m_pdpscalar(NULL),
	m_pgexpr(NULL),
	m_cost(GPOPT_INVALID_COST),
	m_ulOriginGrpId(gpos::ulong_max),
	m_ulOriginGrpExprId(gpos::ulong_max)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(NULL != pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::CExpression
//
//	@doc:
//		Ctor, generic n-ary with origin group expression
//
//---------------------------------------------------------------------------
CExpression::CExpression
	(
	IMemoryPool *memory_pool,
	COperator *pop,
	CGroupExpression *pgexpr,
	DrgPexpr *pdrgpexpr,
	IStatistics *input_stats,
	CCost cost
	)
	:
	m_memory_pool(memory_pool),
	m_pop(pop),
	m_pdrgpexpr(pdrgpexpr),
	m_pdprel(NULL),
	m_pstats(NULL),
	m_prpp(NULL),
	m_pdpplan(NULL),
	m_pdpscalar(NULL),
	m_pgexpr(pgexpr),
	m_cost(cost),
	m_ulOriginGrpId(gpos::ulong_max),
	m_ulOriginGrpExprId(gpos::ulong_max)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(pgexpr->Arity() == (pdrgpexpr == NULL ? 0 : pdrgpexpr->Size()));
	GPOS_ASSERT(NULL != pgexpr->Pgroup());

	CopyGroupPropsAndStats(input_stats);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::~CExpression
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CExpression::~CExpression()
{
	{
		CAutoSuspendAbort asa;

		CRefCount::SafeRelease(m_pdprel);
		CRefCount::SafeRelease(m_pstats);
		CRefCount::SafeRelease(m_prpp);
		CRefCount::SafeRelease(m_pdpplan);
		CRefCount::SafeRelease(m_pdpscalar);
		CRefCount::SafeRelease(m_pdrgpexpr);

		m_pop->Release();
	}

#ifdef GPOS_DEBUG
	CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::CopyGroupPropsAndStats
//
//	@doc:
//		Copy group properties and stats to expression
//
//---------------------------------------------------------------------------
void
CExpression::CopyGroupPropsAndStats
	(
	IStatistics *input_stats
	)
{
	GPOS_ASSERT(NULL != m_pgexpr);

	CDrvdProp *pdp = m_pgexpr->Pgroup()->Pdp();
	GPOS_ASSERT(NULL != pdp);

	// copy properties
	pdp->AddRef();
	if (m_pgexpr->Pgroup()->FScalar())
	{
		GPOS_ASSERT(NULL == m_pdpscalar);

		m_pdpscalar = CDrvdPropScalar::GetDrvdScalarProps(pdp);
	}
	else
	{
		GPOS_ASSERT(NULL == m_pdprel);

		m_pdprel = CDrvdPropRelational::GetRelationalProperties(pdp);
	}

	IStatistics *stats = NULL;
	if (NULL != input_stats)
	{
		// copy stats from  input
		stats = input_stats;
	}
	else
	{
		// copy stats from group
		stats = m_pgexpr->Pgroup()->Pstats();
	}

	if (NULL != stats)
	{
		stats->AddRef();
		m_pstats = stats;
	}

	m_ulOriginGrpExprId = m_pgexpr->Id();
	m_ulOriginGrpId = m_pgexpr->Pgroup()->Id();
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::Pdp
//
//	@doc:
//		Get derivable property based on operator type;
//		only used internally during property derivation
//
//---------------------------------------------------------------------------
CDrvdProp *
CExpression::Pdp
	(
	const CDrvdProp::EPropType ept
	)
	const
{
	switch (ept)
	{
		case CDrvdProp::EptRelational:
			return m_pdprel;
		case CDrvdProp::EptPlan:
			return m_pdpplan;
		case CDrvdProp::EptScalar:
			return m_pdpscalar;
		default:
			break;
	}

	GPOS_ASSERT(!"Invalid property type");

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::SetPdp
//
//	@doc:
//		Set property m_bytearray_value based on operator type;
//		only used internally during property derivation
//
//---------------------------------------------------------------------------
void
CExpression::SetPdp
	(
	CDrvdProp *pdp,
	const CDrvdProp::EPropType ept
	)
{
	GPOS_ASSERT(NULL != pdp);

	switch (ept)
	{
		case CDrvdProp::EptRelational:
			m_pdprel = CDrvdPropRelational::GetRelationalProperties(pdp);
			break;
		case CDrvdProp::EptPlan:
			m_pdpplan = CDrvdPropPlan::Pdpplan(pdp);
			break;
		case CDrvdProp::EptScalar:
			m_pdpscalar = CDrvdPropScalar::GetDrvdScalarProps(pdp);
			break;
		default:
			GPOS_ASSERT(!"Invalid property type");
			break;
	}
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CExpression::AssertValidPropDerivation
//
//	@doc:
//		Assert valid property derivation
//
//---------------------------------------------------------------------------
void
CExpression::AssertValidPropDerivation
	(
	const CDrvdProp::EPropType ept
	)
{
	COperator *pop = Pop();

	GPOS_ASSERT_IMP(pop->FScalar(), CDrvdProp::EptScalar == ept);
	GPOS_ASSERT_IMP(pop->FLogical(), CDrvdProp::EptRelational == ept);
	GPOS_ASSERT_IMP
		(
		pop->FPattern(),
		CDrvdProp::EptRelational == ept || CDrvdProp::EptScalar == ept
		);

	GPOS_ASSERT_IMP
		(
		pop->FPhysical(),
		CDrvdProp::EptRelational == ept || CDrvdProp::EptPlan == ept
		);

	GPOS_ASSERT_IMP
		(
		pop->FPhysical() && CDrvdProp::EptRelational == ept,
		NULL != m_pdprel &&
		"Relational properties were not copied from Memo"
		);
}

#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CExpression::Ept
//
//	@doc:
//		Get the suitable derived property type based on operator
//
//---------------------------------------------------------------------------
CDrvdProp::EPropType
CExpression::Ept() const
{
	if (Pop()->FLogical())
	{
		return CDrvdProp::EptRelational;
	}

	if (Pop()->FPhysical())
	{
		GPOS_ASSERT(NULL != m_pdprel && "Relational properties were not copied from Memo");
		return CDrvdProp::EptPlan;
	}

	if (Pop()->FScalar())
	{
		return CDrvdProp::EptScalar;
	}

	GPOS_ASSERT(!"Unexpected operator type");
	return CDrvdProp::EptInvalid;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::PdpDerive
//
//	@doc:
//		Derive properties;
//		Determine the suitable derived property type internally
//
//---------------------------------------------------------------------------
CDrvdProp *
CExpression::PdpDerive
	(
	CDrvdPropCtxt *pdpctxt // derivation context, passed by caller
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	const CDrvdProp::EPropType ept = Ept();
#ifdef GPOS_DEBUG
	AssertValidPropDerivation(ept);
#endif // GPOS_DEBUG

	// see if suitable prop is already cached
	if (NULL == Pdp(ept))
	{
		CExpressionHandle exprhdl(m_memory_pool);
		exprhdl.Attach(this);

		// trigger recursive property derivation
		exprhdl.DeriveProps(pdpctxt);
		
		// cache handle's derived properties on expression
		CRefCount::SafeRelease(Pdp(ept));
		CDrvdProp *pdp = exprhdl.Pdp();
		pdp->AddRef();
		SetPdp(pdp, ept);
	}

	return Pdp(ept);
}



//---------------------------------------------------------------------------
//	@function:
//		CExpression::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CExpression::PstatsDerive
	(
	CReqdPropRelational *prprel,
	StatsArray *stats_ctxt
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != prprel);
	GPOS_ASSERT(prprel->FRelational());
	GPOS_CHECK_ABORT;

	if (Pop()->FScalar())
	{
		if (NULL == m_pstats)
		{
			// create an empty statistics container
			m_pstats = CStatistics::MakeEmptyStats(m_memory_pool);
		}

		return m_pstats;
	}

	prprel->AddRef();
	CReqdPropRelational *prprelInput = prprel;

	// if expression has derived statistics, check if requirements are covered
	// by what's already derived
	if (NULL != m_pstats)
	{
		prprelInput->Release();
		CReqdPropRelational *prprelExisting = m_pstats->GetReqdRelationalProps(m_memory_pool);
		prprelInput = prprel->PrprelDifference(m_memory_pool, prprelExisting);
		prprelExisting->Release();

		if (prprelInput->IsEmpty())
		{
			// required statistics columns are already covered by existing statistics

			// clean up
			prprelInput->Release();
			return m_pstats;
		}
	}

	StatsArray *pdrgpstatCtxtNew = stats_ctxt;
	if (NULL == stats_ctxt)
	{
		// create an empty context
		pdrgpstatCtxtNew = GPOS_NEW(m_memory_pool) StatsArray(m_memory_pool);
	}
	else
	{
		pdrgpstatCtxtNew->AddRef();
	}

	// trigger recursive property derivation
	CExpressionHandle exprhdl(m_memory_pool);
	exprhdl.Attach(this);
	CDrvdPropCtxtRelational *pdpctxtrel = GPOS_NEW(m_memory_pool) CDrvdPropCtxtRelational(m_memory_pool);
	exprhdl.DeriveProps(pdpctxtrel);

	// compute required relational properties of expression's children
	exprhdl.ComputeReqdProps(prprelInput, 0 /*ulOptReq*/);

	// trigger recursive statistics derivation
	exprhdl.DeriveStats(pdrgpstatCtxtNew);

	// cache derived stats on expression
	IStatistics *stats = exprhdl.Pstats();
	GPOS_ASSERT(NULL != stats);

	if (NULL == m_pstats)
	{
		stats->AddRef();
		m_pstats = stats;
	}
	else
	{
		IStatistics *stats_copy = stats->CopyStats(m_memory_pool);
		stats_copy->AppendStats(m_memory_pool, m_pstats);

		m_pstats->Release();
		m_pstats = NULL;
		m_pstats = stats_copy;
	}
	GPOS_ASSERT(NULL != m_pstats);

	// clean up
	prprelInput->Release();
	pdrgpstatCtxtNew->Release();
	pdpctxtrel->Release();

	return m_pstats;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::ResetDerivedProperty
//
//	@doc:
//		Reset a derived property
//
//---------------------------------------------------------------------------
void
CExpression::ResetDerivedProperty
	(
	CDrvdProp::EPropType ept
	)
{
	switch (ept)
	{
		case CDrvdProp::EptRelational:
			CRefCount::SafeRelease(m_pdprel);
			m_pdprel = NULL;
			break;
		case CDrvdProp::EptPlan:
			CRefCount::SafeRelease(m_pdpplan);
			m_pdpplan = NULL;
			break;
		case CDrvdProp::EptScalar:
			CRefCount::SafeRelease(m_pdpscalar);
			m_pdpscalar  = NULL;
			break;
		default:
			GPOS_ASSERT(!"Invalid property type");
			break;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::ResetDerivedProperties
//
//	@doc:
//		Reset all derived properties
//
//---------------------------------------------------------------------------
void
CExpression::ResetDerivedProperties()
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;

	CDrvdProp::EPropType rgept[] =
		{
		CDrvdProp::EptRelational,
		CDrvdProp::EptScalar,
		CDrvdProp::EptPlan
		};

	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgept); i++)
	{
		// reset self
		ResetDerivedProperty(rgept[i]);
	}

	for (ULONG i = 0; i <  Arity(); i++)
	{
		// reset children
		(*this)[i]->ResetDerivedProperties();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::ResetStats
//
//	@doc:
//		Reset stats on expression tree
//
//---------------------------------------------------------------------------
void
CExpression::ResetStats()
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;

	// reset stats on self
	CRefCount::SafeRelease(m_pstats);
	m_pstats = NULL;

	const ULONG arity = Arity();
	for (ULONG ul = 0; ul <  arity; ul++)
	{
		// reset children stats
		(*this)[ul]->ResetStats();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::HasOuterRefs
//
//	@doc:
//		Check for outer references
//
//
//---------------------------------------------------------------------------
BOOL
CExpression::HasOuterRefs()
{
	return (0 < CDrvdPropRelational::GetRelationalProperties(PdpDerive())->PcrsOuter()->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CExpression::PrppCompute
//
//	@doc:
//		Compute required plan properties of all nodes in expression tree
//
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CExpression::PrppCompute
	(
	IMemoryPool *memory_pool,
	CReqdPropPlan *prppInput
	)
{
	// derive plan properties
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(memory_pool) CDrvdPropCtxtPlan(memory_pool);
	(void) PdpDerive(pdpctxtplan);
	pdpctxtplan->Release();

	// decorate nodes with required properties
	return PrppDecorate(memory_pool, prppInput);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::PrppDecorate
//
//	@doc:
//		Decorate all expression nodes with required properties
//
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CExpression::PrppDecorate
	(
	IMemoryPool *memory_pool,
	CReqdPropPlan *prppInput
	)
{
	// if operator is physical, trigger property computation
	if (Pop()->FPhysical())
	{
		GPOS_CHECK_STACK_SIZE;
		GPOS_ASSERT(NULL != memory_pool);
		GPOS_ASSERT(NULL != prppInput);

		CRefCount::SafeRelease(m_prpp);

		CExpressionHandle exprhdl(memory_pool);
		exprhdl.Attach(this);

		// init required properties of expression
		exprhdl.InitReqdProps(prppInput);

		// create array of child derived properties
		DrgPdp *pdrgpdp = GPOS_NEW(m_memory_pool) DrgPdp(m_memory_pool);

		const ULONG arity =  Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			// compute required columns of the n-th child
			exprhdl.ComputeChildReqdCols(ul, pdrgpdp);

			CExpression *pexprChild = (*this)[ul];
			(void) pexprChild->PrppCompute(memory_pool, exprhdl.Prpp(ul));

			// add plan props of current child to derived props array
			CDrvdProp *pdp = pexprChild->PdpDerive();
			pdp->AddRef();
			pdrgpdp->Append(pdp);
		}

		// cache handle's required properties on expression
		m_prpp = CReqdPropPlan::Prpp(exprhdl.Prp());
		m_prpp->AddRef();

		pdrgpdp->Release();
	}

	return m_prpp;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::FMatchPattern
//
//	@doc:
//		Check a pattern expression against a given group;
//		shallow, do not	match its children, check only arity of the root
//
//---------------------------------------------------------------------------
BOOL
CExpression::FMatchPattern
	(
	CGroupExpression *pgexpr
	) 
	const
{
	GPOS_ASSERT(NULL != pgexpr);
	
	if (this->Pop()->FPattern())
	{
		// a pattern operator matches any group expression
		return true;
	}
	else
	{
		ULONG arity = Arity();
		BOOL fMultiNode =
			(
				(1 == arity || 2 == arity) && // has 2 or fewer children
				CPattern::FMultiNode((*this)[0]->Pop()) // child is multileaf or a multitree
			);
		
		// match operator id and arity
		if (this->Pop()->Eopid() == pgexpr->Pop()->Eopid() &&
				(this->Arity() == pgexpr->Arity() || (fMultiNode && pgexpr->Arity() > 1)))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpression::Matches
//
//	@doc:
//		Recursive comparison of this expression against another given one
//
//---------------------------------------------------------------------------
BOOL
CExpression::Matches
	(
	CExpression *pexpr
	) 
	const
{
	GPOS_CHECK_STACK_SIZE;
	
	// check local operator
	if (!Pop()->Matches(pexpr->Pop()))
	{
		return false;
	}
		
	ULONG arity = Arity();
	if (arity != pexpr->Arity())
	{
		return false;
	}
	
	// decend into children
	for(ULONG ul = 0; ul < arity; ul++)
	{
		if (!(*this)[ul]->Matches((*pexpr)[ul]))
		{
			return false;
		}
	}
	
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpression::PexprCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the expression with remapped columns
//
//---------------------------------------------------------------------------
CExpression *
CExpression::PexprCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	UlongColRefHashMap *colref_mapping,
	BOOL must_exist
	)
	const
{
	GPOS_ASSERT(NULL != m_pop);
	// this is only valid for logical and scalar expressions
	GPOS_ASSERT(m_pop->FLogical() || m_pop->FScalar());

	// copy children
	DrgPexpr *pdrgpexpr = GPOS_NEW(memory_pool) DrgPexpr(memory_pool);
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*m_pdrgpexpr)[ul];
		pdrgpexpr->Append(pexprChild->PexprCopyWithRemappedColumns(memory_pool, colref_mapping, must_exist));
	}

	COperator *pop = m_pop->PopCopyWithRemappedColumns(memory_pool, colref_mapping, must_exist);

	if (0 == arity)
	{
		pdrgpexpr->Release();
		return GPOS_NEW(memory_pool) CExpression(memory_pool, pop);
	}

	return GPOS_NEW(memory_pool) CExpression(memory_pool, pop, pdrgpexpr);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CExpression::FMatchPattern
//
//	@doc:
//		Check expression against a given pattern;
//
//---------------------------------------------------------------------------
BOOL
CExpression::FMatchPattern
	(
	CExpression *pexprPattern
	) 
	const
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprPattern);
	
	COperator::EOperatorId op_id = pexprPattern->Pop()->Eopid();
	
	if (COperator::EopPatternLeaf == op_id ||
		COperator::EopPatternTree == op_id)
	{
		// leaf and tree operators always match
		return true;
	}
	
	if (Pop()->Eopid() == op_id)
	{
		// check arity, children
		return FMatchPatternChildren(pexprPattern);
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::FMatchPatternChildren
//
//	@doc:
//		Check expression's children against a given pattern's children;
//
//---------------------------------------------------------------------------
BOOL
CExpression::FMatchPatternChildren
	(
	CExpression *pexprPattern
	) 
	const
{
	GPOS_CHECK_STACK_SIZE;

	ULONG arity = Arity();
	ULONG ulArityPattern = pexprPattern->Arity();
		
	BOOL fMultiNode =
			(
				(1 == ulArityPattern || 2 == ulArityPattern) &&
				CPattern::FMultiNode((*pexprPattern)[0]->Pop())
			);

	if (fMultiNode)
	{
		// match if there are multiple children;
		// allow only-children to match multi children against its own pattern in asserts;
		if (1 == ulArityPattern)
		{
			return arity > 0;
		}
		else 
		{
			// check last child explicitly
			CExpression *pexpr = (*this)[arity - 1];
			return arity > 1 && pexpr->FMatchPattern((*pexprPattern)[ulArityPattern - 1]);
		}
	}

	// all other matches must have same arity
	if (arity != ulArityPattern)
	{
		return false;
	}

	BOOL fMatch = true;
	for (ULONG ul = 0; ul < arity && fMatch; ul++)
	{
		CExpression *pexpr = (*this)[ul];
		fMatch = fMatch && pexpr->FMatchPattern((*pexprPattern)[ul]);
	}
	
	return fMatch;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::FMatchDebug
//
//	@doc:
//		Recursive comparison of this expression against another given one
//
//---------------------------------------------------------------------------
BOOL
CExpression::FMatchDebug
	(
	CExpression *pexpr
	) 
	const
{
	GPOS_CHECK_STACK_SIZE;
	
	// check local operator
	if (!Pop()->Matches(pexpr->Pop()))
	{
		GPOS_ASSERT(Pop()->HashValue() == pexpr->Pop()->HashValue());
		return false;
	}
	
	// operator match must be commutative
	GPOS_ASSERT(pexpr->Pop()->Matches(Pop()));
	
	// scalar operators must agree on return type
	GPOS_ASSERT_IMP
		(
		Pop()->FScalar() && 
			CScalar::EopScalarProjectList != Pop()->Eopid() &&
			CScalar::EopScalarProjectElement != Pop()->Eopid(),
		CScalar::PopConvert(pexpr->Pop())->MDIdType()->Equals(CScalar::PopConvert(Pop())->MDIdType())
		);
	
	ULONG arity = Arity();
	
	if (arity != pexpr->Arity())
	{
		return false;
	}
	// inner nodes have same sensitivity
	GPOS_ASSERT_IMP
		(
		0 < arity,
		Pop()->FInputOrderSensitive() == pexpr->Pop()->FInputOrderSensitive()
		);
	
	// decend into children
	for(ULONG ul = 0; ul < arity; ul++)
	{
		if (!(*this)[ul]->FMatchDebug((*pexpr)[ul]))
		{
			return false;
		}
	}
	
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpression::PrintProperties
//
//	@doc:
//		Print expression properties;
//
//---------------------------------------------------------------------------
void
CExpression::PrintProperties
	(
	IOstream &os,
	CPrintPrefix &pfx
	)
	const
{
	GPOS_CHECK_ABORT;

	if (NULL != m_pdprel)
	{
		os << pfx << "DrvdRelProps:{" << *m_pdprel << "}" << std::endl;
	}

	if (NULL != m_pdpscalar)
	{
		os << pfx << "DrvdScalarProps:{" << *m_pdpscalar << "}" << std::endl;
	}

	if (NULL != m_pdpplan)
	{
		os << pfx << "DrvdPlanProps:{" << *m_pdpplan << "}" << std::endl;
	}

	if (NULL != m_prpp)
	{
		os << pfx << "ReqdPlanProps:{" << *m_prpp << "}" << std::endl;
	}
}

// ----------------------------------------------------------------
//	Print driving functions for use in interactive debugging;
//	always prints to stderr.
// ----------------------------------------------------------------

// prints expression only
void
CExpression::DbgPrint() const
{
	CAutoTrace at(m_pmp);
	(void) this->OsPrint(at.Os());
}

// Prints expression along with it's properties
void
CExpression::DbgPrintWithProperties() const
{
	CAutoTraceFlag atf(EopttracePrintExpressionProperties, true);
	CAutoTrace at(m_memory_pool);
	(void) this->OsPrint(at.Os());
}

#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CExpression::OsPrint
//
//	@doc:
//		Print driving function
//
//---------------------------------------------------------------------------
IOstream &
CExpression::OsPrint
	(
	IOstream &os,
	const CPrintPrefix *ppfx,
	BOOL fLast
	)
	const
{
	// recursive, check stack depth
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	if (NULL != ppfx)
	{
		(void) ppfx->OsPrint(os);
	}

	CHAR *szChildPrefix = NULL;
	if (fLast)
	{
		os << szExprPlusOpPrefix;
		szChildPrefix = szExprLevelWS;
	}
	else
	{
		os << szExprBarOpPrefix;
		szChildPrefix = szExprBarLevelWS;
	}
	
	(void) m_pop->OsPrint(os);
	if (!m_pop->FScalar() && NULL != m_pstats)
	{
		os
			<< "   rows:"
			<< LINT(m_pstats->Rows().Get())
			<< "   width:"
			<< LINT(m_pstats->Width().Get())
			<< "  rebinds:"
			<< LINT(m_pstats->NumRebinds().Get());
	}
	if (m_pop->FPhysical())
	{
		os << "   cost:" << m_cost;
	}
	if (gpos::ulong_max != m_ulOriginGrpId)
	{
		os << "   origin: [Grp:" << m_ulOriginGrpId << ", GrpExpr:" << m_ulOriginGrpExprId<< "]";
	}
	os << std::endl;

	CPrintPrefix pfx(ppfx, szChildPrefix);

#ifdef GPOS_DEBUG
	if (GPOS_FTRACE(EopttracePrintExpressionProperties))
	{
		PrintProperties(os, pfx);
	}
#endif // GPOS_DEBUG
		
	const ULONG ulChildren = this->Arity();
	for (ULONG i = 0; i < ulChildren; i++)
	{
		(*this)[i]->OsPrint
					(
					os,
					&pfx,
					i == (ulChildren - 1)
					);
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CExpression::HashValue
	(
	const CExpression *pexpr
	)
{
	GPOS_CHECK_STACK_SIZE;

	ULONG ulHash = pexpr->Pop()->HashValue();

	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ulHash = CombineHashes(ulHash, HashValue((*pexpr)[ul]));
	}

	return ulHash;
}


// Less strict hash function to support expressions that are not order
// sensitive. This hash function specifically used in CUtils::PdrgpexprDedup
// for deduping the expressions in a given list.
ULONG
CExpression::UlHashDedup
	(
	const CExpression *pexpr
	)
{
	GPOS_CHECK_STACK_SIZE;

	ULONG ulHash = pexpr->Pop()->HashValue();

	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if(pexpr->Pop()->FInputOrderSensitive())
		{
			// If the two expressions are order sensitive, then even though
			// thir inputs are the same, if the order of the inputs are not the
			// same, hash function puts two different expressions into separate
			// buckets.
			// e.g logically a < b is not equal to b < a
			ulHash = CombineHashes(ulHash, HashValue((*pexpr)[ul]));
		}
		else
		{
			// If the two expressions are not order sensitive and their
			// inputs are the same, the expressions are considered as equal
			// and fall into the same bucket in the hash map.
			//  e.g logically a = b is equal to b = a
			ulHash ^= HashValue((*pexpr)[ul]);
		}
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpression::PexprRehydrate
//
//	@doc:
//		Rehydrate expression from a given cost context and child expressions
//
//---------------------------------------------------------------------------
CExpression *
CExpression::PexprRehydrate
	(
	IMemoryPool *memory_pool,
	CCostContext *pcc,
	DrgPexpr *pdrgpexpr,
	CDrvdPropCtxtPlan *pdpctxtplan
	)
{
	GPOS_ASSERT(NULL != pcc);
	GPOS_ASSERT(NULL != pdpctxtplan);

	CGroupExpression *pgexpr = pcc->Pgexpr();
	COperator *pop = pgexpr->Pop();
	pop->AddRef();

	CCost cost = pcc->Cost();
	if (pop->FPhysical())
	{
		const ULONG arity = pgexpr->Arity();
		DrgPcost *pdrgpcost = GPOS_NEW(memory_pool) DrgPcost(memory_pool);
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprChild = (*pdrgpexpr)[ul];
			CCost costChild = pexprChild->Cost();
			pdrgpcost->Append(GPOS_NEW(memory_pool) CCost(costChild));
		}
		cost = pcc->CostCompute(memory_pool, pdrgpcost);
		pdrgpcost->Release();
	}
	CExpression *pexpr = GPOS_NEW(memory_pool) CExpression(memory_pool, pop, pgexpr, pdrgpexpr,
                                              pcc->Pstats(), CCost(cost));

	// set the number of expected partition selectors in the context
	pdpctxtplan->SetExpectedPartitionSelectors(pop, pcc);

	if (pop->FPhysical() && !pexpr->FValidPlan(pcc->Poc()->Prpp(), pdpctxtplan))
	{
#ifdef GPOS_DEBUG
		{
			CAutoTrace at(memory_pool);
			IOstream &os = at.Os();

			os << std::endl << "INVALID EXPRESSION: " << std::endl << *pexpr;
			os << std::endl << "REQUIRED PROPERTIES: " << std::endl << *(pcc->Poc()->Prpp());
			os << std::endl << "DERIVED PROPERTIES: " << std::endl << *CDrvdPropPlan::Pdpplan(pexpr->PdpDerive()) << std::endl;
		}
#endif  // GPOS_DEBUG
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsatisfiedRequiredProperties);
	}
	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpression::FValidPlan
//
//	@doc:
//		Check if the expression satisfies given required properties.
//
//---------------------------------------------------------------------------
BOOL
CExpression::FValidPlan
	(
	const CReqdPropPlan *prpp,
	CDrvdPropCtxtPlan *pdpctxtplan
	)
{
	GPOS_ASSERT(Pop()->FPhysical());
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdpctxtplan);

	CExpressionHandle exprhdl(m_memory_pool);
	exprhdl.Attach(this);
	exprhdl.DeriveProps(pdpctxtplan);
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(exprhdl.Pdp());


	if (COperator::EopPhysicalCTEProducer == m_pop->Eopid())
	{
		ULONG ulCTEId = CPhysicalCTEProducer::PopConvert(m_pop)->UlCTEId();
		pdpctxtplan->CopyCTEProducerProps(pdpplan, ulCTEId);
	}

	CDrvdPropRelational *pdprel = CDrvdPropRelational::GetRelationalProperties(Pdp(CDrvdProp::EptRelational));

	return prpp->FCompatible(exprhdl, CPhysical::PopConvert(m_pop), pdprel, pdpplan)
	        && FValidChildrenDistribution(pdpctxtplan)
	        && FValidPartEnforcers(pdpctxtplan);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpression::FValidChildrenDistribution
//
//	@doc:
//		Check if the distributions of all children are compatible.
//
//---------------------------------------------------------------------------
BOOL
CExpression::FValidChildrenDistribution
	(
	CDrvdPropCtxtPlan *pdpctxtplan
	)
{
	GPOS_ASSERT(Pop()->FPhysical());

	CPhysical *pop = CPhysical::PopConvert(Pop());
	CExpressionHandle exprhdl(m_memory_pool);
	exprhdl.Attach(this);
	exprhdl.DeriveProps(pdpctxtplan);

	if (!pop->FCompatibleChildrenDistributions(exprhdl))
	{

		return false;
	}

	// we cannot enforce a motion gather if the input is already on the master
	if (COperator::EopPhysicalMotionGather == Pop()->Eopid())
	{
		CExpression *pexprChild = (*this)[0];
		CDrvdPropPlan *pdpplanChild = CDrvdPropPlan::Pdpplan(pexprChild->PdpDerive(pdpctxtplan));
		if (CDistributionSpec::EdtSingleton == pdpplanChild->Pds()->Edt() ||
			CDistributionSpec::EdtStrictSingleton == pdpplanChild->Pds()->Edt())
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpression::FValidPartEnforcers
//
//	@doc:
//		Check if the expression is valid with respect to the partition enforcers.
//
//---------------------------------------------------------------------------
BOOL
CExpression::FValidPartEnforcers
	(
	CDrvdPropCtxtPlan *pdpctxtplan
	)
{
	GPOS_ASSERT(Pop()->FPhysical());

	CDrvdPropRelational *pdprel = CDrvdPropRelational::GetRelationalProperties(Pdp(CDrvdProp::EptRelational));
	CPartInfo *ppartinfo = pdprel->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfo);

	if (0 == ppartinfo->UlConsumers())
	{
		// no part consumers found
		return true;
	}

	// retrieve plan properties
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(PdpDerive(pdpctxtplan));

	if (CUtils::FPhysicalMotion(Pop()) && pdpplan->Ppim()->FContainsUnresolved())
	{
		// prohibit Motion on top of unresolved partition consumers
		return false;
	}

	return true;
}

// EOF
