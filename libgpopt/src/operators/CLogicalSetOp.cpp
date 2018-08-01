//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CLogicalSetOp.cpp
//
//	@doc:
//		Implementation of setops
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CConstraintNegation.h"
#include "gpopt/operators/CLogicalSetOp.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp
	(
	IMemoryPool *memory_pool
	)
	:
	CLogical(memory_pool),
	m_pdrgpcrOutput(NULL),
	m_pdrgpdrgpcrInput(NULL),
	m_pcrsOutput(NULL),
	m_pdrgpcrsInput(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp
	(
	IMemoryPool *memory_pool,
	ColRefArray *pdrgpcrOutput,
	ColRefArrays *pdrgpdrgpcrInput
	)
	:
	CLogical(memory_pool),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pdrgpdrgpcrInput(pdrgpdrgpcrInput),
	m_pcrsOutput(NULL),
	m_pdrgpcrsInput(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);

	BuildColumnSets(memory_pool);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp
	(
	IMemoryPool *memory_pool,
	ColRefArray *pdrgpcrOutput,
	ColRefArray *pdrgpcrLeft,
	ColRefArray *pdrgpcrRight
	)
	:
	CLogical(memory_pool),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pdrgpdrgpcrInput(NULL)
{	
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpcrLeft);
	GPOS_ASSERT(NULL != pdrgpcrRight);

	m_pdrgpdrgpcrInput = GPOS_NEW(memory_pool) ColRefArrays(memory_pool, 2);

	m_pdrgpdrgpcrInput->Append(pdrgpcrLeft);
	m_pdrgpdrgpcrInput->Append(pdrgpcrRight);

	BuildColumnSets(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::~CLogicalSetOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalSetOp::~CLogicalSetOp()
{
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrInput);
	CRefCount::SafeRelease(m_pcrsOutput);
	CRefCount::SafeRelease(m_pdrgpcrsInput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::BuildColumnSets
//
//	@doc:
//		Build set representation of input/output columns for faster
//		set operations
//
//---------------------------------------------------------------------------
void
CLogicalSetOp::BuildColumnSets
	(
	IMemoryPool *memory_pool
	)
{
	GPOS_ASSERT(NULL != m_pdrgpcrOutput);
	GPOS_ASSERT(NULL != m_pdrgpdrgpcrInput);
	GPOS_ASSERT(NULL == m_pcrsOutput);
	GPOS_ASSERT(NULL == m_pdrgpcrsInput);

	m_pcrsOutput = GPOS_NEW(memory_pool) CColRefSet(memory_pool, m_pdrgpcrOutput);
	m_pdrgpcrsInput = GPOS_NEW(memory_pool) ColRefSetArray(memory_pool);
	const ULONG ulChildren = m_pdrgpdrgpcrInput->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CColRefSet *pcrsInput = GPOS_NEW(memory_pool) CColRefSet(memory_pool, (*m_pdrgpdrgpcrInput)[ul]);
		m_pdrgpcrsInput->Append(pcrsInput);

		m_pcrsLocalUsed->Include(pcrsInput);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSetOp::PcrsDeriveOutput
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	)
{
#ifdef GPOS_DEBUG
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColRefSet *pcrsChildOutput = exprhdl.GetRelationalProperties(ul)->PcrsOutput();
		CColRefSet *pcrsInput = (*m_pdrgpcrsInput)[ul];
		GPOS_ASSERT(pcrsChildOutput->ContainsAll(pcrsInput) &&
				"Unexpected outer references in SetOp input");
	}
#endif // GPOS_DEBUG

	m_pcrsOutput->AddRef();

	return m_pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalSetOp::PkcDeriveKeys
	(
	IMemoryPool *memory_pool,
	CExpressionHandle & // exprhdl
	)
	const
{
	// TODO: 3/3/2012 - ; we can do better by remapping the keys between
	// all children and check if they align

	// True set ops return sets, hence, all output columns are keys
	m_pdrgpcrOutput->AddRef();
	return GPOS_NEW(memory_pool) CKeyCollection(memory_pool, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PpartinfoDerive
//
//	@doc:
//		Derive partition consumer info
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalSetOp::PpartinfoDerive
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG arity = exprhdl.Arity();
	GPOS_ASSERT(0 < arity);

	// start with the part info of the first child
	CPartInfo *ppartinfo = exprhdl.GetRelationalProperties(0 /*child_index*/)->Ppartinfo();
	ppartinfo->AddRef();

	for (ULONG ul = 1; ul < arity; ul++)
	{
		CPartInfo *ppartinfoChild = exprhdl.GetRelationalProperties(ul)->Ppartinfo();
		GPOS_ASSERT(NULL != ppartinfoChild);

		ColRefArray *pdrgpcrInput = (*m_pdrgpdrgpcrInput)[ul];
		GPOS_ASSERT(pdrgpcrInput->Size() == m_pdrgpcrOutput->Size());

		CPartInfo *ppartinfoRemapped = ppartinfoChild->PpartinfoWithRemappedKeys(memory_pool, pdrgpcrInput, m_pdrgpcrOutput);
		CPartInfo *ppartinfoCombined = CPartInfo::PpartinfoCombine(memory_pool, ppartinfo, ppartinfoRemapped);
		ppartinfoRemapped->Release();

		ppartinfo->Release();
		ppartinfo = ppartinfoCombined;
	}

	return ppartinfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalSetOp::Matches
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalSetOp *popSetOp = CLogicalSetOp::PopConvert(pop);
	ColRefArrays *pdrgpdrgpcrInput = popSetOp->PdrgpdrgpcrInput();
	const ULONG arity = pdrgpdrgpcrInput->Size();

	if (arity != m_pdrgpdrgpcrInput->Size() ||
		!m_pdrgpcrOutput->Equals(popSetOp->PdrgpcrOutput()))
	{
		return false;
	}

	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!(*m_pdrgpdrgpcrInput)[ul]->Equals((*pdrgpdrgpcrInput)[ul]))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcrsOutputEquivClasses
//
//	@doc:
//		Get output equivalence classes
//
//---------------------------------------------------------------------------
ColRefSetArray *
CLogicalSetOp::PdrgpcrsOutputEquivClasses
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	BOOL fIntersect
	)
	const
{
	const ULONG ulChildren = exprhdl.Arity();
	ColRefSetArray *pdrgpcrs = PdrgpcrsInputMapped(memory_pool, exprhdl, 0 /*ulChild*/);

	for (ULONG ul = 1; ul < ulChildren; ul++)
	{
		ColRefSetArray *pdrgpcrsChild = PdrgpcrsInputMapped(memory_pool, exprhdl, ul);
		ColRefSetArray *pdrgpcrsMerged = NULL;

		if (fIntersect)
		{
			// merge with the equivalence classes we have so far
			pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(memory_pool, pdrgpcrs, pdrgpcrsChild);
		}
		else
		{
			// in case of a union, an equivalence class must be coming from all
			// children to be part of the output
			pdrgpcrsMerged = CUtils::PdrgpcrsIntersectEquivClasses(memory_pool, pdrgpcrs, pdrgpcrsChild);

		}
		pdrgpcrsChild->Release();
		pdrgpcrs->Release();
		pdrgpcrs = pdrgpcrsMerged;
	}

	return pdrgpcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcrsInputMapped
//
//	@doc:
//		Get equivalence classes from one input child, mapped to output columns
//
//---------------------------------------------------------------------------
ColRefSetArray *
CLogicalSetOp::PdrgpcrsInputMapped
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	ULONG ulChild
	)
	const
{
	ColRefSetArray *pdrgpcrsInput = exprhdl.GetRelationalProperties(ulChild)->Ppc()->PdrgpcrsEquivClasses();
	const ULONG length = pdrgpcrsInput->Size();

	CColRefSet* pcrsChildInput = (*m_pdrgpcrsInput)[ulChild];
	ColRefSetArray *pdrgpcrs = GPOS_NEW(memory_pool) ColRefSetArray(memory_pool);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRefSet *pcrs = GPOS_NEW(memory_pool) CColRefSet(memory_pool);
		pcrs->Include((*pdrgpcrsInput)[ul]);
		pcrs->Intersection(pcrsChildInput);

		if (0 == pcrs->Size())
		{
			pcrs->Release();
			continue;
		}

		// replace each input column with its corresponding output column
		pcrs->Replace((*m_pdrgpdrgpcrInput)[ulChild], m_pdrgpcrOutput);

		pdrgpcrs->Append(pcrs);
	}

	return pdrgpcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcnstrColumn
//
//	@doc:
//		Get constraints for a given output column from all children
//
//---------------------------------------------------------------------------
ConstraintArray *
CLogicalSetOp::PdrgpcnstrColumn
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	ULONG ulColIndex,
	ULONG ulStart
	)
	const
{
	ConstraintArray *pdrgpcnstr = GPOS_NEW(memory_pool) ConstraintArray(memory_pool);

	CColRef *colref = (*m_pdrgpcrOutput)[ulColIndex];
	if (!CUtils::FConstrainableType(colref->RetrieveType()->MDId()))
	{
		return pdrgpcnstr;
	}

	const ULONG ulChildren = exprhdl.Arity();
	for (ULONG ul = ulStart; ul < ulChildren; ul++)
	{
		CConstraint *pcnstr = PcnstrColumn(memory_pool, exprhdl, ulColIndex, ul);
		if (NULL == pcnstr)
		{
			pcnstr = CConstraintInterval::PciUnbounded(memory_pool, colref, true /*is_null*/);
		}
		GPOS_ASSERT (NULL != pcnstr);
		pdrgpcnstr->Append(pcnstr);
	}

	return pdrgpcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcnstrColumn
//
//	@doc:
//		Get constraint for a given output column from a given children
//
//---------------------------------------------------------------------------
CConstraint *
CLogicalSetOp::PcnstrColumn
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	ULONG ulColIndex,
	ULONG ulChild
	)
	const
{
	GPOS_ASSERT(ulChild < exprhdl.Arity());

	// constraint from child
	CConstraint *pcnstrChild = exprhdl.GetRelationalProperties(ulChild)->Ppc()->Pcnstr();
	if (NULL == pcnstrChild)
	{
		return NULL;
	}

	// part of constraint on the current input column
	CConstraint *pcnstrCol = pcnstrChild->Pcnstr(memory_pool, (*(*m_pdrgpdrgpcrInput)[ulChild])[ulColIndex]);
	if (NULL == pcnstrCol)
	{
		return NULL;
	}

	// make a copy of this constraint but for the output column instead
	CConstraint *pcnstrOutput = pcnstrCol->PcnstrRemapForColumn(memory_pool, (*m_pdrgpcrOutput)[ulColIndex]);
	pcnstrCol->Release();
	return pcnstrOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PpcDeriveConstraintIntersectUnion
//
//	@doc:
//		Derive constraint property for intersect and union operators
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalSetOp::PpcDeriveConstraintIntersectUnion
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	BOOL fIntersect
	)
	const
{
	const ULONG num_cols = m_pdrgpcrOutput->Size();

	ConstraintArray *pdrgpcnstr = GPOS_NEW(memory_pool) ConstraintArray(memory_pool);
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		// get constraints for this column from all children
		ConstraintArray *pdrgpcnstrCol = PdrgpcnstrColumn(memory_pool, exprhdl, ul, 0 /*ulStart*/);

		CConstraint *pcnstrCol = NULL;
		if (fIntersect)
		{
			pcnstrCol = CConstraint::PcnstrConjunction(memory_pool, pdrgpcnstrCol);
		}
		else
		{
			pcnstrCol = CConstraint::PcnstrDisjunction(memory_pool, pdrgpcnstrCol);
		}

		if (NULL != pcnstrCol)
		{
			pdrgpcnstr->Append(pcnstrCol);
		}
	}

	CConstraint *pcnstrAll = CConstraint::PcnstrConjunction(memory_pool, pdrgpcnstr);

	ColRefSetArray *pdrgpcrs = PdrgpcrsOutputEquivClasses(memory_pool, exprhdl, fIntersect);

	return GPOS_NEW(memory_pool) CPropConstraint(memory_pool, pdrgpcrs, pcnstrAll);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcrsStat
//
//	@doc:
//		Compute required stats columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSetOp::PcrsStat
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle &, //exprhdl,
	CColRefSet *, //pcrsInput
	ULONG child_index
	)
	const
{
	CColRefSet *pcrs = (*m_pdrgpcrsInput)[child_index];
	pcrs->AddRef();

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalSetOp::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " Output: (";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << ")";
	
	os << ", Input: [";
	const ULONG ulChildren = m_pdrgpdrgpcrInput->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		os << "(";
		CUtils::OsPrintDrgPcr(os, (*m_pdrgpdrgpcrInput)[ul]);
		os << ")";

		if (ul < ulChildren - 1)
		{
			os << ", ";
		}
	}
	os << "]";

	return os;
}

// EOF
