//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPropConstraint.cpp
//
//	@doc:
//		Implementation of constraint property
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CPropConstraint.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::CPropConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPropConstraint::CPropConstraint
	(
	IMemoryPool *memory_pool,
	DrgPcrs *pdrgpcrs,
	CConstraint *pcnstr
	)
	:
	m_pdrgpcrs(pdrgpcrs),
	m_phmcrcrs(NULL),
	m_pcnstr(pcnstr)
{
	GPOS_ASSERT(NULL != pdrgpcrs);
	InitHashMap(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::~CPropConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPropConstraint::~CPropConstraint()
{
	m_pdrgpcrs->Release();
	m_phmcrcrs->Release();
	CRefCount::SafeRelease(m_pcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::InitHashMap
//
//	@doc:
//		Initialize mapping between columns and equivalence classes
//
//---------------------------------------------------------------------------
void
CPropConstraint::InitHashMap
	(
	IMemoryPool *memory_pool
	)
{
	GPOS_ASSERT(NULL == m_phmcrcrs);
	m_phmcrcrs = GPOS_NEW(memory_pool) HMCrCrs(memory_pool);
	const ULONG ulEquiv = m_pdrgpcrs->Size();
	for (ULONG ul = 0; ul < ulEquiv; ul++)
	{
		CColRefSet *pcrs = (*m_pdrgpcrs)[ul];

		CColRefSetIter crsi(*pcrs);
		while (crsi.Advance())
		{
			pcrs->AddRef();
#ifdef GPOS_DEBUG
			BOOL fres =
#endif //GPOS_DEBUG
			m_phmcrcrs->Insert(crsi.Pcr(), pcrs);
			GPOS_ASSERT(fres);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::FContradiction
//
//	@doc:
//		Is this a contradiction
//
//---------------------------------------------------------------------------
BOOL
CPropConstraint::FContradiction()
const
{
	return (NULL != m_pcnstr && m_pcnstr->FContradiction());
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::PexprScalarMappedFromEquivCols
//
//	@doc:
//		Return scalar expression on the given column mapped from all constraints
//		on its equivalent columns
//
//---------------------------------------------------------------------------
CExpression *
CPropConstraint::PexprScalarMappedFromEquivCols
	(
	IMemoryPool *memory_pool,
	CColRef *colref
	)
	const
{
	if(NULL == m_pcnstr)
	{
		return NULL;
	}

	CColRefSet *pcrs = m_phmcrcrs->Find(colref);
	if (NULL == pcrs || 1 == pcrs->Size())
	{
		return NULL;
	}

	// get constraints for all other columns in this equivalence class
	// except the current column
	CColRefSet *pcrsEquiv = GPOS_NEW(memory_pool) CColRefSet(memory_pool);
	pcrsEquiv->Include(pcrs);
	pcrsEquiv->Exclude(colref);

	CConstraint *pcnstr = m_pcnstr->Pcnstr(memory_pool, pcrsEquiv);
	pcrsEquiv->Release();
	if (NULL == pcnstr)
	{
		return NULL;
	}

	// generate a copy of all these constraints for the current column
	CConstraint *pcnstrCol = pcnstr->PcnstrRemapForColumn(memory_pool, colref);
	CExpression *pexprScalar = pcnstrCol->PexprScalar(memory_pool);
	pexprScalar->AddRef();

	pcnstr->Release();
	pcnstrCol->Release();

	return pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPropConstraint::OsPrint
	(
	IOstream &os
	)
	const
{
	const ULONG length = m_pdrgpcrs->Size();
	if (0 < length)
	{
		os << "Equivalence Classes: { ";

		for (ULONG ul = 0; ul < length; ul++)
		{
			CColRefSet *pcrs = (*m_pdrgpcrs)[ul];
			os << "(" << *pcrs << ") ";
		}

		os << "} ";
	}

	if (NULL != m_pcnstr)
	{
		os << "Constraint:" << *m_pcnstr;
	}

	return os;
}

void
CPropConstraint::DbgPrint() const
{
	IMemoryPool *memory_pool = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(memory_pool);
	(void) this->OsPrint(at.Os());
}
// EOF

