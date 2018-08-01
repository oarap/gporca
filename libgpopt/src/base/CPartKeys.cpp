//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartKeys.cpp
//
//	@doc:
//		Implementation of partitioning keys
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CPartKeys.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::CPartKeys
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartKeys::CPartKeys
	(
	ColRefArrays *pdrgpdrgpcr
	)
	:
	m_pdrgpdrgpcr(pdrgpdrgpcr)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	m_num_of_part_levels = pdrgpdrgpcr->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::~CPartKeys
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartKeys::~CPartKeys()
{
	m_pdrgpdrgpcr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PcrKey
//
//	@doc:
//		Return key at a given level
//
//---------------------------------------------------------------------------
CColRef *
CPartKeys::PcrKey
	(
	ULONG ulLevel
	)
	const
{
	GPOS_ASSERT(ulLevel < m_num_of_part_levels);
	ColRefArray *colref_array = (*m_pdrgpdrgpcr)[ulLevel];
	return (*colref_array)[0];
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::FOverlap
//
//	@doc:
//		Check whether the key columns overlap the given column
//
//---------------------------------------------------------------------------
BOOL
CPartKeys::FOverlap
	(
	CColRefSet *pcrs
	)
	const
{
	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		CColRef *colref = PcrKey(ul);
		if (pcrs->FMember(colref))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PpartkeysCopy
//
//	@doc:
//		Copy part key into the given memory pool
//
//---------------------------------------------------------------------------
CPartKeys *
CPartKeys::PpartkeysCopy
	(
	IMemoryPool *mp
	)
{
	ColRefArrays *pdrgpdrgpcrCopy = GPOS_NEW(mp) ColRefArrays(mp);

	const ULONG length = m_pdrgpdrgpcr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ColRefArray *colref_array = (*m_pdrgpdrgpcr)[ul];
		ColRefArray *pdrgpcrCopy = GPOS_NEW(mp) ColRefArray(mp);
		const ULONG num_cols = colref_array->Size();
		for (ULONG ulCol = 0; ulCol < num_cols; ulCol++)
		{
			pdrgpcrCopy->Append((*colref_array)[ulCol]);
		}
		pdrgpdrgpcrCopy->Append(pdrgpcrCopy);
	}

	return GPOS_NEW(mp) CPartKeys(pdrgpdrgpcrCopy);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PdrgppartkeysCopy
//
//	@doc:
//		Copy array of part keys into given memory pool
//
//---------------------------------------------------------------------------
PartKeysArray *
CPartKeys::PdrgppartkeysCopy
	(
	IMemoryPool *mp,
	const PartKeysArray *pdrgppartkeys
	)
{
	GPOS_ASSERT(NULL != pdrgppartkeys);

	PartKeysArray *pdrgppartkeysCopy = GPOS_NEW(mp) PartKeysArray(mp);
	const ULONG length = pdrgppartkeys->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		pdrgppartkeysCopy->Append((*pdrgppartkeys)[ul]->PpartkeysCopy(mp));
	}
	return pdrgppartkeysCopy;
}


//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::PpartkeysRemap
//
//	@doc:
//		Create a new PartKeys object from the current one by remapping the
//		keys using the given hashmap
//
//---------------------------------------------------------------------------
CPartKeys *
CPartKeys::PpartkeysRemap
	(
	IMemoryPool *mp,
	UlongToColRefMap *colref_mapping
	)
	const
{
	GPOS_ASSERT(NULL != colref_mapping);
	ColRefArrays *pdrgpdrgpcr = GPOS_NEW(mp) ColRefArrays(mp);

	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		CColRef *colref = CUtils::PcrRemap(PcrKey(ul), colref_mapping, false /*must_exist*/);

		ColRefArray *colref_array = GPOS_NEW(mp) ColRefArray(mp);
		colref_array->Append(colref);

		pdrgpdrgpcr->Append(colref_array);
	}

	return GPOS_NEW(mp) CPartKeys(pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartKeys::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPartKeys::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "(";
	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		CColRef *colref = PcrKey(ul);
		os << *colref;

		// separator
		os << (ul == m_num_of_part_levels - 1 ? "" : ", ");
	}

	os << ")";

	return os;
}

#ifdef GPOS_DEBUG
void
CPartKeys::DbgPrint() const
{

	IMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(mp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG
// EOF
