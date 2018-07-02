//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008, 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnFactory.cpp
//
//	@doc:
//		Implementation of column reference management
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/common/CAutoP.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CColRefComputed.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarProjectElement.h"

#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

#define GPOPT_COLFACTORY_HT_BUCKETS	10000

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::CColumnFactory
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColumnFactory::CColumnFactory()
	:
	m_memory_pool(NULL),
	m_phmcrcrs(NULL)
{
	CAutoMemoryPool amp;
	m_memory_pool = amp.Pmp();
	
	// initialize hash table
	m_sht.Init
		(
		m_memory_pool,
		GPOPT_COLFACTORY_HT_BUCKETS,
		GPOS_OFFSET(CColRef, m_link),
		GPOS_OFFSET(CColRef, m_id),
		&(CColRef::m_ulInvalid),
		CColRef::HashValue,
		CColRef::Equals
		);

	// now it's safe to detach the auto pool
	(void) amp.Detach();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::~CColumnFactory
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CColumnFactory::~CColumnFactory()
{
	CRefCount::SafeRelease(m_phmcrcrs);

	// dealloc hash table
	m_sht.Cleanup();

	// destroy mem pool
	CMemoryPoolManager *pmpm = CMemoryPoolManager::GetMemoryPoolMgr();
	pmpm->Destroy(m_memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::Initialize
//
//	@doc:
//		Initialize the hash map between computed column and used columns
//
//---------------------------------------------------------------------------
void
CColumnFactory::Initialize()
{
	m_phmcrcrs = GPOS_NEW(m_memory_pool) HMCrCrs(m_memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant without name for computed columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT type_modifier
	)
{
	// increment atomic counter
	ULONG id = m_aul.Incr();
	
	WCHAR wszFmt[] = GPOS_WSZ_LIT("ColRef_%04d");
	CWStringDynamic *pstrTempName = GPOS_NEW(m_memory_pool) CWStringDynamic(m_memory_pool);
	CAutoP<CWStringDynamic> a_pstrTempName(pstrTempName);
	pstrTempName->AppendFormat(wszFmt, id);
	CWStringConst strName(pstrTempName->GetBuffer());
	return PcrCreate(pmdtype, type_modifier, id, CName(&strName));
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant without name for computed columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT type_modifier,
	const CName &name
	)
{
	ULONG id = m_aul.Incr();

	return PcrCreate(pmdtype, type_modifier, id, name);
}
	
	
//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT type_modifier,
	ULONG id,
	const CName &name
	)
{
	CName *pnameCopy = GPOS_NEW(m_memory_pool) CName(m_memory_pool, name); 
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *colref = GPOS_NEW(m_memory_pool) CColRefComputed(pmdtype, type_modifier, id, pnameCopy);
	(void) a_pnameCopy.Reset();
	CAutoP<CColRef> a_pcr(colref);
	
	// ensure uniqueness
	GPOS_ASSERT(NULL == LookupColRef(id));
	m_sht.Insert(colref);
	
	return a_pcr.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const CColumnDescriptor *pcoldesc,
	ULONG id,
	const CName &name,
	ULONG ulOpSource
	)
{
	CName *pnameCopy = GPOS_NEW(m_memory_pool) CName(m_memory_pool, name);
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *colref = GPOS_NEW(m_memory_pool) CColRefTable(pcoldesc, id, pnameCopy, ulOpSource);
	(void) a_pnameCopy.Reset();
	CAutoP<CColRef> a_pcr(colref);

	// ensure uniqueness
	GPOS_ASSERT(NULL == LookupColRef(id));
	m_sht.Insert(colref);
	
	return a_pcr.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const IMDType *pmdtype,
	INT type_modifier,
	INT attno,
	BOOL is_nullable,
	ULONG id,
	const CName &name,
	ULONG ulOpSource,
	ULONG ulWidth
	)
{
	CName *pnameCopy = GPOS_NEW(m_memory_pool) CName(m_memory_pool, name);
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *colref =
			GPOS_NEW(m_memory_pool) CColRefTable(pmdtype, type_modifier, attno, is_nullable, id, pnameCopy, ulOpSource, ulWidth);
	(void) a_pnameCopy.Reset();
	CAutoP<CColRef> a_pcr(colref);

	// ensure uniqueness
	GPOS_ASSERT(NULL == LookupColRef(id));
	m_sht.Insert(colref);

	return a_pcr.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant with alias/name for base table columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate
	(
	const CColumnDescriptor *pcoldesc,
	const CName &name,
	ULONG ulOpSource
	)
{
	ULONG id = m_aul.Incr();
	
	return PcrCreate(pcoldesc, id, name, ulOpSource);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCopy
//
//	@doc:
//		Create a copy of the given colref
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCopy
	(
	const CColRef* colref
	)
{
	CName name(colref->Name());
	if (CColRef::EcrtComputed == colref->Ecrt())
	{
		return PcrCreate(colref->RetrieveType(), colref->TypeModifier(), name);
	}

	GPOS_ASSERT(CColRef::EcrtTable == colref->Ecrt());
	ULONG id = m_aul.Incr();
	CColRefTable *pcrTable = CColRefTable::PcrConvert(const_cast<CColRef*>(colref));

	return PcrCreate
			(
			colref->RetrieveType(),
			colref->TypeModifier(),
			pcrTable->AttrNum(),
			pcrTable->IsNullable(),
			id,
			name,
			pcrTable->UlSourceOpId(),
			pcrTable->Width()
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::LookupColRef
//
//	@doc:
//		Lookup by id
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::LookupColRef
	(
	ULONG id
	)
{
	CSyncHashtableAccessByKey<CColRef, ULONG,
		CSpinlockColumnFactory> shtacc(m_sht, id);
	
	CColRef *colref = shtacc.Find();
	
	return colref;
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::Destroy
//
//	@doc:
//		unlink and destruct
//
//---------------------------------------------------------------------------
void
CColumnFactory::Destroy
	(
	CColRef *colref
	)
{
	GPOS_ASSERT(NULL != colref);

	ULONG id = colref->m_id;
	
	{
		// scope for the hash table accessor
		CSyncHashtableAccessByKey<CColRef, ULONG, CSpinlockColumnFactory>
			shtacc(m_sht, id);
		
		CColRef *pcrFound = shtacc.Find();
		GPOS_ASSERT(colref == pcrFound);
		
		// unlink from hashtable
		shtacc.Remove(pcrFound);
	}
	
	GPOS_DELETE(colref);
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrsUsedInComputedCol
//
//	@doc:
//		Lookup the set of used column references (if any) based on id of
//		computed column
//---------------------------------------------------------------------------
const CColRefSet *
CColumnFactory::PcrsUsedInComputedCol
	(
	const CColRef *colref
	)
{
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(NULL != m_phmcrcrs);

	// get its column reference set from the hash map
	const CColRefSet *pcrs = m_phmcrcrs->Find(colref);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::AddComputedToUsedColsMap
//
//	@doc:
//		Add the map between computed column and its used columns
//
//---------------------------------------------------------------------------
void
CColumnFactory::AddComputedToUsedColsMap
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != m_phmcrcrs);

	const CScalarProjectElement *popScPrEl = CScalarProjectElement::PopConvert(pexpr->Pop());
	CColRef *pcrComputedCol = popScPrEl->Pcr();

	CDrvdPropScalar *pdpscalar = CDrvdPropScalar::GetDrvdScalarProps(pexpr->PdpDerive());
	CColRefSet *pcrsUsed = pdpscalar->PcrsUsed();
	if (NULL != pcrsUsed && 0 < pcrsUsed->Size())
	{
#ifdef GPOS_DEBUG
		BOOL fres =
#endif // GPOS_DEBUG
			m_phmcrcrs->Insert(pcrComputedCol, GPOS_NEW(m_memory_pool) CColRefSet(m_memory_pool, *pcrsUsed));
		GPOS_ASSERT(fres);
	}
}


// EOF

