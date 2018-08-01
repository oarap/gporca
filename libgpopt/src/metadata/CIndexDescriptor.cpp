//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CIndexDescriptor.cpp
//
//	@doc:
//		Implementation of index description
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CIndexDescriptor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::CIndexDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CIndexDescriptor::CIndexDescriptor
	(
	IMemoryPool *memory_pool,
	IMDId *pmdidIndex,
	const CName &name,
	DrgPcoldesc *pdrgcoldescKeyCols,
	DrgPcoldesc *pdrgcoldescIncludedCols,
	BOOL is_clustered
	)
	:
	m_pmdidIndex(pmdidIndex),
	m_name(memory_pool, name),
	m_pdrgpcoldescKeyCols(pdrgcoldescKeyCols),
	m_pdrgpcoldescIncludedCols(pdrgcoldescIncludedCols),
	m_clustered(is_clustered)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(pmdidIndex->IsValid());
	GPOS_ASSERT(NULL != pdrgcoldescKeyCols);
	GPOS_ASSERT(NULL != pdrgcoldescIncludedCols);
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::~CIndexDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CIndexDescriptor::~CIndexDescriptor()
{
	m_pmdidIndex->Release();

	m_pdrgpcoldescKeyCols->Release();
	m_pdrgpcoldescIncludedCols->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Keys
//
//	@doc:
//		number of key columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::Keys() const
{
	return m_pdrgpcoldescKeyCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::UlIncludedColumns
//
//	@doc:
//		Number of included columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::UlIncludedColumns() const
{
	// array allocated in ctor
	GPOS_ASSERT(NULL != m_pdrgpcoldescIncludedCols);

	return m_pdrgpcoldescIncludedCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Pindexdesc
//
//	@doc:
//		Create the index descriptor from the table descriptor and index
//		information from the catalog
//
//---------------------------------------------------------------------------
CIndexDescriptor *
CIndexDescriptor::Pindexdesc
	(
	IMemoryPool *memory_pool,
	const CTableDescriptor *ptabdesc,
	const IMDIndex *pmdindex
	)
{
	CWStringConst strIndexName(memory_pool, pmdindex->Mdname().GetMDName()->GetBuffer());

	DrgPcoldesc *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();

	pmdindex->MDId()->AddRef();

	// array of index column descriptors
	DrgPcoldesc *pdrgcoldescKey = GPOS_NEW(memory_pool) DrgPcoldesc(memory_pool);

	for (ULONG ul = 0; ul < pmdindex->Keys(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescKey->Append(pcoldesc);
	}

	// array of included column descriptors
	DrgPcoldesc *pdrgcoldescIncluded = GPOS_NEW(memory_pool) DrgPcoldesc(memory_pool);
	for (ULONG ul = 0; ul < pmdindex->IncludedCols(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescIncluded->Append(pcoldesc);
	}


	// create the index descriptors
	CIndexDescriptor *pindexdesc = GPOS_NEW(memory_pool) CIndexDescriptor
											(
											memory_pool,
											pmdindex->MDId(),
											CName(&strIndexName),
											pdrgcoldescKey,
											pdrgcoldescIncluded,
											pmdindex->IsClustered()
											);
	return pindexdesc;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CIndexDescriptor::OsPrint
	(
	IOstream &os
	)
	const
{
	m_name.OsPrint(os);
	os << ": (Keys :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescKeyCols, m_pdrgpcoldescKeyCols->Size());
	os << "); ";

	os << "(Included Columns :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescIncludedCols, m_pdrgpcoldescIncludedCols->Size());
	os << ")";

	os << " [ Clustered :";
	if (m_clustered)
	{
		os << "true";
	}
	else
	{
		os << "false";
	}
	os << " ]";
	return os;
}

#endif // GPOS_DEBUG

// EOF

