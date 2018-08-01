//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDTypeInt2GPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific int2 type in the
//		MD cache
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CGPDBTypeHelper.h"

#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/operators/CDXLDatumInt2.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/base/CDatumInt2GPDB.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

// static member initialization
CWStringConst
CMDTypeInt2GPDB::m_str = CWStringConst(GPOS_WSZ_LIT("int2"));
CMDName
CMDTypeInt2GPDB::m_mdname(&m_str);

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::CMDTypeInt2GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTypeInt2GPDB::CMDTypeInt2GPDB
	(
	IMemoryPool *memory_pool
	)
	:
	m_memory_pool(memory_pool)
{
	m_mdid = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_OID);
	m_mdid_op_eq = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_EQ_OP);
	m_mdid_op_neq = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_NEQ_OP);
	m_mdid_op_lt = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_LT_OP);
	m_mdid_op_leq = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_LEQ_OP);
	m_mdid_op_gt = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_GT_OP);
	m_mdid_op_geq = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_GEQ_OP);
	m_mdid_op_cmp = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_COMP_OP);
	m_mdid_type_array = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_ARRAY_TYPE);
	m_mdid_min = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_AGG_MIN);
	m_mdid_max = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_AGG_MAX);
	m_mdid_avg = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_AGG_AVG);
	m_mdid_sum = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_AGG_SUM);
	m_mdid_count = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_INT2_AGG_COUNT);

	m_dxl_str = CDXLUtils::SerializeMDObj(m_memory_pool, this, false /*fSerializeHeader*/, false /*indentation*/);

	GPOS_ASSERT(GPDB_INT2_OID == CMDIdGPDB::CastMdid(m_mdid)->OidObjectId());
	m_mdid->AddRef();
	m_datum_null = GPOS_NEW(memory_pool) CDatumInt2GPDB(m_mdid, 1 /* m_bytearray_value */, true /* is_null */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::~CMDTypeInt2GPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTypeInt2GPDB::~CMDTypeInt2GPDB()
{
	m_mdid->Release();
	m_mdid_op_eq->Release();
	m_mdid_op_neq->Release();
	m_mdid_op_lt->Release();
	m_mdid_op_leq->Release();
	m_mdid_op_gt->Release();
	m_mdid_op_geq->Release();
	m_mdid_op_cmp->Release();
	m_mdid_type_array->Release();
	m_mdid_min->Release();
	m_mdid_max->Release();
	m_mdid_avg->Release();
	m_mdid_sum->Release();
	m_mdid_count->Release();
	m_datum_null->Release();

	GPOS_DELETE(m_dxl_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetDatum
//
//	@doc:
//		Factory function for creating INT2 datums
//
//---------------------------------------------------------------------------
IDatumInt2 *
CMDTypeInt2GPDB::CreateInt2Datum
	(
	IMemoryPool *memory_pool, 
	SINT value,
	BOOL is_null
	)
	const
{
	return GPOS_NEW(memory_pool) CDatumInt2GPDB(m_mdid->Sysid(), value, is_null);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::MDId
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt2GPDB::MDId() const
{
	return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::Mdname
//
//	@doc:
//		Returns the name of this type
//
//---------------------------------------------------------------------------
CMDName
CMDTypeInt2GPDB::Mdname() const
{
	return CMDTypeInt2GPDB::m_mdname;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetMdidForCmpType
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt2GPDB::GetMdidForCmpType
	(
	ECmpType cmp_type
	) 
	const
{
	switch (cmp_type)
	{
		case EcmptEq:
			return m_mdid_op_eq;
		case EcmptNEq:
			return m_mdid_op_neq;
		case EcmptL:
			return m_mdid_op_lt;
		case EcmptLEq: 
			return m_mdid_op_leq;
		case EcmptG:
			return m_mdid_op_gt;
		case EcmptGEq:
			return m_mdid_op_geq;
		default:
			GPOS_ASSERT(!"Invalid operator type");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetMdidForAggType
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt2GPDB::GetMdidForAggType
	(
	EAggType agg_type
	) 
	const
{
	switch (agg_type)
	{
		case EaggMin:
			return m_mdid_min;
		case EaggMax:
			return m_mdid_max;
		case EaggAvg:
			return m_mdid_avg;
		case EaggSum:
			return m_mdid_sum;
		case EaggCount:
			return m_mdid_count;
		default:
			GPOS_ASSERT(!"Invalid aggregate type");
			return NULL;
	}
}
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTypeInt2GPDB::Serialize
	(
	CXMLSerializer *xml_serializer
	) 
	const
{
	CGPDBTypeHelper<CMDTypeInt2GPDB>::Serialize(xml_serializer, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetDatumForDXLConstVal
//
//	@doc:
//		Transformation method for generating int2 datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt2GPDB::GetDatumForDXLConstVal
	(
	const CDXLScalarConstValue *dxl_op
	)
	const
{
	CDXLDatumInt2 *datum_dxl = CDXLDatumInt2::Cast(const_cast<CDXLDatum*>(dxl_op->GetDatumVal()));
	GPOS_ASSERT(datum_dxl->IsPassedByValue());

	return GPOS_NEW(m_memory_pool) CDatumInt2GPDB(m_mdid->Sysid(), datum_dxl->Value(), datum_dxl->IsNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetDatumForDXLDatum
//
//	@doc:
//		Construct an int2 datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt2GPDB::GetDatumForDXLDatum
	(
	IMemoryPool *memory_pool,
	const CDXLDatum *datum_dxl
	)
	const
{
	CDXLDatumInt2 *int2_dxl_datum = CDXLDatumInt2::Cast(const_cast<CDXLDatum *>(datum_dxl));
	GPOS_ASSERT(int2_dxl_datum->IsPassedByValue());
	SINT val = int2_dxl_datum->Value();
	BOOL is_null = int2_dxl_datum->IsNull();

	return GPOS_NEW(memory_pool) CDatumInt2GPDB(m_mdid->Sysid(), val, is_null);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetDatumVal
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt2GPDB::GetDatumVal
	(
	IMemoryPool *memory_pool,
	IDatum *datum
	)
	const
{
	m_mdid->AddRef();
	CDatumInt2GPDB *int2_datum = dynamic_cast<CDatumInt2GPDB*>(datum);

	return GPOS_NEW(memory_pool) CDXLDatumInt2(memory_pool, m_mdid, int2_datum->IsNull(), int2_datum->Value());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetDXLOpScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeInt2GPDB::GetDXLOpScConst
	(
	IMemoryPool *memory_pool,
	IDatum *datum
	)
	const
{
	CDatumInt2GPDB *int2gpdb_datum = dynamic_cast<CDatumInt2GPDB *>(datum);

	m_mdid->AddRef();
	CDXLDatumInt2 *datum_dxl = GPOS_NEW(memory_pool) CDXLDatumInt2(memory_pool, m_mdid, int2gpdb_datum->IsNull(), int2gpdb_datum->Value());

	return GPOS_NEW(memory_pool) CDXLScalarConstValue(memory_pool, datum_dxl);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::GetDXLDatumNull
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt2GPDB::GetDXLDatumNull
	(
	IMemoryPool *memory_pool
	)
	const
{
	m_mdid->AddRef();

	return GPOS_NEW(memory_pool) CDXLDatumInt2(memory_pool, m_mdid, true /*is_null*/, 1 /* a dummy m_bytearray_value */);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTypeInt2GPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	CGPDBTypeHelper<CMDTypeInt2GPDB>::DebugPrint(os,this);
}

#endif // GPOS_DEBUG

// EOF

