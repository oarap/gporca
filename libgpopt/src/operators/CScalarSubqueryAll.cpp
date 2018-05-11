//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarSubqueryAll.cpp
//
//	@doc:
//		Implementation of scalar subquery ALL operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CScalarSubqueryAll.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryAll::CScalarSubqueryAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSubqueryAll::CScalarSubqueryAll
	(
	IMemoryPool *memory_pool,
	IMDId *scalar_op_mdid,
	const CWStringConst *pstrScalarOp,
	const CColRef *colref
	)
	:
	CScalarSubqueryQuantified(memory_pool, scalar_op_mdid, pstrScalarOp, colref)
{}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryAll::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CScalarSubqueryAll::PopCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	UlongColRefHashMap *colref_mapping,
	BOOL must_exist
	)
{
	CColRef *colref = CUtils::PcrRemap(Pcr(), colref_mapping, must_exist);

	IMDId *scalar_op_mdid = MdIdOp();
	scalar_op_mdid->AddRef();

	CWStringConst *pstrScalarOp = GPOS_NEW(memory_pool) CWStringConst(memory_pool, PstrOp()->GetBuffer());

	return GPOS_NEW(memory_pool) CScalarSubqueryAll(memory_pool, scalar_op_mdid, pstrScalarOp, colref);
}

// EOF
