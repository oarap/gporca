//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLOperatorFactory.cpp
//
//	@doc:
//		Implementation of the factory methods for creation of DXL elements.
//---------------------------------------------------------------------------

#include "gpos/string/CWStringConst.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/common/clibwrapper.h"

#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDIdGPDBCtas.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdScCmp.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"
#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"
#include "naucrates/dxl/operators/CDXLDatumStatsDoubleMappable.h"
#include "naucrates/dxl/operators/CDXLDatumInt2.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/operators/CDXLDatumBool.h"
#include "naucrates/dxl/operators/CDXLDatumOid.h"

#include <xercesc/util/NumberFormatException.hpp>

using namespace gpos;
using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

#define GPDXL_GPDB_MDID_COMPONENTS 3
#define GPDXL_DEFAULT_USERID 0

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLTblScan
//
//	@doc:
//		Construct a table scan operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLTblScan(CDXLMemoryManager *memory_manager_dxl,
									const Attributes &  // attrs
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLPhysicalTableScan(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLSubqScan
//
//	@doc:
//		Construct a subquery scan operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLSubqScan(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse subquery name from attributes
	const XMLCh *subquery_name_xml =
		ExtractAttrValue(attrs, EdxltokenAlias, EdxltokenPhysicalSubqueryScan);

	CWStringDynamic *subquery_name_str =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, subquery_name_xml);


	// create a copy of the string in the CMDName constructor
	CMDName *subquery_name = GPOS_NEW(memory_pool) CMDName(memory_pool, subquery_name_str);

	GPOS_DELETE(subquery_name_str);

	return GPOS_NEW(memory_pool) CDXLPhysicalSubqueryScan(memory_pool, subquery_name);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLResult
//
//	@doc:
//		Construct a result operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLResult(CDXLMemoryManager *memory_manager_dxl)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLPhysicalResult(memory_pool);
}

//		Construct a hashjoin operator
CDXLPhysical *
CDXLOperatorFactory::MakeDXLHashJoin(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	const XMLCh *join_type_xml =
		ExtractAttrValue(attrs, EdxltokenJoinType, EdxltokenPhysicalHashJoin);

	EdxlJoinType join_type =
		ParseJoinType(join_type_xml, CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalHashJoin));

	return GPOS_NEW(memory_pool) CDXLPhysicalHashJoin(memory_pool, join_type);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLNLJoin
//
//	@doc:
//		Construct a nested loop join operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLNLJoin(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	const XMLCh *join_type_xml =
		ExtractAttrValue(attrs, EdxltokenJoinType, EdxltokenPhysicalNLJoin);

	BOOL is_index_nlj = false;
	const XMLCh *index_nlj_xml =
		attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenPhysicalNLJoinIndex));
	if (NULL != index_nlj_xml)
	{
		is_index_nlj = ConvertAttrValueToBool(memory_manager_dxl,
											  index_nlj_xml,
											  EdxltokenPhysicalNLJoinIndex,
											  EdxltokenPhysicalNLJoin);
	}

	EdxlJoinType join_type =
		ParseJoinType(join_type_xml, CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalNLJoin));

	return GPOS_NEW(memory_pool) CDXLPhysicalNLJoin(memory_pool, join_type, is_index_nlj);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLMergeJoin
//
//	@doc:
//		Construct a merge join operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLMergeJoin(CDXLMemoryManager *memory_manager_dxl,
									  const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	const XMLCh *join_type_xml =
		ExtractAttrValue(attrs, EdxltokenJoinType, EdxltokenPhysicalMergeJoin);

	EdxlJoinType join_type =
		ParseJoinType(join_type_xml, CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalMergeJoin));

	BOOL is_unique_outer = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenMergeJoinUniqueOuter, EdxltokenPhysicalMergeJoin);

	return GPOS_NEW(memory_pool) CDXLPhysicalMergeJoin(memory_pool, join_type, is_unique_outer);
}

//		Construct a gather motion operator
CDXLPhysical *
CDXLOperatorFactory::MakeDXLGatherMotion(CDXLMemoryManager *memory_manager_dxl,
										 const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	CDXLPhysicalGatherMotion *dxl_op = GPOS_NEW(memory_pool) CDXLPhysicalGatherMotion(memory_pool);
	SetSegmentInfo(memory_manager_dxl, dxl_op, attrs, EdxltokenPhysicalGatherMotion);

	return dxl_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLBroadcastMotion
//
//	@doc:
//		Construct a broadcast motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLBroadcastMotion(CDXLMemoryManager *memory_manager_dxl,
											const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	CDXLPhysicalBroadcastMotion *dxl_op =
		GPOS_NEW(memory_pool) CDXLPhysicalBroadcastMotion(memory_pool);
	SetSegmentInfo(memory_manager_dxl, dxl_op, attrs, EdxltokenPhysicalBroadcastMotion);

	return dxl_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLRedistributeMotion
//
//	@doc:
//		Construct a redistribute motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLRedistributeMotion(CDXLMemoryManager *memory_manager_dxl,
											   const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	BOOL is_duplicate_sensitive = false;

	const XMLCh *duplicate_sensitive_xml =
		attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenDuplicateSensitive));
	if (NULL != duplicate_sensitive_xml)
	{
		is_duplicate_sensitive = ConvertAttrValueToBool(memory_manager_dxl,
														duplicate_sensitive_xml,
														EdxltokenDuplicateSensitive,
														EdxltokenPhysicalRedistributeMotion);
	}

	CDXLPhysicalRedistributeMotion *dxl_op =
		GPOS_NEW(memory_pool) CDXLPhysicalRedistributeMotion(memory_pool, is_duplicate_sensitive);
	SetSegmentInfo(memory_manager_dxl, dxl_op, attrs, EdxltokenPhysicalRedistributeMotion);

	return dxl_op;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLRoutedMotion
//
//	@doc:
//		Construct a routed motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLRoutedMotion(CDXLMemoryManager *memory_manager_dxl,
										 const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	ULONG segment_col_id = CDXLOperatorFactory::ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenSegmentIdCol, EdxltokenPhysicalRoutedDistributeMotion);

	CDXLPhysicalRoutedDistributeMotion *dxl_op =
		GPOS_NEW(memory_pool) CDXLPhysicalRoutedDistributeMotion(memory_pool, segment_col_id);
	SetSegmentInfo(memory_manager_dxl, dxl_op, attrs, EdxltokenPhysicalRoutedDistributeMotion);

	return dxl_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLRandomMotion
//
//	@doc:
//		Construct a random motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLRandomMotion(CDXLMemoryManager *memory_manager_dxl,
										 const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	BOOL is_duplicate_sensitive = false;

	const XMLCh *duplicate_sensitive_xml =
		attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenDuplicateSensitive));
	if (NULL != duplicate_sensitive_xml)
	{
		is_duplicate_sensitive = ConvertAttrValueToBool(memory_manager_dxl,
														duplicate_sensitive_xml,
														EdxltokenDuplicateSensitive,
														EdxltokenPhysicalRandomMotion);
	}

	CDXLPhysicalRandomMotion *dxl_op =
		GPOS_NEW(memory_pool) CDXLPhysicalRandomMotion(memory_pool, is_duplicate_sensitive);
	SetSegmentInfo(memory_manager_dxl, dxl_op, attrs, EdxltokenPhysicalRandomMotion);

	return dxl_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLAppend
//	@doc:
//		Construct an Append operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLAppend(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	BOOL is_target = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenAppendIsTarget, EdxltokenPhysicalAppend);

	BOOL is_zapped = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenAppendIsZapped, EdxltokenPhysicalAppend);

	return GPOS_NEW(memory_pool) CDXLPhysicalAppend(memory_pool, is_target, is_zapped);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLLimit
//	@doc:
//		Construct a Limit operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLLimit(CDXLMemoryManager *memory_manager_dxl,
								  const Attributes &  // attrs
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLPhysicalLimit(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLLimitCount
//
//	@doc:
//		Construct a Limit Count operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLLimitCount(CDXLMemoryManager *memory_manager_dxl,
									   const Attributes &  // attrs
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLScalarLimitCount(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLLimitOffset
//
//	@doc:
//		Construct a Limit Offset operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLLimitOffset(CDXLMemoryManager *memory_manager_dxl,
										const Attributes &  // attrs
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLScalarLimitOffset(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLAgg
//
//	@doc:
//		Construct an aggregate operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLAgg(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	const XMLCh *agg_strategy_xml =
		ExtractAttrValue(attrs, EdxltokenAggStrategy, EdxltokenPhysicalAggregate);

	EdxlAggStrategy agg_strategy_dxl = EdxlaggstrategySentinel;

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggStrategyPlain),
									  agg_strategy_xml))
	{
		agg_strategy_dxl = EdxlaggstrategyPlain;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggStrategySorted),
										   agg_strategy_xml))
	{
		agg_strategy_dxl = EdxlaggstrategySorted;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggStrategyHashed),
										   agg_strategy_xml))
	{
		agg_strategy_dxl = EdxlaggstrategyHashed;
	}
	else
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenAggStrategy)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalAggregate)->GetBuffer());
	}

	BOOL stream_safe = false;

	const XMLCh *stream_safe_xml = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenAggStreamSafe));
	if (NULL != stream_safe_xml)
	{
		stream_safe = ConvertAttrValueToBool(memory_manager_dxl,
											 stream_safe_xml,
											 EdxltokenAggStreamSafe,
											 EdxltokenPhysicalAggregate);
	}

	return GPOS_NEW(memory_pool) CDXLPhysicalAgg(memory_pool, agg_strategy_dxl, stream_safe);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLSort
//
//	@doc:
//		Construct a sort operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLSort(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse discard duplicates and nulls first properties from the attributes
	BOOL discard_duplicates = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenSortDiscardDuplicates, EdxltokenPhysicalSort);

	return GPOS_NEW(memory_pool) CDXLPhysicalSort(memory_pool, discard_duplicates);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLMaterialize
//
//	@doc:
//		Construct a materialize operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::MakeDXLMaterialize(CDXLMemoryManager *memory_manager_dxl,
										const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse spooling info from the attributes

	CDXLPhysicalMaterialize *materialize_dxlnode = NULL;

	// is this a multi-slice spool
	BOOL eager_materialize = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenMaterializeEager, EdxltokenPhysicalMaterialize);

	if (1 == attrs.getLength())
	{
		// no spooling info specified -> create a non-spooling materialize operator
		materialize_dxlnode =
			GPOS_NEW(memory_pool) CDXLPhysicalMaterialize(memory_pool, eager_materialize);
	}
	else
	{
		// parse spool id
		ULONG spool_id = ExtractConvertAttrValueToUlong(
			memory_manager_dxl, attrs, EdxltokenSpoolId, EdxltokenPhysicalMaterialize);

		// parse id of executor slice
		INT executor_slice = ExtractConvertAttrValueToInt(
			memory_manager_dxl, attrs, EdxltokenExecutorSliceId, EdxltokenPhysicalMaterialize);

		ULONG num_consumer_slices = ExtractConvertAttrValueToUlong(
			memory_manager_dxl, attrs, EdxltokenConsumerSliceCount, EdxltokenPhysicalMaterialize);

		materialize_dxlnode = GPOS_NEW(memory_pool) CDXLPhysicalMaterialize(
			memory_pool, eager_materialize, spool_id, executor_slice, num_consumer_slices);
	}

	return materialize_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLScalarCmp
//
//	@doc:
//		Construct a scalar comparison operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLScalarCmp(CDXLMemoryManager *memory_manager_dxl,
									  const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// get comparison operator from attributes
	const XMLCh *scalar_cmp_xml =
		ExtractAttrValue(attrs, EdxltokenComparisonOp, EdxltokenScalarComp);

	// parse op no and function id
	IMDId *op_id = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenOpNo, EdxltokenScalarComp);

	// parse comparison operator from string
	CWStringDynamic *comp_op_name =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, scalar_cmp_xml);

	// copy dynamic string into const string
	CWStringConst *comp_op_name_copy =
		GPOS_NEW(memory_pool) CWStringConst(memory_pool, comp_op_name->GetBuffer());

	// cleanup
	GPOS_DELETE(comp_op_name);

	return GPOS_NEW(memory_pool) CDXLScalarComp(memory_pool, op_id, comp_op_name_copy);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLDistinctCmp
//
//	@doc:
//		Construct a scalar distinct comparison operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLDistinctCmp(CDXLMemoryManager *memory_manager_dxl,
										const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse operator and function id
	IMDId *op_id = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenOpNo, EdxltokenScalarDistinctComp);

	return GPOS_NEW(memory_pool) CDXLScalarDistinctComp(memory_pool, op_id);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLOpExpr
//
//	@doc:
//		Construct a scalar OpExpr
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLOpExpr(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// get scalar OpExpr from attributes
	const XMLCh *op_expr_xml = ExtractAttrValue(attrs, EdxltokenOpName, EdxltokenScalarOpExpr);

	IMDId *op_id = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenOpNo, EdxltokenScalarOpExpr);

	IMDId *return_type_mdid = NULL;
	const XMLCh *return_type_xml = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenOpType));

	if (NULL != return_type_xml)
	{
		return_type_mdid = ExtractConvertAttrValueToMdId(
			memory_manager_dxl, attrs, EdxltokenOpType, EdxltokenScalarOpExpr);
	}

	CWStringDynamic *value =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, op_expr_xml);
	CWStringConst *value_copy =
		GPOS_NEW(memory_pool) CWStringConst(memory_pool, value->GetBuffer());
	GPOS_DELETE(value);

	return GPOS_NEW(memory_pool) CDXLScalarOpExpr(memory_pool, op_id, return_type_mdid, value_copy);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLArrayComp
//
//	@doc:
//		Construct a scalar array comparison
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLArrayComp(CDXLMemoryManager *memory_manager_dxl,
									  const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// get attributes
	const XMLCh *op_expr_xml = ExtractAttrValue(attrs, EdxltokenOpName, EdxltokenScalarArrayComp);

	const XMLCh *op_type_xml = ExtractAttrValue(attrs, EdxltokenOpType, EdxltokenScalarArrayComp);

	// parse operator no and function id
	IMDId *op_id = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenOpNo, EdxltokenScalarArrayComp);

	EdxlArrayCompType array_comp_type = Edxlarraycomptypeany;

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpTypeAll), op_type_xml))
	{
		array_comp_type = Edxlarraycomptypeall;
	}
	else if (0 !=
			 XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpTypeAny), op_type_xml))
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenOpType)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenScalarArrayComp)->GetBuffer());
	}

	CWStringDynamic *opname =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, op_expr_xml);
	CWStringConst *opname_copy =
		GPOS_NEW(memory_pool) CWStringConst(memory_pool, opname->GetBuffer());
	GPOS_DELETE(opname);

	return GPOS_NEW(memory_pool)
		CDXLScalarArrayComp(memory_pool, op_id, opname_copy, array_comp_type);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLBoolExpr
//
//	@doc:
//		Construct a scalar BoolExpr
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLBoolExpr(CDXLMemoryManager *memory_manager_dxl,
									 const EdxlBoolExprType edxlboolexprType)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLScalarBoolExpr(memory_pool, edxlboolexprType);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLBooleanTest
//
//	@doc:
//		Construct a scalar BooleanTest
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLBooleanTest(CDXLMemoryManager *memory_manager_dxl,
										const EdxlBooleanTestType edxlbooleantesttype)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLScalarBooleanTest(memory_pool, edxlbooleantesttype);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLSubPlan
//
//	@doc:
//		Construct a SubPlan node
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLSubPlan(CDXLMemoryManager *memory_manager_dxl,
									IMDId *mdid,
									DrgPdxlcr *dxl_colref_array,
									EdxlSubPlanType dxl_subplan_type,
									CDXLNode *dxlnode_test_expr)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool)
		CDXLScalarSubPlan(memory_pool, mdid, dxl_colref_array, dxl_subplan_type, dxlnode_test_expr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLNullTest
//
//	@doc:
//		Construct a scalar NullTest
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLNullTest(CDXLMemoryManager *memory_manager_dxl, const BOOL is_null)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	return GPOS_NEW(memory_pool) CDXLScalarNullTest(memory_pool, is_null);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLCast
//
//	@doc:
//		Construct a scalar RelabelType
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLCast(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse type id and function id
	IMDId *mdid_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarCast);

	IMDId *mdid_func = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenFuncId, EdxltokenScalarCast);

	return GPOS_NEW(memory_pool) CDXLScalarCast(memory_pool, mdid_type, mdid_func);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLCoerceToDomain
//
//	@doc:
//		Construct a scalar coerce
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLCoerceToDomain(CDXLMemoryManager *memory_manager_dxl,
										   const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse type id and function id
	IMDId *mdid_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarCoerceToDomain);
	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenScalarCoerceToDomain,
													 true,
													 default_type_modifier);
	ULONG coercion_form = ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenCoercionForm, EdxltokenScalarCoerceToDomain);
	INT location = ExtractConvertAttrValueToInt(
		memory_manager_dxl, attrs, EdxltokenLocation, EdxltokenScalarCoerceToDomain);

	return GPOS_NEW(memory_pool) CDXLScalarCoerceToDomain(
		memory_pool, mdid_type, type_modifier, (EdxlCoercionForm) coercion_form, location);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLCoerceViaIO
//
//	@doc:
//		Construct a scalar coerce
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLCoerceViaIO(CDXLMemoryManager *memory_manager_dxl,
										const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse type id and function id
	IMDId *mdid_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarCoerceViaIO);
	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenScalarCoerceViaIO,
													 true,
													 default_type_modifier);
	ULONG coercion_form = ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenCoercionForm, EdxltokenScalarCoerceViaIO);
	INT location = ExtractConvertAttrValueToInt(
		memory_manager_dxl, attrs, EdxltokenLocation, EdxltokenScalarCoerceViaIO);

	return GPOS_NEW(memory_pool) CDXLScalarCoerceViaIO(
		memory_pool, mdid_type, type_modifier, (EdxlCoercionForm) coercion_form, location);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLArrayCoerceExpr
//
//	@doc:
//		Construct a scalar array coerce expression
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLArrayCoerceExpr(CDXLMemoryManager *memory_manager_dxl,
											const Attributes &attrs)
{
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	IMDId *element_func = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenElementFunc, EdxltokenScalarArrayCoerceExpr);
	IMDId *mdid_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarArrayCoerceExpr);
	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenScalarArrayCoerceExpr,
													 true,
													 default_type_modifier);
	BOOL is_explicit = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenIsExplicit, EdxltokenScalarArrayCoerceExpr);
	ULONG coercion_form = ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenCoercionForm, EdxltokenScalarArrayCoerceExpr);
	INT location = ExtractConvertAttrValueToInt(
		memory_manager_dxl, attrs, EdxltokenLocation, EdxltokenScalarArrayCoerceExpr);

	return GPOS_NEW(memory_pool) CDXLScalarArrayCoerceExpr(memory_pool,
														   element_func,
														   mdid_type,
														   type_modifier,
														   is_explicit,
														   (EdxlCoercionForm) coercion_form,
														   location);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLConstValue
//
//	@doc:
//		Construct a scalar Const
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLConstValue(CDXLMemoryManager *memory_manager_dxl,
									   const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();
	CDXLDatum *datum_dxl = GetDatumVal(memory_manager_dxl, attrs, EdxltokenScalarConstValue);

	return GPOS_NEW(memory_pool) CDXLScalarConstValue(memory_pool, datum_dxl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLIfStmt
//
//	@doc:
//		Construct an if statement operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLIfStmt(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// get the type id
	IMDId *mdid_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarIfStmt);

	return GPOS_NEW(memory_pool) CDXLScalarIfStmt(memory_pool, mdid_type);
}


//		Construct an funcexpr operator
CDXLScalar *
CDXLOperatorFactory::MakeDXLFuncExpr(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	IMDId *mdid_func = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenFuncId, EdxltokenScalarFuncExpr);

	BOOL is_retset = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenFuncRetSet, EdxltokenScalarFuncExpr);

	IMDId *mdid_return_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarFuncExpr);

	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenScalarCast,
													 true,
													 default_type_modifier);

	return GPOS_NEW(memory_pool)
		CDXLScalarFuncExpr(memory_pool, mdid_func, mdid_return_type, type_modifier, is_retset);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLAggFunc
//
//	@doc:
//		Construct an AggRef operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLAggFunc(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	IMDId *agg_mdid = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenAggrefOid, EdxltokenScalarAggref);

	const XMLCh *agg_stage_xml =
		ExtractAttrValue(attrs, EdxltokenAggrefStage, EdxltokenScalarAggref);

	BOOL is_distinct = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenAggrefDistinct, EdxltokenScalarAggref);

	EdxlAggrefStage agg_stage = EdxlaggstageFinal;

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggrefStageNormal),
									  agg_stage_xml))
	{
		agg_stage = EdxlaggstageNormal;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggrefStagePartial),
										   agg_stage_xml))
	{
		agg_stage = EdxlaggstagePartial;
	}
	else if (0 == XMLString::compareString(
					  CDXLTokens::XmlstrToken(EdxltokenAggrefStageIntermediate), agg_stage_xml))
	{
		agg_stage = EdxlaggstageIntermediate;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggrefStageFinal),
										   agg_stage_xml))
	{
		agg_stage = EdxlaggstageFinal;
	}
	else
	{
		// turn Xerces exception in optimizer exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenAggrefStage)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenScalarAggref)->GetBuffer());
	}

	IMDId *resolved_rettype = NULL;
	const XMLCh *return_type_xml = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenTypeId));
	if (NULL != return_type_xml)
	{
		resolved_rettype = ExtractConvertAttrValueToMdId(
			memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarAggref);
	}

	return GPOS_NEW(memory_pool)
		CDXLScalarAggref(memory_pool, agg_mdid, resolved_rettype, is_distinct, agg_stage);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseDXLFrameBoundary
//
//	@doc:
//		Parse the frame boundary
//
//---------------------------------------------------------------------------
EdxlFrameBoundary
CDXLOperatorFactory::ParseDXLFrameBoundary(const Attributes &attrs, Edxltoken token_type)
{
	const XMLCh *frame_boundary_xml = ExtractAttrValue(attrs, token_type, EdxltokenWindowFrame);

	EdxlFrameBoundary frame_boundary = EdxlfbSentinel;
	ULONG window_frame_boundary_to_frame_boundary_mapping[][2] = {
		{EdxlfbUnboundedPreceding, EdxltokenWindowBoundaryUnboundedPreceding},
		{EdxlfbBoundedPreceding, EdxltokenWindowBoundaryBoundedPreceding},
		{EdxlfbCurrentRow, EdxltokenWindowBoundaryCurrentRow},
		{EdxlfbUnboundedFollowing, EdxltokenWindowBoundaryUnboundedFollowing},
		{EdxlfbBoundedFollowing, EdxltokenWindowBoundaryBoundedFollowing},
		{EdxlfbDelayedBoundedPreceding, EdxltokenWindowBoundaryDelayedBoundedPreceding},
		{EdxlfbDelayedBoundedFollowing, EdxltokenWindowBoundaryDelayedBoundedFollowing}};

	const ULONG arity = GPOS_ARRAY_SIZE(window_frame_boundary_to_frame_boundary_mapping);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ULONG *current = window_frame_boundary_to_frame_boundary_mapping[ul];
		Edxltoken current_window = (Edxltoken) current[1];
		if (0 ==
			XMLString::compareString(CDXLTokens::XmlstrToken(current_window), frame_boundary_xml))
		{
			frame_boundary = (EdxlFrameBoundary) current[0];
			break;
		}
	}

	if (EdxlfbSentinel == frame_boundary)
	{
		// turn Xerces exception in optimizer exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(token_type)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenWindowFrame)->GetBuffer());
	}

	return frame_boundary;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseDXLFrameSpec
//
//	@doc:
//		Parse the frame specification
//
//---------------------------------------------------------------------------
EdxlFrameSpec
CDXLOperatorFactory::ParseDXLFrameSpec(const Attributes &attrs)
{
	const XMLCh *frame_spec_xml =
		ExtractAttrValue(attrs, EdxltokenWindowFrameSpec, EdxltokenWindowFrame);

	EdxlFrameSpec frame_spec = EdxlfsSentinel;
	if (0 ==
		XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowFSRow), frame_spec_xml))
	{
		frame_spec = EdxlfsRow;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowFSRange),
										   frame_spec_xml))
	{
		frame_spec = EdxlfsRange;
	}
	else
	{
		// turn Xerces exception in optimizer exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenWindowFrameSpec)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenWindowFrame)->GetBuffer());
	}

	return frame_spec;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseFrameExclusionStrategy
//
//	@doc:
//		Parse the frame exclusion strategy
//
//---------------------------------------------------------------------------
EdxlFrameExclusionStrategy
CDXLOperatorFactory::ParseFrameExclusionStrategy(const Attributes &attrs)
{
	const XMLCh *frame_exc_strategy_xml =
		ExtractAttrValue(attrs, EdxltokenWindowExclusionStrategy, EdxltokenWindowFrame);

	ULONG window_frame_boundary_to_frame_boundary_mapping[][2] = {
		{EdxlfesNone, EdxltokenWindowESNone},
		{EdxlfesNulls, EdxltokenWindowESNulls},
		{EdxlfesCurrentRow, EdxltokenWindowESCurrentRow},
		{EdxlfesGroup, EdxltokenWindowESGroup},
		{EdxlfesTies, EdxltokenWindowESTies}};

	EdxlFrameExclusionStrategy frame_exc_strategy = EdxlfesSentinel;
	const ULONG arity = GPOS_ARRAY_SIZE(window_frame_boundary_to_frame_boundary_mapping);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ULONG *current = window_frame_boundary_to_frame_boundary_mapping[ul];
		Edxltoken current_window = (Edxltoken) current[1];
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(current_window),
										  frame_exc_strategy_xml))
		{
			frame_exc_strategy = (EdxlFrameExclusionStrategy) current[0];
			break;
		}
	}

	if (EdxlfesSentinel == frame_exc_strategy)
	{
		// turn Xerces exception in optimizer exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenWindowExclusionStrategy)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenWindowFrame)->GetBuffer());
	}

	return frame_exc_strategy;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLArray
//
//	@doc:
//		Construct an array operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLArray(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	IMDId *elem_type_mdid = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenArrayElementType, EdxltokenScalarArray);
	IMDId *array_type_mdid = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenArrayType, EdxltokenScalarArray);
	BOOL is_multidimenstional = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenArrayMultiDim, EdxltokenScalarArray);

	return GPOS_NEW(memory_pool)
		CDXLScalarArray(memory_pool, elem_type_mdid, array_type_mdid, is_multidimenstional);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLScalarIdent
//
//	@doc:
//		Construct a scalar identifier operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLScalarIdent(CDXLMemoryManager *memory_manager_dxl,
										const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	CDXLColRef *dxl_colref = MakeDXLColRef(memory_manager_dxl, attrs, EdxltokenScalarIdent);

	return GPOS_NEW(memory_pool) CDXLScalarIdent(memory_pool, dxl_colref);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLProjElem
//
//	@doc:
//		Construct a proj elem operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLProjElem(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse alias from attributes
	const XMLCh *xml_alias = ExtractAttrValue(attrs, EdxltokenAlias, EdxltokenScalarProjElem);

	// parse column id
	ULONG id = ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenColId, EdxltokenScalarProjElem);

	CWStringDynamic *alias =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, xml_alias);

	// create a copy of the string in the CMDName constructor
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, alias);

	GPOS_DELETE(alias);

	return GPOS_NEW(memory_pool) CDXLScalarProjElem(memory_pool, id, mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLHashExpr
//
//	@doc:
//		Construct a hash expr operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLHashExpr(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// get column type id and type name from attributes

	IMDId *mdid_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarHashExpr);

	return GPOS_NEW(memory_pool) CDXLScalarHashExpr(memory_pool, mdid_type);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLSortCol
//
//	@doc:
//		Construct a sorting column description
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeDXLSortCol(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// get column id from attributes
	ULONG col_id = ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenColId, EdxltokenScalarSortCol);

	// get sorting operator name
	const XMLCh *sort_op_xml = ExtractAttrValue(attrs, EdxltokenSortOpName, EdxltokenScalarSortCol);
	CWStringDynamic *sort_op_name =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, sort_op_xml);

	// get null first property
	BOOL nulls_first = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenSortNullsFirst, EdxltokenPhysicalSort);

	// parse sorting operator id
	IMDId *sort_op_id = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenSortOpId, EdxltokenPhysicalSort);

	// copy dynamic string into const string
	CWStringConst *sort_op_name_copy =
		GPOS_NEW(memory_pool) CWStringConst(memory_pool, sort_op_name->GetBuffer());

	GPOS_DELETE(sort_op_name);

	return GPOS_NEW(memory_pool)
		CDXLScalarSortCol(memory_pool, col_id, sort_op_id, sort_op_name_copy, nulls_first);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLOperatorCost
//
//	@doc:
//		Construct a cost estimates element
//
//---------------------------------------------------------------------------
CDXLOperatorCost *
CDXLOperatorFactory::MakeDXLOperatorCost(CDXLMemoryManager *memory_manager_dxl,
										 const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	const XMLCh *startup_cost_xml = ExtractAttrValue(attrs, EdxltokenStartupCost, EdxltokenCost);

	const XMLCh *total_cost_xml = ExtractAttrValue(attrs, EdxltokenTotalCost, EdxltokenCost);

	const XMLCh *rows_xml = ExtractAttrValue(attrs, EdxltokenRows, EdxltokenCost);

	const XMLCh *width_xml = ExtractAttrValue(attrs, EdxltokenWidth, EdxltokenCost);

	CWStringDynamic *startup_cost_str =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, startup_cost_xml);
	CWStringDynamic *total_cost_str =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, total_cost_xml);
	CWStringDynamic *rows_out_str =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, rows_xml);
	CWStringDynamic *width_str =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, width_xml);

	return GPOS_NEW(memory_pool)
		CDXLOperatorCost(startup_cost_str, total_cost_str, rows_out_str, width_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLTableDescr
//
//	@doc:
//		Construct a table descriptor
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CDXLOperatorFactory::MakeDXLTableDescr(CDXLMemoryManager *memory_manager_dxl,
									   const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse table descriptor from attributes
	const XMLCh *xml_str_table_name =
		ExtractAttrValue(attrs, EdxltokenTableName, EdxltokenTableDescr);

	CMDName *mdname = CDXLUtils::CreateMDNameFromXMLChar(memory_manager_dxl, xml_str_table_name);

	// parse metadata id
	IMDId *mdid = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenMdid, EdxltokenTableDescr);

	// parse execute as user m_bytearray_value if the attribute is specified
	ULONG user_id = GPDXL_DEFAULT_USERID;
	const XMLCh *execute_as_user_xml =
		attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenExecuteAsUser));

	if (NULL != execute_as_user_xml)
	{
		user_id = ConvertAttrValueToUlong(
			memory_manager_dxl, execute_as_user_xml, EdxltokenExecuteAsUser, EdxltokenTableDescr);
	}

	return GPOS_NEW(memory_pool) CDXLTableDescr(memory_pool, mdid, mdname, user_id);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLIndexDescr
//
//	@doc:
//		Construct an index descriptor
//
//---------------------------------------------------------------------------
CDXLIndexDescr *
CDXLOperatorFactory::MakeDXLIndexDescr(CDXLMemoryManager *memory_manager_dxl,
									   const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse index descriptor from attributes
	const XMLCh *index_name_xml = ExtractAttrValue(attrs, EdxltokenIndexName, EdxltokenIndexDescr);

	CWStringDynamic *index_name =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, index_name_xml);

	// parse metadata id
	IMDId *mdid = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenMdid, EdxltokenIndexDescr);

	// create a copy of the string in the CMDName constructor
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, index_name);
	GPOS_DELETE(index_name);

	return GPOS_NEW(memory_pool) CDXLIndexDescr(memory_pool, mdid, mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeColumnDescr
//
//	@doc:
//		Construct a column descriptor
//
//---------------------------------------------------------------------------
CDXLColDescr *
CDXLOperatorFactory::MakeColumnDescr(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse column name from attributes
	const XMLCh *column_name_xml = ExtractAttrValue(attrs, EdxltokenColName, EdxltokenColDescr);

	// parse column id
	ULONG id = ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenColId, EdxltokenColDescr);

	// parse attno
	INT attno =
		ExtractConvertAttrValueToInt(memory_manager_dxl, attrs, EdxltokenAttno, EdxltokenColDescr);

	if (0 == attno)
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenAttno)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenColDescr)->GetBuffer());
	}

	// parse column type id
	IMDId *mdid_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenColDescr);

	// parse optional type modifier from attributes
	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenColDescr,
													 true,
													 default_type_modifier);

	BOOL col_dropped = false;

	const XMLCh *col_dropped_xml = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColDropped));

	if (NULL != col_dropped_xml)
	{
		// attribute is present: get m_bytearray_value
		col_dropped = ConvertAttrValueToBool(
			memory_manager_dxl, col_dropped_xml, EdxltokenColDropped, EdxltokenColDescr);
	}

	ULONG col_len = gpos::ulong_max;

	// parse column length from attributes
	const XMLCh *col_len_xml = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColWidth));

	if (NULL != col_len_xml)
	{
		col_len = ConvertAttrValueToUlong(
			memory_manager_dxl, col_len_xml, EdxltokenColWidth, EdxltokenColDescr);
	}

	CWStringDynamic *col_name =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, column_name_xml);

	// create a copy of the string in the CMDName constructor
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, col_name);

	GPOS_DELETE(col_name);

	return GPOS_NEW(memory_pool) CDXLColDescr(
		memory_pool, mdname, id, attno, mdid_type, type_modifier, col_dropped, col_len);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeDXLColRef
//
//	@doc:
//		Construct a column reference
//
//---------------------------------------------------------------------------
CDXLColRef *
CDXLOperatorFactory::MakeDXLColRef(CDXLMemoryManager *memory_manager_dxl,
								   const Attributes &attrs,
								   Edxltoken target_elem)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	// parse column name from attributes
	const XMLCh *column_name_xml = ExtractAttrValue(attrs, EdxltokenColName, target_elem);

	// parse column id
	ULONG id = 0;
	const XMLCh *col_id_xml = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColId));
	if (NULL == col_id_xml)
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLMissingAttribute,
				   CDXLTokens::GetDXLTokenStr(EdxltokenColRef)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}

	id = XMLString::parseInt(col_id_xml, memory_manager_dxl);

	CWStringDynamic *col_name =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, column_name_xml);

	// create a copy of the string in the CMDName constructor
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, col_name);

	GPOS_DELETE(col_name);

	IMDId *mdid_type =
		ExtractConvertAttrValueToMdId(memory_manager_dxl, attrs, EdxltokenTypeId, target_elem);

	// parse optional type modifier
	INT type_modifier = ExtractConvertAttrValueToInt(
		memory_manager_dxl, attrs, EdxltokenTypeMod, target_elem, true, default_type_modifier);

	return GPOS_NEW(memory_pool) CDXLColRef(memory_pool, mdname, id, mdid_type, type_modifier);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseOutputSegId
//
//	@doc:
//		Parse an output segment index
//
//---------------------------------------------------------------------------
INT
CDXLOperatorFactory::ParseOutputSegId(CDXLMemoryManager *memory_manager_dxl,
									  const Attributes &attrs)
{
	// get output segment index from attributes
	const XMLCh *seg_id_xml = ExtractAttrValue(attrs, EdxltokenSegId, EdxltokenSegment);

	// parse segment id from string
	INT segment_id = -1;
	try
	{
		segment_id = XMLString::parseInt(seg_id_xml, memory_manager_dxl);
	}
	catch (const NumberFormatException &toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenSegId)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenSegment)->GetBuffer());
	}

	return segment_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractAttrValue
//
//	@doc:
//    	Extracts the m_bytearray_value for the given attribute.
// 		If there is no such attribute defined, and the given optional
// 		flag is set to false then it will raise an exception
//---------------------------------------------------------------------------
const XMLCh *
CDXLOperatorFactory::ExtractAttrValue(const Attributes &attrs,
									  Edxltoken target_attr,
									  Edxltoken target_elem,
									  BOOL is_optional)
{
	const XMLCh *attribute_val_xml = attrs.getValue(CDXLTokens::XmlstrToken(target_attr));

	if (NULL == attribute_val_xml && !is_optional)
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLMissingAttribute,
				   CDXLTokens::GetDXLTokenStr(target_attr)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}

	return attribute_val_xml;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToUlong
//
//	@doc:
//	  	Converts the attribute m_bytearray_value to ULONG
//
//---------------------------------------------------------------------------
ULONG
CDXLOperatorFactory::ConvertAttrValueToUlong(CDXLMemoryManager *memory_manager_dxl,
											 const XMLCh *attribute_val_xml,
											 Edxltoken target_attr,
											 Edxltoken target_elem)
{
	GPOS_ASSERT(attribute_val_xml != NULL);
	ULONG attr = 0;
	try
	{
		attr = XMLString::parseInt(attribute_val_xml, memory_manager_dxl);
	}
	catch (const NumberFormatException &toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(target_attr)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}
	return attr;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToUllong
//
//	@doc:
//	  	Converts the attribute m_bytearray_value to ULLONG
//
//---------------------------------------------------------------------------
ULLONG
CDXLOperatorFactory::ConvertAttrValueToUllong(CDXLMemoryManager *memory_manager_dxl,
											  const XMLCh *attribute_val_xml,
											  Edxltoken target_attr,
											  Edxltoken target_elem)
{
	GPOS_ASSERT(attribute_val_xml != NULL);

	CHAR *attr = XMLString::transcode(attribute_val_xml, memory_manager_dxl);
	GPOS_ASSERT(NULL != attr);

	CHAR **end = NULL;
	LINT converted_val = clib::Strtoll(attr, end, 10 /*ulBase*/);

	if ((NULL != end && attr == *end) || gpos::lint_max == converted_val ||
		gpos::lint_min == converted_val || 0 > converted_val)
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(target_attr)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}

	XMLString::release(&attr, memory_manager_dxl);

	return (ULLONG) converted_val;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToBool
//
//	@doc:
//	  	Converts the attribute m_bytearray_value to BOOL
//
//---------------------------------------------------------------------------
BOOL
CDXLOperatorFactory::ConvertAttrValueToBool(CDXLMemoryManager *memory_manager_dxl,
											const XMLCh *attribute_val_xml,
											Edxltoken target_attr,
											Edxltoken target_elem)
{
	GPOS_ASSERT(attribute_val_xml != NULL);
	BOOL flag = false;
	CHAR *attr = XMLString::transcode(attribute_val_xml, memory_manager_dxl);

	if (0 == strncasecmp(attr, "true", 4))
	{
		flag = true;
	}
	else if (0 != strncasecmp(attr, "false", 5))
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(target_attr)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}

	XMLString::release(&attr, memory_manager_dxl);
	return flag;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToInt
//
//	@doc:
//	  	Converts the attribute m_bytearray_value from xml string to INT
//
//---------------------------------------------------------------------------
INT
CDXLOperatorFactory::ConvertAttrValueToInt(CDXLMemoryManager *memory_manager_dxl,
										   const XMLCh *attribute_val_xml,
										   Edxltoken target_attr,
										   Edxltoken target_elem)
{
	GPOS_ASSERT(attribute_val_xml != NULL);
	INT attr = 0;
	try
	{
		attr = XMLString::parseInt(attribute_val_xml, memory_manager_dxl);
	}
	catch (const NumberFormatException &toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(target_attr)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}
	return attr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToInt
//
//	@doc:
//	  	Extracts the m_bytearray_value for the given attribute and converts it into INT
//
//---------------------------------------------------------------------------
INT
CDXLOperatorFactory::ExtractConvertAttrValueToInt(CDXLMemoryManager *memory_manager_dxl,
												  const Attributes &attrs,
												  Edxltoken target_attr,
												  Edxltoken target_elem,
												  BOOL is_optional,
												  INT default_val)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return default_val;
	}

	return ConvertAttrValueToInt(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToShortInt
//
//	@doc:
//	  	Converts the attribute m_bytearray_value from xml string to short int
//
//---------------------------------------------------------------------------
SINT
CDXLOperatorFactory::ConvertAttrValueToShortInt(CDXLMemoryManager *memory_manager_dxl,
												const XMLCh *attribute_val_xml,
												Edxltoken target_attr,
												Edxltoken target_elem)
{
	GPOS_ASSERT(attribute_val_xml != NULL);
	SINT attr = 0;
	try
	{
		attr = (SINT) XMLString::parseInt(attribute_val_xml, memory_manager_dxl);
	}
	catch (const NumberFormatException &toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(target_attr)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}
	return attr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToShortInt
//
//	@doc:
//	  	Extracts the m_bytearray_value for the given attribute and converts it into short
//		int
//
//---------------------------------------------------------------------------
SINT
CDXLOperatorFactory::ExtractConvertAttrValueToShortInt(CDXLMemoryManager *memory_manager_dxl,
													   const Attributes &attrs,
													   Edxltoken target_attr,
													   Edxltoken target_elem,
													   BOOL is_optional,
													   SINT default_val)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return default_val;
	}

	return ConvertAttrValueToShortInt(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

// Converts the attribute m_bytearray_value from xml string to char
CHAR
CDXLOperatorFactory::ConvertAttrValueToChar(CDXLMemoryManager *memory_manager_dxl,
											const XMLCh *xml_val,
											Edxltoken,  // target_attr,
											Edxltoken   // target_elem
)
{
	GPOS_ASSERT(xml_val != NULL);
	CHAR *attr = XMLString::transcode(xml_val, memory_manager_dxl);
	CHAR val = *attr;
	XMLString::release(&attr, memory_manager_dxl);
	return val;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToOid
//
//	@doc:
//	  	Converts the attribute m_bytearray_value to OID
//
//---------------------------------------------------------------------------
OID
CDXLOperatorFactory::ConvertAttrValueToOid(CDXLMemoryManager *memory_manager_dxl,
										   const XMLCh *attribute_val_xml,
										   Edxltoken target_attr,
										   Edxltoken target_elem)
{
	GPOS_ASSERT(attribute_val_xml != NULL);
	OID oid = 0;
	try
	{
		oid = XMLString::parseInt(attribute_val_xml, memory_manager_dxl);
	}
	catch (const NumberFormatException &toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(target_attr)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(target_elem)->GetBuffer());
	}
	return oid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToOid
//
//	@doc:
//	  	Extracts the m_bytearray_value for the given attribute and converts it into OID
//
//---------------------------------------------------------------------------
OID
CDXLOperatorFactory::ExtractConvertAttrValueToOid(CDXLMemoryManager *memory_manager_dxl,
												  const Attributes &attrs,
												  Edxltoken target_attr,
												  Edxltoken target_elem,
												  BOOL is_optional,
												  OID OidDefaultValue)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return OidDefaultValue;
	}

	return ConvertAttrValueToOid(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToSz
//
//	@doc:
//	  	Converts the string attribute m_bytearray_value
//
//---------------------------------------------------------------------------
CHAR *
CDXLOperatorFactory::ConvertAttrValueToSz(CDXLMemoryManager *memory_manager_dxl,
										  const XMLCh *xml_val,
										  Edxltoken,  // target_attr,
										  Edxltoken   // target_elem
)
{
	GPOS_ASSERT(NULL != xml_val);
	return XMLString::transcode(xml_val, memory_manager_dxl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToSz
//
//	@doc:
//	  	Extracts the m_bytearray_value for the given attribute and converts it into CHAR*
//
//---------------------------------------------------------------------------
CHAR *
CDXLOperatorFactory::ExtractConvertAttrValueToSz(CDXLMemoryManager *memory_manager_dxl,
												 const Attributes &attrs,
												 Edxltoken target_attr,
												 Edxltoken target_elem,
												 BOOL is_optional,
												 CHAR *default_value)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return default_value;
	}

	return CDXLOperatorFactory::ConvertAttrValueToSz(
		memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToStr
//
//	@doc:
//	  	Extracts the string m_bytearray_value for the given attribute
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLOperatorFactory::ExtractConvertAttrValueToStr(CDXLMemoryManager *memory_manager_dxl,
												  const Attributes &attrs,
												  Edxltoken target_attr,
												  Edxltoken target_elem)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem);
	return CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, attr_val_xml);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToBool
//
//	@doc:
//	  	Extracts the m_bytearray_value for the given attribute and converts it into BOOL
//
//---------------------------------------------------------------------------
BOOL
CDXLOperatorFactory::ExtractConvertAttrValueToBool(CDXLMemoryManager *memory_manager_dxl,
												   const Attributes &attrs,
												   Edxltoken target_attr,
												   Edxltoken target_elem,
												   BOOL is_optional,
												   BOOL default_value)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return default_value;
	}

	return ConvertAttrValueToBool(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToUlong
//
//	@doc:
//	  	Extracts the m_bytearray_value for the given attribute and converts it into ULONG
//
//---------------------------------------------------------------------------
ULONG
CDXLOperatorFactory::ExtractConvertAttrValueToUlong(CDXLMemoryManager *memory_manager_dxl,
													const Attributes &attrs,
													Edxltoken target_attr,
													Edxltoken target_elem,
													BOOL is_optional,
													ULONG default_value)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return default_value;
	}

	return ConvertAttrValueToUlong(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToUllong
//
//	@doc:
//	  	Extracts the m_bytearray_value for the given attribute and converts it into ULLONG
//
//---------------------------------------------------------------------------
ULLONG
CDXLOperatorFactory::ExtractConvertAttrValueToUllong(CDXLMemoryManager *memory_manager_dxl,
													 const Attributes &attrs,
													 Edxltoken target_attr,
													 Edxltoken target_elem,
													 BOOL is_optional,
													 ULLONG default_value)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return default_value;
	}

	return ConvertAttrValueToUllong(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseGroupingColId
//
//	@doc:
//		Parse a grouping column id
//
//---------------------------------------------------------------------------
ULONG
CDXLOperatorFactory::ParseGroupingColId(CDXLMemoryManager *memory_manager_dxl,
										const Attributes &attrs)
{
	const CWStringConst *grouping_col_id_str = CDXLTokens::GetDXLTokenStr(EdxltokenGroupingCol);
	const CWStringConst *col_id_str = CDXLTokens::GetDXLTokenStr(EdxltokenColId);

	// get grouping column id from attributes
	INT col_id = ExtractConvertAttrValueToInt(
		memory_manager_dxl, attrs, EdxltokenColId, EdxltokenGroupingCol);

	if (col_id < 0)
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   col_id_str->GetBuffer(),
				   grouping_col_id_str->GetBuffer());
	}

	return (ULONG) col_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToMdId
//
//	@doc:
//		Parse a metadata id object from the XML attributes of the specified element.
//
//---------------------------------------------------------------------------
IMDId *
CDXLOperatorFactory::ExtractConvertAttrValueToMdId(CDXLMemoryManager *memory_manager_dxl,
												   const Attributes &attrs,
												   Edxltoken target_attr,
												   Edxltoken target_elem,
												   BOOL is_optional,
												   IMDId *default_val)
{
	// extract mdid
	const XMLCh *mdid_xml = ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == mdid_xml)
	{
		if (NULL != default_val)
		{
			default_val->AddRef();
		}

		return default_val;
	}

	return MakeMdIdFromStr(memory_manager_dxl, mdid_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeMdIdFromStr
//
//	@doc:
//		Parse a metadata id object from the XML attributes of the specified element.
//
//---------------------------------------------------------------------------
IMDId *
CDXLOperatorFactory::MakeMdIdFromStr(CDXLMemoryManager *memory_manager_dxl,
									 const XMLCh *mdid_xml,
									 Edxltoken target_attr,
									 Edxltoken target_elem)
{
	// extract mdid's components: MdidType.Oid.Major.Minor
	XMLStringTokenizer mdid_components(mdid_xml, CDXLTokens::XmlstrToken(EdxltokenDotSemicolon));

	GPOS_ASSERT(1 < mdid_components.countTokens());

	// get mdid type from first component
	XMLCh *mdid_type = mdid_components.nextToken();

	// collect the remaining tokens in an array
	DrgPxmlsz *remaining_tokens =
		GPOS_NEW(memory_manager_dxl->Pmp()) DrgPxmlsz(memory_manager_dxl->Pmp());

	XMLCh *xml_val = NULL;
	while (NULL != (xml_val = mdid_components.nextToken()))
	{
		remaining_tokens->Append(xml_val);
	}

	IMDId::EMDIdType typ = (IMDId::EMDIdType) ConvertAttrValueToUlong(
		memory_manager_dxl, mdid_type, target_attr, target_elem);

	IMDId *mdid = NULL;
	switch (typ)
	{
		case IMDId::EmdidGPDB:
			mdid = GetGPDBMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
			break;

		case IMDId::EmdidGPDBCtas:
			mdid = GetGPDBCTASMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
			break;

		case IMDId::EmdidColStats:
			mdid = GetColStatsMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
			break;

		case IMDId::EmdidRelStats:
			mdid = GetRelStatsMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
			break;

		case IMDId::EmdidCastFunc:
			mdid = GetCastFuncMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
			break;

		case IMDId::EmdidScCmp:
			mdid = GetScCmpMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
			break;

		default:
			GPOS_ASSERT(!"Unrecognized mdid type");
	}

	remaining_tokens->Release();

	return mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetGPDBMdId
//
//	@doc:
//		Construct a GPDB mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdGPDB *
CDXLOperatorFactory::GetGPDBMdId(CDXLMemoryManager *memory_manager_dxl,
								 DrgPxmlsz *remaining_tokens,
								 Edxltoken target_attr,
								 Edxltoken target_elem)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS <= remaining_tokens->Size());

	XMLCh *xml_oid = (*remaining_tokens)[0];
	ULONG oid_colid =
		ConvertAttrValueToUlong(memory_manager_dxl, xml_oid, target_attr, target_elem);

	XMLCh *version_major_xml = (*remaining_tokens)[1];
	ULONG version_major =
		ConvertAttrValueToUlong(memory_manager_dxl, version_major_xml, target_attr, target_elem);

	XMLCh *xmlszVersionMinor = (*remaining_tokens)[2];
	;
	ULONG version_minor =
		ConvertAttrValueToUlong(memory_manager_dxl, xmlszVersionMinor, target_attr, target_elem);

	// construct metadata id object
	return GPOS_NEW(memory_manager_dxl->Pmp()) CMDIdGPDB(oid_colid, version_major, version_minor);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetGPDBCTASMdId
//
//	@doc:
//		Construct a GPDB CTAS mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdGPDB *
CDXLOperatorFactory::GetGPDBCTASMdId(CDXLMemoryManager *memory_manager_dxl,
									 DrgPxmlsz *remaining_tokens,
									 Edxltoken target_attr,
									 Edxltoken target_elem)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS <= remaining_tokens->Size());

	XMLCh *xml_oid = (*remaining_tokens)[0];
	ULONG oid_colid =
		ConvertAttrValueToUlong(memory_manager_dxl, xml_oid, target_attr, target_elem);

	// construct metadata id object
	return GPOS_NEW(memory_manager_dxl->Pmp()) CMDIdGPDBCtas(oid_colid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetColStatsMdId
//
//	@doc:
//		Construct a column stats mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdColStats *
CDXLOperatorFactory::GetColStatsMdId(CDXLMemoryManager *memory_manager_dxl,
									 DrgPxmlsz *remaining_tokens,
									 Edxltoken target_attr,
									 Edxltoken target_elem)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS + 1 == remaining_tokens->Size());

	CMDIdGPDB *rel_mdid =
		GetGPDBMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);

	XMLCh *attno_xml = (*remaining_tokens)[3];
	ULONG attno = ConvertAttrValueToUlong(memory_manager_dxl, attno_xml, target_attr, target_elem);

	// construct metadata id object
	return GPOS_NEW(memory_manager_dxl->Pmp()) CMDIdColStats(rel_mdid, attno);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetRelStatsMdId
//
//	@doc:
//		Construct a relation stats mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdRelStats *
CDXLOperatorFactory::GetRelStatsMdId(CDXLMemoryManager *memory_manager_dxl,
									 DrgPxmlsz *remaining_tokens,
									 Edxltoken target_attr,
									 Edxltoken target_elem)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS == remaining_tokens->Size());

	CMDIdGPDB *rel_mdid =
		GetGPDBMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);

	// construct metadata id object
	return GPOS_NEW(memory_manager_dxl->Pmp()) CMDIdRelStats(rel_mdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetCastFuncMdId
//
//	@doc:
//		Construct a cast function mdid from the array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdCast *
CDXLOperatorFactory::GetCastFuncMdId(CDXLMemoryManager *memory_manager_dxl,
									 DrgPxmlsz *remaining_tokens,
									 Edxltoken target_attr,
									 Edxltoken target_elem)
{
	GPOS_ASSERT(2 * GPDXL_GPDB_MDID_COMPONENTS == remaining_tokens->Size());

	CMDIdGPDB *mdid_src =
		GetGPDBMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
	DrgPxmlsz *dest_xml = GPOS_NEW(memory_manager_dxl->Pmp()) DrgPxmlsz(memory_manager_dxl->Pmp());

	for (ULONG ul = GPDXL_GPDB_MDID_COMPONENTS; ul < GPDXL_GPDB_MDID_COMPONENTS * 2; ul++)
	{
		dest_xml->Append((*remaining_tokens)[ul]);
	}

	CMDIdGPDB *mdid_dest = GetGPDBMdId(memory_manager_dxl, dest_xml, target_attr, target_elem);
	dest_xml->Release();

	return GPOS_NEW(memory_manager_dxl->Pmp()) CMDIdCast(mdid_src, mdid_dest);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetScCmpMdId
//
//	@doc:
//		Construct a scalar comparison operator mdid from the array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdScCmp *
CDXLOperatorFactory::GetScCmpMdId(CDXLMemoryManager *memory_manager_dxl,
								  DrgPxmlsz *remaining_tokens,
								  Edxltoken target_attr,
								  Edxltoken target_elem)
{
	GPOS_ASSERT(2 * GPDXL_GPDB_MDID_COMPONENTS + 1 == remaining_tokens->Size());

	CMDIdGPDB *left_mdid =
		GetGPDBMdId(memory_manager_dxl, remaining_tokens, target_attr, target_elem);
	DrgPxmlsz *right_xml = GPOS_NEW(memory_manager_dxl->Pmp()) DrgPxmlsz(memory_manager_dxl->Pmp());

	for (ULONG ul = GPDXL_GPDB_MDID_COMPONENTS; ul < GPDXL_GPDB_MDID_COMPONENTS * 2 + 1; ul++)
	{
		right_xml->Append((*remaining_tokens)[ul]);
	}

	CMDIdGPDB *right_mdid = GetGPDBMdId(memory_manager_dxl, right_xml, target_attr, target_elem);

	// parse the comparison type from the last component of the mdid
	XMLCh *xml_str_comp_type = (*right_xml)[right_xml->Size() - 1];
	IMDType::ECmpType cmp_type = (IMDType::ECmpType) ConvertAttrValueToUlong(
		memory_manager_dxl, xml_str_comp_type, target_attr, target_elem);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);

	right_xml->Release();

	return GPOS_NEW(memory_manager_dxl->Pmp()) CMDIdScCmp(left_mdid, right_mdid, cmp_type);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumVal
//
//	@doc:
//		Parses a DXL datum from the given attributes
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumVal(CDXLMemoryManager *memory_manager_dxl,
								 const Attributes &attrs,
								 Edxltoken target_elem)
{
	// get the type id and m_bytearray_value of the datum from attributes
	IMDId *mdid =
		ExtractConvertAttrValueToMdId(memory_manager_dxl, attrs, EdxltokenTypeId, target_elem);
	GPOS_ASSERT(IMDId::EmdidGPDB == mdid->MdidType());
	CMDIdGPDB *gpdb_mdid = CMDIdGPDB::CastMdid(mdid);

	// get the type id from string
	BOOL is_const_null =
		ExtractConvertAttrValueToBool(memory_manager_dxl, attrs, EdxltokenIsNull, target_elem);
	BOOL is_const_by_val =
		ExtractConvertAttrValueToBool(memory_manager_dxl, attrs, EdxltokenIsByValue, target_elem);


	SDXLDatumFactoryElem translators_mapping[] = {
		// native support
		{CMDIdGPDB::m_mdid_int2.OidObjectId(), &CDXLOperatorFactory::GetDatumInt2},
		{CMDIdGPDB::m_mdid_int4.OidObjectId(), &CDXLOperatorFactory::GetDatumInt4},
		{CMDIdGPDB::m_mdid_int8.OidObjectId(), &CDXLOperatorFactory::GetDatumInt8},
		{CMDIdGPDB::m_mdid_bool.OidObjectId(), &CDXLOperatorFactory::GetDatumBool},
		{CMDIdGPDB::m_mdid_oid.OidObjectId(), &CDXLOperatorFactory::GetDatumOid},
		// types with long int mapping
		{CMDIdGPDB::m_mdid_bpchar.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsLintMappable},
		{CMDIdGPDB::m_mdid_varchar.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsLintMappable},
		{CMDIdGPDB::m_mdid_text.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsLintMappable},
		{CMDIdGPDB::m_mdid_cash.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsLintMappable},
		// non-integer numeric types
		{CMDIdGPDB::m_mdid_numeric.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_float4.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_float8.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		// network-related types
		{CMDIdGPDB::m_mdid_inet.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_cidr.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_macaddr.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		// time-related types
		{CMDIdGPDB::m_mdid_date.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_time.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_timeTz.OidObjectId(), &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_timestamp.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_timestampTz.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_abs_time.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_relative_time.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_interval.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdid_time_interval.OidObjectId(),
		 &CDXLOperatorFactory::GetDatumStatsDoubleMappable}};

	const ULONG translators_mapping_len = GPOS_ARRAY_SIZE(translators_mapping);
	// find translator for the datum type
	PfPdxldatum *func = NULL;
	for (ULONG ul = 0; ul < translators_mapping_len; ul++)
	{
		SDXLDatumFactoryElem elem = translators_mapping[ul];
		if (gpdb_mdid->OidObjectId() == elem.oid)
		{
			func = elem.pf;
			break;
		}
	}

	if (NULL == func)
	{
		// generate a datum of generic type
		return GetDatumGeneric(
			memory_manager_dxl, attrs, target_elem, mdid, is_const_null, is_const_by_val);
	}
	else
	{
		return (
			*func)(memory_manager_dxl, attrs, target_elem, mdid, is_const_null, is_const_by_val);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumOid
//
//	@doc:
//		Parses a DXL datum of oid type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumOid(CDXLMemoryManager *memory_manager_dxl,
								 const Attributes &attrs,
								 Edxltoken target_elem,
								 IMDId *mdid,
								 BOOL is_const_null,
								 BOOL
#ifdef GPOS_DEBUG
									 is_const_by_val
#endif  // GPOS_DEBUG
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	GPOS_ASSERT(is_const_by_val);
	OID val = 0;
	if (!is_const_null)
	{
		val = ExtractConvertAttrValueToOid(memory_manager_dxl, attrs, EdxltokenValue, target_elem);
	}

	return GPOS_NEW(memory_pool) CDXLDatumOid(memory_pool, mdid, is_const_null, val);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumInt2
//
//	@doc:
//		Parses a DXL datum of int2 type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumInt2(CDXLMemoryManager *memory_manager_dxl,
								  const Attributes &attrs,
								  Edxltoken target_elem,
								  IMDId *mdid,
								  BOOL is_const_null,
								  BOOL
#ifdef GPOS_DEBUG
									  is_const_by_val
#endif  // GPOS_DEBUG
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	GPOS_ASSERT(is_const_by_val);
	SINT val = 0;
	if (!is_const_null)
	{
		val = ExtractConvertAttrValueToShortInt(
			memory_manager_dxl, attrs, EdxltokenValue, target_elem);
	}

	return GPOS_NEW(memory_pool) CDXLDatumInt2(memory_pool, mdid, is_const_null, val);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumInt4
//
//	@doc:
//		Parses a DXL datum of int4 type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumInt4(CDXLMemoryManager *memory_manager_dxl,
								  const Attributes &attrs,
								  Edxltoken target_elem,
								  IMDId *mdid,
								  BOOL is_const_null,
								  BOOL
#ifdef GPOS_DEBUG
									  is_const_by_val
#endif  // GPOS_DEBUG
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	GPOS_ASSERT(is_const_by_val);
	INT val = 0;
	if (!is_const_null)
	{
		val = ExtractConvertAttrValueToInt(memory_manager_dxl, attrs, EdxltokenValue, target_elem);
	}

	return GPOS_NEW(memory_pool) CDXLDatumInt4(memory_pool, mdid, is_const_null, val);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumInt8
//
//	@doc:
//		Parses a DXL datum of int8 type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumInt8(CDXLMemoryManager *memory_manager_dxl,
								  const Attributes &attrs,
								  Edxltoken target_elem,
								  IMDId *mdid,
								  BOOL is_const_null,
								  BOOL
#ifdef GPOS_DEBUG
									  is_const_by_val
#endif  // GPOS_DEBUG
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	GPOS_ASSERT(is_const_by_val);
	LINT val = 0;
	if (!is_const_null)
	{
		val = ExtractConvertAttrValueToLint(memory_manager_dxl, attrs, EdxltokenValue, target_elem);
	}

	return GPOS_NEW(memory_pool) CDXLDatumInt8(memory_pool, mdid, is_const_null, val);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumBool
//
//	@doc:
//		Parses a DXL datum of boolean type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumBool(CDXLMemoryManager *memory_manager_dxl,
								  const Attributes &attrs,
								  Edxltoken target_elem,
								  IMDId *mdid,
								  BOOL is_const_null,
								  BOOL
#ifdef GPOS_DEBUG
									  is_const_by_val
#endif  // GPOS_DEBUG
)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	GPOS_ASSERT(is_const_by_val);
	BOOL value = false;
	if (!is_const_null)
	{
		value =
			ExtractConvertAttrValueToBool(memory_manager_dxl, attrs, EdxltokenValue, target_elem);
	}

	return GPOS_NEW(memory_pool) CDXLDatumBool(memory_pool, mdid, is_const_null, value);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumGeneric
//
//	@doc:
//		Parses a DXL datum of generic type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumGeneric(CDXLMemoryManager *memory_manager_dxl,
									 const Attributes &attrs,
									 Edxltoken target_elem,
									 IMDId *mdid,
									 BOOL is_const_null,
									 BOOL is_const_by_val)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	ULONG len = 0;
	BYTE *data = NULL;

	if (!is_const_null)
	{
		data = GetByteArray(memory_manager_dxl, attrs, target_elem, &len);
		if (NULL == data)
		{
			// unable to decode m_bytearray_value. probably not Base64 encoded.
			GPOS_RAISE(gpdxl::ExmaDXL,
					   gpdxl::ExmiDXLInvalidAttributeValue,
					   CDXLTokens::XmlstrToken(EdxltokenValue),
					   CDXLTokens::GetDXLTokenStr(target_elem));
		}
	}

	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenScalarCast,
													 true,
													 default_type_modifier);

	return GPOS_NEW(memory_pool) CDXLDatumGeneric(
		memory_pool, mdid, type_modifier, is_const_by_val, is_const_null, data, len);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumStatsLintMappable
//
//	@doc:
//		Parses a DXL datum of types having lint mapping
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumStatsLintMappable(CDXLMemoryManager *memory_manager_dxl,
											   const Attributes &attrs,
											   Edxltoken target_elem,
											   IMDId *mdid,
											   BOOL is_const_null,
											   BOOL is_const_by_val)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	ULONG len = 0;
	BYTE *data = NULL;

	LINT value = 0;
	if (!is_const_null)
	{
		data = GetByteArray(memory_manager_dxl, attrs, target_elem, &len);
		value = Value(memory_manager_dxl, attrs, target_elem, data);
	}

	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenScalarCast,
													 true,
													 -1 /* default_val m_bytearray_value */
	);

	return GPOS_NEW(memory_pool) CDXLDatumStatsLintMappable(
		memory_pool, mdid, type_modifier, is_const_by_val, is_const_null, data, len, value);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Value
//
//	@doc:
//		Return the LINT m_bytearray_value of byte array
//
//---------------------------------------------------------------------------
LINT
CDXLOperatorFactory::Value(CDXLMemoryManager *memory_manager_dxl,
						   const Attributes &attrs,
						   Edxltoken target_elem,
						   BYTE *data)
{
	if (NULL == data)
	{
		// unable to decode m_bytearray_value. probably not Base64 encoded.
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::XmlstrToken(EdxltokenValue),
				   CDXLTokens::GetDXLTokenStr(target_elem));
	}

	return ExtractConvertAttrValueToLint(
		memory_manager_dxl, attrs, EdxltokenLintValue, target_elem);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetByteArray
//
//	@doc:
//		Parses a byte array representation of the datum
//
//---------------------------------------------------------------------------
BYTE *
CDXLOperatorFactory::GetByteArray(CDXLMemoryManager *memory_manager_dxl,
								  const Attributes &attrs,
								  Edxltoken target_elem,
								  ULONG *length)
{
	const XMLCh *attr_val_xml = ExtractAttrValue(attrs, EdxltokenValue, target_elem);

	return CDXLUtils::CreateStringFrom64XMLStr(memory_manager_dxl, attr_val_xml, length);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::GetDatumStatsDoubleMappable
//
//	@doc:
//		Parses a DXL datum of types that need double mapping
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::GetDatumStatsDoubleMappable(CDXLMemoryManager *memory_manager_dxl,
												 const Attributes &attrs,
												 Edxltoken target_elem,
												 IMDId *mdid,
												 BOOL is_const_null,
												 BOOL is_const_by_val)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	ULONG len = 0;
	BYTE *data = NULL;
	CDouble value = 0;

	if (!is_const_null)
	{
		data = GetByteArray(memory_manager_dxl, attrs, target_elem, &len);

		if (NULL == data)
		{
			// unable to decode m_bytearray_value. probably not Base64 encoded.
			GPOS_RAISE(gpdxl::ExmaDXL,
					   gpdxl::ExmiDXLInvalidAttributeValue,
					   CDXLTokens::XmlstrToken(EdxltokenValue),
					   CDXLTokens::GetDXLTokenStr(target_elem));
		}

		value = ExtractConvertAttrValueToDouble(
			memory_manager_dxl, attrs, EdxltokenDoubleValue, target_elem);
	}
	INT type_modifier = ExtractConvertAttrValueToInt(memory_manager_dxl,
													 attrs,
													 EdxltokenTypeMod,
													 EdxltokenScalarCast,
													 true,
													 -1 /* default_val m_bytearray_value */
	);
	return GPOS_NEW(memory_pool) CDXLDatumStatsDoubleMappable(
		memory_pool, mdid, type_modifier, is_const_by_val, is_const_null, data, len, value);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertValuesToArray
//
//	@doc:
//		Parse a comma-separated list of unsigned long integers ids into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
ULongPtrArray *
CDXLOperatorFactory::ExtractConvertValuesToArray(CDXLMemoryManager *memory_manager_dxl,
												 const Attributes &attrs,
												 Edxltoken target_attr,
												 Edxltoken target_elem)
{
	const XMLCh *xml_val = CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem);

	return ExtractIntsToUlongArray(memory_manager_dxl, xml_val, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertMdIdsToArray
//
//	@doc:
//		Parse a comma-separated list of MDids into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
MdidPtrArray *
CDXLOperatorFactory::ExtractConvertMdIdsToArray(CDXLMemoryManager *memory_manager_dxl,
												const XMLCh *mdid_list_xml,
												Edxltoken target_attr,
												Edxltoken target_elem)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	MdidPtrArray *mdid_array = GPOS_NEW(memory_pool) MdidPtrArray(memory_pool);

	XMLStringTokenizer mdid_components(mdid_list_xml, CDXLTokens::XmlstrToken(EdxltokenComma));
	const ULONG num_tokens = mdid_components.countTokens();

	for (ULONG ul = 0; ul < num_tokens; ul++)
	{
		XMLCh *mdid_xml = mdid_components.nextToken();
		GPOS_ASSERT(NULL != mdid_xml);

		IMDId *mdid = MakeMdIdFromStr(memory_manager_dxl, mdid_xml, target_attr, target_elem);
		mdid_array->Append(mdid);
	}

	return mdid_array;
}

// Parse a comma-separated list of CHAR partition types into a dynamic array.
// Will raise an exception if list is not well-formed
CharPtrArray *
CDXLOperatorFactory::ExtractConvertPartitionTypeToArray(CDXLMemoryManager *memory_manager_dxl,
														const XMLCh *xml_val,
														Edxltoken target_attr,
														Edxltoken target_elem)
{
	return ExtractIntsToArray<CHAR, CleanupDelete, ConvertAttrValueToChar>(
		memory_manager_dxl, xml_val, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertUlongTo2DArray
//
//	@doc:
//		Parse a semicolon-separated list of comma-separated unsigned long
//		integers into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
ULongPtrArray2D *
CDXLOperatorFactory::ExtractConvertUlongTo2DArray(CDXLMemoryManager *memory_manager_dxl,
												  const XMLCh *xml_val,
												  Edxltoken target_attr,
												  Edxltoken target_elem)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	ULongPtrArray2D *array_2D = GPOS_NEW(memory_pool) ULongPtrArray2D(memory_pool);

	XMLStringTokenizer mdid_components(xml_val, CDXLTokens::XmlstrToken(EdxltokenSemicolon));
	const ULONG num_tokens = mdid_components.countTokens();

	for (ULONG ul = 0; ul < num_tokens; ul++)
	{
		XMLCh *comp_xml = mdid_components.nextToken();

		GPOS_ASSERT(NULL != comp_xml);

		ULongPtrArray *array_1D =
			ExtractIntsToUlongArray(memory_manager_dxl, comp_xml, target_attr, target_elem);
		array_2D->Append(array_1D);
	}

	return array_2D;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertSegmentIdsToArray
//
//	@doc:
//		Parse a comma-separated list of segment ids into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
IntPtrArray *
CDXLOperatorFactory::ExtractConvertSegmentIdsToArray(CDXLMemoryManager *memory_manager_dxl,
													 const XMLCh *seg_id_list_xml,
													 Edxltoken target_attr,
													 Edxltoken target_elem)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	GPOS_ASSERT(NULL != seg_id_list_xml);

	IntPtrArray *seg_ids = GPOS_NEW(memory_pool) IntPtrArray(memory_pool);

	XMLStringTokenizer mdid_components(seg_id_list_xml, CDXLTokens::XmlstrToken(EdxltokenComma));

	const ULONG num_of_segments = mdid_components.countTokens();
	GPOS_ASSERT(0 < num_of_segments);

	for (ULONG ul = 0; ul < num_of_segments; ul++)
	{
		XMLCh *seg_id_xml = mdid_components.nextToken();

		GPOS_ASSERT(NULL != seg_id_xml);

		INT *seg_id = GPOS_NEW(memory_pool)
			INT(ConvertAttrValueToInt(memory_manager_dxl, seg_id_xml, target_attr, target_elem));
		seg_ids->Append(seg_id);
	}

	return seg_ids;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertStrsToArray
//
//	@doc:
//		Parse a semicolon-separated list of strings into a dynamic array.
//
//---------------------------------------------------------------------------
StringPtrArray *
CDXLOperatorFactory::ExtractConvertStrsToArray(CDXLMemoryManager *memory_manager_dxl,
											   const XMLCh *xml_val)
{
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	StringPtrArray *array_strs = GPOS_NEW(memory_pool) StringPtrArray(memory_pool);

	XMLStringTokenizer mdid_components(xml_val, CDXLTokens::XmlstrToken(EdxltokenSemicolon));
	const ULONG num_tokens = mdid_components.countTokens();

	for (ULONG ul = 0; ul < num_tokens; ul++)
	{
		XMLCh *current_str = mdid_components.nextToken();
		GPOS_ASSERT(NULL != current_str);

		CWStringDynamic *str =
			CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, current_str);
		array_strs->Append(str);
	}

	return array_strs;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::SetSegmentInfo
//
//	@doc:
//		Parses the input and output segment ids from Xerces attributes and
//		stores them in the provided DXL Motion operator.
//		Will raise an exception if lists are not well-formed
//
//---------------------------------------------------------------------------
void
CDXLOperatorFactory::SetSegmentInfo(CDXLMemoryManager *memory_manager_dxl,
									CDXLPhysicalMotion *motion,
									const Attributes &attrs,
									Edxltoken target_elem)
{
	const XMLCh *input_seglist_xml = ExtractAttrValue(attrs, EdxltokenInputSegments, target_elem);
	IntPtrArray *input_segments = ExtractConvertSegmentIdsToArray(
		memory_manager_dxl, input_seglist_xml, EdxltokenInputSegments, target_elem);
	motion->SetInputSegIds(input_segments);

	const XMLCh *output_seglist_xml = ExtractAttrValue(attrs, EdxltokenOutputSegments, target_elem);
	IntPtrArray *output_segments = ExtractConvertSegmentIdsToArray(
		memory_manager_dxl, output_seglist_xml, EdxltokenOutputSegments, target_elem);
	motion->SetOutputSegIds(output_segments);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseJoinType
//
//	@doc:
//		Parse a join type from the attribute m_bytearray_value.
//		Raise an exception if join type m_bytearray_value is invalid.
//
//---------------------------------------------------------------------------
EdxlJoinType
CDXLOperatorFactory::ParseJoinType(const XMLCh *join_type_xml, const CWStringConst *join_name)
{
	EdxlJoinType join_type = EdxljtSentinel;

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinInner), join_type_xml))
	{
		join_type = EdxljtInner;
	}
	else if (0 ==
			 XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinLeft), join_type_xml))
	{
		join_type = EdxljtLeft;
	}
	else if (0 ==
			 XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinFull), join_type_xml))
	{
		join_type = EdxljtFull;
	}
	else if (0 ==
			 XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinRight), join_type_xml))
	{
		join_type = EdxljtRight;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinIn), join_type_xml))
	{
		join_type = EdxljtIn;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinLeftAntiSemiJoin),
										   join_type_xml))
	{
		join_type = EdxljtLeftAntiSemijoin;
	}
	else if (0 == XMLString::compareString(
					  CDXLTokens::XmlstrToken(EdxltokenJoinLeftAntiSemiJoinNotIn), join_type_xml))
	{
		join_type = EdxljtLeftAntiSemijoinNotIn;
	}
	else
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenJoinType)->GetBuffer(),
				   join_name->GetBuffer());
	}

	return join_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseIndexScanDirection
//
//	@doc:
//		Parse the index scan direction from the attribute m_bytearray_value. Raise
//		exception if it is invalid
//
//---------------------------------------------------------------------------
EdxlIndexScanDirection
CDXLOperatorFactory::ParseIndexScanDirection(const XMLCh *direction_xml,
											 const CWStringConst *index_scan)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexScanDirectionBackward),
									  direction_xml))
	{
		return EdxlisdBackward;
	}
	else if (0 == XMLString::compareString(
					  CDXLTokens::XmlstrToken(EdxltokenIndexScanDirectionForward), direction_xml))
	{
		return EdxlisdForward;
	}
	else if (0 ==
			 XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenIndexScanDirectionNoMovement), direction_xml))
	{
		return EdxlisdNoMovement;
	}
	else
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenIndexScanDirection)->GetBuffer(),
				   index_scan->GetBuffer());
	}

	return EdxlisdSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeLogicalJoin
//
//	@doc:
//		Construct a logical join operator
//
//---------------------------------------------------------------------------
CDXLLogical *
CDXLOperatorFactory::MakeLogicalJoin(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();

	const XMLCh *join_type_xml = ExtractAttrValue(attrs, EdxltokenJoinType, EdxltokenLogicalJoin);
	EdxlJoinType join_type =
		ParseJoinType(join_type_xml, CDXLTokens::GetDXLTokenStr(EdxltokenLogicalJoin));

	return GPOS_NEW(memory_pool) CDXLLogicalJoin(memory_pool, join_type);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToDouble
//
//	@doc:
//	  Converts the attribute m_bytearray_value to CDouble
//
//---------------------------------------------------------------------------
CDouble
CDXLOperatorFactory::ConvertAttrValueToDouble(CDXLMemoryManager *memory_manager_dxl,
											  const XMLCh *attribute_val_xml,
											  Edxltoken,  //target_attr,
											  Edxltoken   //target_elem
)
{
	GPOS_ASSERT(attribute_val_xml != NULL);
	CHAR *sz = XMLString::transcode(attribute_val_xml, memory_manager_dxl);

	CDouble value(clib::Strtod(sz));

	XMLString::release(&sz, memory_manager_dxl);
	return value;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToDouble
//
//	@doc:
//	  Extracts the m_bytearray_value for the given attribute and converts it into CDouble
//
//---------------------------------------------------------------------------
CDouble
CDXLOperatorFactory::ExtractConvertAttrValueToDouble(CDXLMemoryManager *memory_manager_dxl,
													 const Attributes &attrs,
													 Edxltoken target_attr,
													 Edxltoken target_elem)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem);
	return ConvertAttrValueToDouble(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ConvertAttrValueToLint
//
//	@doc:
//	  Converts the attribute m_bytearray_value to LINT
//
//---------------------------------------------------------------------------
LINT
CDXLOperatorFactory::ConvertAttrValueToLint(CDXLMemoryManager *memory_manager_dxl,
											const XMLCh *attribute_val_xml,
											Edxltoken,  //target_attr,
											Edxltoken   //target_elem
)
{
	GPOS_ASSERT(NULL != attribute_val_xml);
	CHAR *sz = XMLString::transcode(attribute_val_xml, memory_manager_dxl);
	CHAR *szEnd = NULL;

	LINT value = clib::Strtoll(sz, &szEnd, 10);
	XMLString::release(&sz, memory_manager_dxl);

	return value;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ExtractConvertAttrValueToLint
//
//	@doc:
//	  Extracts the m_bytearray_value for the given attribute and converts it into LINT
//
//---------------------------------------------------------------------------
LINT
CDXLOperatorFactory::ExtractConvertAttrValueToLint(CDXLMemoryManager *memory_manager_dxl,
												   const Attributes &attrs,
												   Edxltoken target_attr,
												   Edxltoken target_elem,
												   BOOL is_optional,
												   LINT default_value)
{
	const XMLCh *attr_val_xml =
		CDXLOperatorFactory::ExtractAttrValue(attrs, target_attr, target_elem, is_optional);

	if (NULL == attr_val_xml)
	{
		return default_value;
	}

	return ConvertAttrValueToLint(memory_manager_dxl, attr_val_xml, target_attr, target_elem);
}


CSystemId
CDXLOperatorFactory::Sysid(CDXLMemoryManager *memory_manager_dxl,
						   const Attributes &attrs,
						   Edxltoken target_attr,
						   Edxltoken target_elem)
{
	// extract systemids
	const XMLCh *xml_val = ExtractAttrValue(attrs, target_attr, target_elem);

	// get sysid components
	XMLStringTokenizer sys_id_components(xml_val, CDXLTokens::XmlstrToken(EdxltokenDot));
	GPOS_ASSERT(2 == sys_id_components.countTokens());

	XMLCh *sys_id_comp = sys_id_components.nextToken();
	ULONG type = CDXLOperatorFactory::ConvertAttrValueToUlong(
		memory_manager_dxl, sys_id_comp, target_attr, target_elem);

	XMLCh *xml_str_name = sys_id_components.nextToken();
	CWStringDynamic *str_name =
		CDXLUtils::CreateDynamicStringFromXMLChArray(memory_manager_dxl, xml_str_name);

	CSystemId sys_id((IMDId::EMDIdType) type, str_name->GetBuffer(), str_name->Length());
	GPOS_DELETE(str_name);

	return sys_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::MakeWindowRef
//
//	@doc:
//		Construct an WindowRef operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::MakeWindowRef(CDXLMemoryManager *memory_manager_dxl, const Attributes &attrs)
{
	// get the memory pool from the memory manager
	IMemoryPool *memory_pool = memory_manager_dxl->Pmp();
	IMDId *mdid_func = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenWindowrefOid, EdxltokenScalarWindowref);
	IMDId *mdid_return_type = ExtractConvertAttrValueToMdId(
		memory_manager_dxl, attrs, EdxltokenTypeId, EdxltokenScalarWindowref);
	BOOL is_distinct = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenWindowrefDistinct, EdxltokenScalarWindowref);
	BOOL is_star_arg = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenWindowrefStarArg, EdxltokenScalarWindowref);
	BOOL is_simple_agg = ExtractConvertAttrValueToBool(
		memory_manager_dxl, attrs, EdxltokenWindowrefSimpleAgg, EdxltokenScalarWindowref);
	ULONG win_spec_pos = ExtractConvertAttrValueToUlong(
		memory_manager_dxl, attrs, EdxltokenWindowrefWinSpecPos, EdxltokenScalarWindowref);

	const XMLCh *agg_stage_xml =
		ExtractAttrValue(attrs, EdxltokenWindowrefStrategy, EdxltokenScalarWindowref);
	EdxlWinStage dxl_win_stage = EdxlwinstageSentinel;

	ULONG window_frame_boundary_to_frame_boundary_mapping[][2] = {
		{EdxlwinstageImmediate, EdxltokenWindowrefStageImmediate},
		{EdxlwinstagePreliminary, EdxltokenWindowrefStagePreliminary},
		{EdxlwinstageRowKey, EdxltokenWindowrefStageRowKey}};

	const ULONG arity = GPOS_ARRAY_SIZE(window_frame_boundary_to_frame_boundary_mapping);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ULONG *mapping = window_frame_boundary_to_frame_boundary_mapping[ul];
		Edxltoken frame_bound = (Edxltoken) mapping[1];
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(frame_bound), agg_stage_xml))
		{
			dxl_win_stage = (EdxlWinStage) mapping[0];
			break;
		}
	}
	GPOS_ASSERT(EdxlwinstageSentinel != dxl_win_stage);

	return GPOS_NEW(memory_pool) CDXLScalarWindowRef(memory_pool,
													 mdid_func,
													 mdid_return_type,
													 is_distinct,
													 is_star_arg,
													 is_simple_agg,
													 dxl_win_stage,
													 win_spec_pos);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseCmpType
//
//	@doc:
//		Parse comparison type
//
//---------------------------------------------------------------------------
IMDType::ECmpType
CDXLOperatorFactory::ParseCmpType(const XMLCh *xml_str_comp_type)
{
	ULONG parse_cmp_type_mapping[][2] = {{EdxltokenCmpEq, IMDType::EcmptEq},
										 {EdxltokenCmpNeq, IMDType::EcmptNEq},
										 {EdxltokenCmpLt, IMDType::EcmptL},
										 {EdxltokenCmpLeq, IMDType::EcmptLEq},
										 {EdxltokenCmpGt, IMDType::EcmptG},
										 {EdxltokenCmpGeq, IMDType::EcmptGEq},
										 {EdxltokenCmpIDF, IMDType::EcmptIDF},
										 {EdxltokenCmpOther, IMDType::EcmptOther}};

	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(parse_cmp_type_mapping); ul++)
	{
		ULONG *mapping = parse_cmp_type_mapping[ul];
		Edxltoken cmp_type = (Edxltoken) mapping[0];

		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(cmp_type), xml_str_comp_type))
		{
			return (IMDType::ECmpType) mapping[1];
		}
	}

	GPOS_RAISE(gpdxl::ExmaDXL,
			   gpdxl::ExmiDXLInvalidAttributeValue,
			   CDXLTokens::GetDXLTokenStr(EdxltokenGPDBScalarOpCmpType)->GetBuffer(),
			   CDXLTokens::GetDXLTokenStr(EdxltokenGPDBScalarOp)->GetBuffer());
	return (IMDType::ECmpType) IMDType::EcmptOther;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseRelationDistPolicy
//
//	@doc:
//		Parse relation distribution policy from XML string
//
//---------------------------------------------------------------------------
IMDRelation::Ereldistrpolicy
CDXLOperatorFactory::ParseRelationDistPolicy(const XMLCh *xml_val)
{
	GPOS_ASSERT(NULL != xml_val);
	IMDRelation::Ereldistrpolicy rel_distr_policy = IMDRelation::EreldistrSentinel;

	if (0 ==
		XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenRelDistrMasterOnly)))
	{
		rel_distr_policy = IMDRelation::EreldistrMasterOnly;
	}
	else if (0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenRelDistrHash)))
	{
		rel_distr_policy = IMDRelation::EreldistrHash;
	}
	else if (0 ==
			 XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenRelDistrRandom)))
	{
		rel_distr_policy = IMDRelation::EreldistrRandom;
	}

	return rel_distr_policy;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseRelationStorageType
//
//	@doc:
//		Parse relation storage type from XML string
//
//---------------------------------------------------------------------------
IMDRelation::Erelstoragetype
CDXLOperatorFactory::ParseRelationStorageType(const XMLCh *xml_val)
{
	GPOS_ASSERT(NULL != xml_val);

	if (0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenRelStorageHeap)))
	{
		return IMDRelation::ErelstorageHeap;
	}

	if (0 == XMLString::compareString(xml_val,
									  CDXLTokens::XmlstrToken(EdxltokenRelStorageAppendOnlyCols)))
	{
		return IMDRelation::ErelstorageAppendOnlyCols;
	}

	if (0 == XMLString::compareString(xml_val,
									  CDXLTokens::XmlstrToken(EdxltokenRelStorageAppendOnlyRows)))
	{
		return IMDRelation::ErelstorageAppendOnlyRows;
	}

	if (0 == XMLString::compareString(
				 xml_val, CDXLTokens::XmlstrToken(EdxltokenRelStorageAppendOnlyParquet)))
	{
		return IMDRelation::ErelstorageAppendOnlyParquet;
	}

	if (0 ==
		XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenRelStorageExternal)))
	{
		return IMDRelation::ErelstorageExternal;
	}

	if (0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenRelStorageVirtual)))
	{
		return IMDRelation::ErelstorageVirtual;
	}

	GPOS_ASSERT(!"Unrecognized storage type");

	return IMDRelation::ErelstorageSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseOnCommitActionSpec
//
//	@doc:
//		Parse on commit action spec from XML attributes
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::ECtasOnCommitAction
CDXLOperatorFactory::ParseOnCommitActionSpec(const Attributes &attrs)
{
	const XMLCh *xml_val = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenOnCommitAction));

	if (NULL == xml_val)
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLMissingAttribute,
				   CDXLTokens::GetDXLTokenStr(EdxltokenOnCommitAction)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenCTASOptions)->GetBuffer());
	}

	if (0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenOnCommitPreserve)))
	{
		return CDXLCtasStorageOptions::EctascommitPreserve;
	}

	if (0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenOnCommitDelete)))
	{
		return CDXLCtasStorageOptions::EctascommitDelete;
	}

	if (0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenOnCommitDrop)))
	{
		return CDXLCtasStorageOptions::EctascommitDrop;
	}

	if (0 != XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenOnCommitNOOP)))
	{
		GPOS_RAISE(gpdxl::ExmaDXL,
				   gpdxl::ExmiDXLInvalidAttributeValue,
				   CDXLTokens::GetDXLTokenStr(EdxltokenOnCommitAction)->GetBuffer(),
				   CDXLTokens::GetDXLTokenStr(EdxltokenCTASOptions)->GetBuffer());
	}

	return CDXLCtasStorageOptions::EctascommitNOOP;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ParseIndexType
//
//	@doc:
//		Parse index type from XML attributes
//
//---------------------------------------------------------------------------
IMDIndex::EmdindexType
CDXLOperatorFactory::ParseIndexType(const Attributes &attrs)
{
	const XMLCh *xml_val = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenIndexType));

	if (NULL == xml_val ||
		0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenIndexTypeBtree)))
	{
		// default_val is btree
		return IMDIndex::EmdindBtree;
	}


	if (0 == XMLString::compareString(xml_val, CDXLTokens::XmlstrToken(EdxltokenIndexTypeBitmap)))
	{
		return IMDIndex::EmdindBitmap;
	}

	GPOS_RAISE(gpdxl::ExmaDXL,
			   gpdxl::ExmiDXLInvalidAttributeValue,
			   CDXLTokens::GetDXLTokenStr(EdxltokenIndexType)->GetBuffer(),
			   CDXLTokens::GetDXLTokenStr(EdxltokenIndex)->GetBuffer());

	return IMDIndex::EmdindSentinel;
}

// EOF
