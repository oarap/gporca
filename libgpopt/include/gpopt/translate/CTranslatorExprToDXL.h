//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorExprToDXL.h
//
//	@doc:
//		Class providing methods for translating optimizer Expression trees 
//		into DXL Tree.
//---------------------------------------------------------------------------

#ifndef GPOPT_CTranslatorExprToDXL_H
#define GPOPT_CTranslatorExprToDXL_H


#include "gpos/base.h"
#include "gpos/memory/IMemoryPool.h"

#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"
#include "naucrates/dxl/operators/CDXLScalarWindowRef.h"
#include "naucrates/dxl/operators/CDXLColRef.h"
#include "naucrates/dxl/operators/CDXLPhysicalDML.h"
#include "naucrates/dxl/operators/CDXLPhysicalAgg.h"
#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"
#include "naucrates/dxl/operators/CDXLScalarArrayRefIndexList.h"
#include "naucrates/dxl/operators/CDXLScalarSubPlan.h"

#include "gpopt/operators/ops.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/metadata/CTableDescriptor.h"

// fwd declaration
namespace gpnaucrates
{
	class IStatistics;
}

// forward declarations
namespace gpdxl
{
	class CDXLTableDescr;
	class CDXLPhysicalProperties;
	class CDXLWindowFrame;
}

namespace gpopt
{	
	using namespace gpmd;
	using namespace gpdxl;
	using namespace gpnaucrates;

	// hash map mapping CColRef -> CDXLNode
	typedef CHashMap<CColRef, CDXLNode, gpos::HashValue<CColRef>, gpos::Equals<CColRef>,
					CleanupNULL<CColRef>, CleanupRelease<CDXLNode> > ColRefToDXLNodeMap;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorExprToDXL
	//
	//	@doc:
	//		Class providing methods for translating optimizer Expression trees
	//		into DXL Trees.
	//
	//---------------------------------------------------------------------------
	class CTranslatorExprToDXL
	{
		private:
		
			// shorthand for functions for translating scalar expressions
			typedef CDXLNode * (CTranslatorExprToDXL::*PfPdxlnScalar)(CExpression *pexpr);
							
			// shorthand for functions for translating physical expressions
			typedef CDXLNode * (CTranslatorExprToDXL::*PfPdxlnPhysical)
						(
						CExpression *pexpr, 
						ColRefArray *colref_array, 
						DrgPds *pdrgpdsBaseTables, 	// output: array of base table hash distributions
						ULONG *pulNonGatherMotions, // output: number of non-Gather motion nodes
						BOOL *pfDML					// output: is this a DML operation
						);

			// pair of scalar operator type and the corresponding translator
			struct SScTranslatorMapping
			{
				// type
				COperator::EOperatorId op_id;
				
				// translator function pointer
				PfPdxlnScalar pf;
			};
			
			// pair of physical operator type and the corresponding translator
			struct SPhTranslatorMapping
			{
				// type
				COperator::EOperatorId op_id;
				
				// translator function pointer
				PfPdxlnPhysical pf;
			};

			// memory pool
			IMemoryPool *m_mp;
			
			// metadata accessor
			CMDAccessor *m_pmda;
			
			// mappings CColRef -> CDXLNode used to lookup subplans
			ColRefToDXLNodeMap *m_phmcrdxln;
			
			// mappings CColRef -> CDXLNode used to for index predicates with outer references
			ColRefToDXLNodeMap *m_phmcrdxlnIndexLookup;

			// derived plan properties of the translated expression
			CDrvdPropPlan *m_pdpplan;

			// a copy of the pointer to column factory, obtained at construction time
			CColumnFactory *m_pcf;

			// segment ids on target system
			IntPtrArray *m_pdrgpiSegments;
			
			// id of master node
			INT m_iMasterId;

			// scalar expression translators indexed by the operator id
			PfPdxlnScalar m_rgpfScalarTranslators[COperator::EopSentinel];
			
			// physical expression translators indexed by the operator id
			PfPdxlnPhysical m_rgpfPhysicalTranslators[COperator::EopSentinel];

			// private copy ctor
			CTranslatorExprToDXL(const CTranslatorExprToDXL&);

			// initialize index of scalar translators
			void InitScalarTranslators();

			// initialize index of physical translators
			void InitPhysicalTranslators();
			
			EdxlBoolExprType Edxlbooltype(const CScalarBoolOp::EBoolOperator eboolop) const;

			// return the EdxlDmlType for a given DML op type
			EdxlDmlType Edxldmloptype(const CLogicalDML::EDMLOperator edmlop) const;

			// return outer refs in correlated join inner child
			CColRefSet * PcrsOuterRefsForCorrelatedNLJoin(CExpression *pexpr) const;

			// functions translating different optimizer expressions into their
			// DXL counterparts
			
			CDXLNode *PdxlnTblScan
						(
						CExpression *pexprTblScan,
						CColRefSet *pcrsOutput,
						ColRefArray *colref_array,
						DrgPds *pdrgpdsBaseTables, 
						CExpression *pexprScalarCond,
						CDXLPhysicalProperties *dxl_properties
						);
			
			// create a (dynamic) index scan node after inlining the given scalar condition, if needed
			CDXLNode *PdxlnIndexScanWithInlinedCondition
				(
				CExpression *pexprIndexScan, 
				CExpression *pexprScalarCond, 
				CDXLPhysicalProperties *dxl_properties, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables
				);

			// translate index scan based on passed properties
			CDXLNode *PdxlnIndexScan(CExpression *pexprIndexScan, ColRefArray *colref_array, CDXLPhysicalProperties *dxl_properties, CReqdPropPlan *prpp);

			// translate index scan
			CDXLNode *PdxlnIndexScan
				(
				CExpression *pexprIndexScan, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);

			// translate a bitmap index probe expression to DXL
			CDXLNode *PdxlnBitmapIndexProbe(CExpression *pexprBitmapIndexProbe);

			// translate a bitmap bool op expression to DXL
			CDXLNode *PdxlnBitmapBoolOp(CExpression *pexprBitmapBoolOp);
			
			// translate a bitmap table scan expression to DXL
			CDXLNode *PdxlnBitmapTableScan
				(
				CExpression *pexprBitmapTableScan, 
				ColRefArray *colref_array, 	
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);

			// translate a bitmap table scan expression to DXL
			CDXLNode *PdxlnBitmapTableScan
				(
				CExpression *pexprBitmapTableScan,
				CColRefSet *pcrsOutput,
				ColRefArray *colref_array,
				DrgPds *pdrgpdsBaseTables, 
				CExpression *pexprScalar,
				CDXLPhysicalProperties *dxl_properties
				);

			// translate a partition selector into DXL while inlining the given condition in the child
			CDXLNode *PdxlnPartitionSelectorWithInlinedCondition(CExpression *pexprFilter, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// create a DXL result node from an optimizer filter node
			CDXLNode *PdxlnResultFromFilter(CExpression *pexprFilter, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnResult(CExpression *pexprFilter, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// create a DXL result node for the given project list
			CDXLNode *PdxlnResult(CDXLNode *proj_list_dxlnode, CExpression *pexprFilter, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// given a DXL plan tree child_dxlnode which represents the physical plan pexprRelational, construct a DXL
			// Result node that filters on top of it using the scalar condition pdxlnScalar
			CDXLNode *PdxlnAddScalarFilterOnRelationalChild
				(
				CDXLNode *pdxlnRelationalChild,
				CDXLNode *pdxlnScalarChild,
				CDXLPhysicalProperties *dxl_properties,
				CColRefSet *pcrsOutput,
				ColRefArray *pdrgpcrOrder
				);

			CDXLNode *PdxlnResult
				(
				CExpression *pexprFilter, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables,
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML, 
				CDXLPhysicalProperties *dxl_properties
				);

			CDXLNode *PdxlnResult
				(
				CExpression *pexprRelational, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML, 
				CDXLNode *pdxlnScalar, 
				CDXLPhysicalProperties *dxl_properties
				);

			CDXLNode *PdxlnResult
				(
				CExpression *pexprRelational, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML, 
				CDXLNode *pdxlnScalar
				);

			CDXLNode *PdxlnComputeScalar(CExpression *pexprComputeScalar, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnAggregate(CExpression *pexprHashAgg, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnAggregateDedup(CExpression *pexprAgg, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnAggregate
				(
				CExpression *pexprAgg,
				ColRefArray *colref_array,
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions,
				BOOL *pfDML,
				EdxlAggStrategy agg_strategy_dxl,
				const ColRefArray *pdrgpcrGroupingCols,
				CColRefSet *pcrsKeys
				);

			CDXLNode *PdxlnSort(CExpression *pexprSort, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnLimit(CExpression *pexprLimit, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnWindow(CExpression *pexprSeqPrj, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);
		
			CDXLNode *PdxlnNLJoin(CExpression *pexprNLJ, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);
			
			CDXLNode *PdxlnHashJoin(CExpression *pexprHJ, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnCorrelatedNLJoin(CExpression *pexprNLJ, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnCTEProducer(CExpression *pexprCTEProducer, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnCTEConsumer(CExpression *pexprCTEConsumer, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// store outer references in index NLJ inner child into global map
			void StoreIndexNLJOuterRefs(CPhysical *pop);

			// build a scalar DXL subplan node
			void BuildDxlnSubPlan
						(
						CDXLNode *pdxlnRelChild,
						const CColRef *colref,
						DXLColRefArray *dxl_colref_array
						);

			// build a boolean scalar dxl node with a subplan as its child
			CDXLNode *PdxlnBooleanScalarWithSubPlan
						(
						CDXLNode *pdxlnRelChild,
						DXLColRefArray *dxl_colref_array
						);

			CDXLNode *PdxlnScBoolExpr(EdxlBoolExprType boolexptype,	CDXLNode *dxlnode_left, CDXLNode *dxlnode_right);

			CDXLNode *PdxlnTblScanFromNLJoinOuter
						(
						CExpression *pexprRelational,
						CDXLNode *pdxlnScalar,
						ColRefArray *colref_array,
						DrgPds *pdrgpdsBaseTables, 
						ULONG *pulNonGatherMotions,
						CDXLPhysicalProperties *dxl_properties
						);

			CDXLNode *PdxlnResultFromNLJoinOuter
						(
						CExpression *pexprRelational,
						CDXLNode *pdxlnScalar,
						ColRefArray *colref_array,
						DrgPds *pdrgpdsBaseTables, 
						ULONG *pulNonGatherMotions, BOOL *pfDML,
						CDXLPhysicalProperties *dxl_properties
						);

			CDXLNode *PdxlnMotion(CExpression *pexprMotion, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			CDXLNode *PdxlnMaterialize(CExpression *pexprSpool, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);
			
			// translate a sequence expression
			CDXLNode *PdxlnSequence(CExpression *pexprSequence, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);
			
			// translate a dynamic table scan
			CDXLNode *PdxlnDynamicTableScan(CExpression *pexprDTS, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate a dynamic table scan with a scalar condition
			CDXLNode *PdxlnDynamicTableScan
				(
				CExpression *pexprDTS, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				CExpression *pexprScalarCond, 
				CDXLPhysicalProperties *dxl_properties
				);

			// translate a dynamic bitmap table scan
			CDXLNode *PdxlnDynamicBitmapTableScan
				(
				CExpression *pexprDynamicBitmapTableScan, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);

			// translate a dynamic bitmap table scan
			CDXLNode *PdxlnDynamicBitmapTableScan
				(
				CExpression *pexprDynamicBitmapTableScan,
				ColRefArray *colref_array,
				DrgPds *pdrgpdsBaseTables, 
				CExpression *pexprScalar,
				CDXLPhysicalProperties *dxl_properties
				);

			// translate a dynamic index scan based on passed properties
			CDXLNode *PdxlnDynamicIndexScan(CExpression *pexprDIS, ColRefArray *colref_array, CDXLPhysicalProperties *dxl_properties, CReqdPropPlan *prpp);

			// translate a dynamic index scan
			CDXLNode *PdxlnDynamicIndexScan(CExpression *pexprDIS, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);
			
			// translate a const table get into a result node
			CDXLNode *PdxlnResultFromConstTableGet(CExpression *pexprCTG, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate a const table get into a result node
			CDXLNode *PdxlnResultFromConstTableGet(CExpression *pexprCTG, ColRefArray *colref_array, CExpression *pexprScalarCond);

			// translate a table-valued function
			CDXLNode *PdxlnTVF(CExpression *pexprTVF, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate an union all op
			CDXLNode *PdxlnAppend(CExpression *pexprUnionAll, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate a partition selector
			CDXLNode *PdxlnPartitionSelector(CExpression *pexpr, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate a partition selector
			CDXLNode *PdxlnPartitionSelector
				(
				CExpression *pexpr, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML, 
				CExpression *pexprScalarCond, 
				CDXLPhysicalProperties *dxl_properties
				);

			// translate a DML partition selector
			CDXLNode *PdxlnPartitionSelectorDML(CExpression *pexpr, ColRefArray *pdrgpc, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDMLr);

			// translate an expansion-based partition selector with a scalar condition to inline
			CDXLNode *PdxlnPartitionSelectorExpand
				(
				CExpression *pexpr, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML, 
				CExpression *pexprScalarCond, 
				CDXLPhysicalProperties *dxl_properties
				);

			// translate partition filter list
			CDXLNode *PdxlnPartFilterList(CExpression *pexpr, BOOL fEqFilters);

			// check whether the given partition selector only has equality filters
			// or no filters on all partitioning levels. return false if it has
			// non-equality filters.
			BOOL FEqPartFiltersAllLevels(CExpression *pexpr, BOOL fCheckGeneralFilters);

			// translate a filter-based partition selector
			CDXLNode *PdxlnPartitionSelectorFilter
				(
				CExpression *pexpr, 
				ColRefArray *colref_array,	
				DrgPds *pdrgpdsBaseTables,
				ULONG *pulNonGatherMotions,
				BOOL *pfDML, 
				CExpression *pexprScalarCond, 
				CDXLPhysicalProperties *dxl_properties
				);

			// translate partition selector filters
			void TranslatePartitionFilters
				(
				CExpression *pexprPartSelector, 
				BOOL fPassThrough, 
				CDXLNode **ppdxlnEqFilters, 
				CDXLNode **ppdxlnFilters, 
				CDXLNode **ppdxlnResidual
				);

			// construct the level filter lists for partition selector
			void ConstructLevelFilters4PartitionSelector(CExpression *pexprPartSelector, CDXLNode **ppdxlnEqFilters, CDXLNode **ppdxlnFilters);

			// translate a general predicate on a part key and update the various
			// comparison type flags accordingly
			CDXLNode *PdxlnPredOnPartKey
				(
				CExpression *pexprPred,
				CColRef *pcrPartKey,
				IMDId *pmdidTypePartKey,
				ULONG ulPartLevel,
				BOOL fRangePart,
				BOOL *pfLTComparison,
				BOOL *pfGTComparison,
				BOOL *pfEQComparison
				);
			
			// translate a conjunctive or disjunctive predicate on a part key and update the various
			// comparison type flags accordingly
			CDXLNode *PdxlnConjDisjOnPartKey
				(
				CExpression *pexprPred,
				CColRef *pcrPartKey,
				IMDId *pmdidTypePartKey,
				ULONG ulPartLevel,
				BOOL fRangePart,
				BOOL *pfLTComparison,
				BOOL *pfGTComparison,
				BOOL *pfEQComparison
				);
			
			// translate a scalar comparison on a part key and update the various
			// comparison type flags accordingly
			CDXLNode *PdxlnScCmpPartKey
				(
				CExpression *pexprScCmp,
				CColRef *pcrPartKey,
				IMDId *pmdidTypePartKey,
				ULONG ulPartLevel,
				BOOL fRangePart,
				BOOL *pfLTComparison,
				BOOL *pfGTComparison,
				BOOL *pfEQComparison
				);

			// translate a scalar null test on a part key
			CDXLNode *PdxlnScNullTestPartKey
				(
				IMDId *pmdidTypePartKey,
				ULONG ulPartLevel,
				BOOL fRangePart,
				BOOL is_null
				);

			// translate the child of a partition selector expression, pushing the given
			// scalar predicate if available
			CDXLNode *PdxlnPartitionSelectorChild
				(
				CExpression *pexprChild, 
				CExpression *pexprScalarCond, 
				CDXLPhysicalProperties *dxl_properties, 
				ColRefArray *colref_array, 
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);

			CDXLNode *PdxlArrayExprOnPartKey
				(
				CExpression *pexprPred,
				CColRef *pcrPartKey,
				IMDId *pmdidTypePartKey,
				ULONG ulPartLevel,
				BOOL fRangePart,
				BOOL *pfLTComparison,	// input/output
				BOOL *pfGTComparison,	// input/output
				BOOL *pfEQComparison	// input/output
				);

			// translate a DML operator
			CDXLNode *PdxlnDML(CExpression *pexpr, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate a CTAS operator
			CDXLNode *PdxlnCTAS(CExpression *pexpr, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);
			
			// translate a split operator
			CDXLNode *PdxlnSplit(CExpression *pexpr, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate an assert operator 
			CDXLNode *PdxlnAssert(CExpression *pexprAssert, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate a row trigger operator
			CDXLNode *PdxlnRowTrigger(CExpression *pexpr, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML);

			// translate a scalar If statement
			CDXLNode *PdxlnScIfStmt(CExpression *pexprScIf);

			// translate a scalar switch
			CDXLNode *PdxlnScSwitch(CExpression *pexprScSwitch);

			// translate a scalar switch case
			CDXLNode *PdxlnScSwitchCase(CExpression *pexprScSwitchCase);

			// translate a scalar case test
			CDXLNode *PdxlnScCaseTest(CExpression *pexprScCaseTest);

			// translate a scalar comparison
			CDXLNode *PdxlnScCmp(CExpression *pexprScCmp);
			
			// translate a scalar distinct comparison
			CDXLNode *PdxlnScDistinctCmp(CExpression *pexprScIsDistFrom);

			// translate a scalar op node
			CDXLNode *PdxlnScOp(CExpression *pexprScOp);

			// translate a scalar constant
			CDXLNode *PdxlnScConst(CExpression *pexprScConst);

			// translate a scalar coalesce
			CDXLNode *PdxlnScCoalesce(CExpression *pexprScCoalesce);

			// translate a scalar MinMax
			CDXLNode *PdxlnScMinMax(CExpression *pexprScMinMax);

			// translate a scalar boolean expression
			CDXLNode *PdxlnScBoolExpr(CExpression *pexprScBoolExpr);

			// translate a scalar identifier
			CDXLNode *PdxlnScId(CExpression *pexprScId);
			
			// translate a scalar function expression
			CDXLNode *PdxlnScFuncExpr(CExpression *pexprScFunc);

			// translate a scalar window function expression
			CDXLNode *PdxlnScWindowFuncExpr(CExpression *pexprScFunc);

			// get the DXL representation of the window stage
			EdxlWinStage Ews(CScalarWindowFunc::EWinStage ews) const;

			// translate a scalar aggref
			CDXLNode *PdxlnScAggref(CExpression *pexprScAggFunc);

			// translate a scalar nullif
			CDXLNode *PdxlnScNullIf(CExpression *pexprScNullIf);

			// translate a scalar null test
			CDXLNode *PdxlnScNullTest(CExpression *pexprScNullTest);
			
			// translate a scalar boolean test
			CDXLNode *PdxlnScBooleanTest(CExpression *pexprScBooleanTest);

			// translate a scalar cast
			CDXLNode *PdxlnScCast(CExpression *pexprScCast);
			
			// translate a scalar coerce
			CDXLNode *PdxlnScCoerceToDomain(CExpression *pexprScCoerce);

			// translate a scalar coerce using I/O functions
			CDXLNode *PdxlnScCoerceViaIO(CExpression *pexprScCoerce);

			// translate a scalar array coerce expr with element coerce function
			CDXLNode *PdxlnScArrayCoerceExpr(CExpression *pexprScArrayCoerceExpr);

			// translate an array
			CDXLNode *PdxlnArray(CExpression *pexpr);

			// translate an arrayref
			CDXLNode *PdxlnArrayRef(CExpression *pexpr);

			// translate an arrayref index list
			CDXLNode *PdxlnArrayRefIndexList(CExpression *pexpr);

			// translate the arrayref index list bound
			CDXLScalarArrayRefIndexList::EIndexListBound Eilb(const CScalarArrayRefIndexList::EIndexListType eilt);

			// translate an array compare
			CDXLNode *PdxlnArrayCmp(CExpression *pexpr);

			// translate an assert predicate
			CDXLNode *PdxlnAssertPredicate(CExpression *pexpr);
			
			// translate an assert constraint
			CDXLNode *PdxlnAssertConstraint(CExpression *pexpr);

			// translate a DML action expression
			CDXLNode *PdxlnDMLAction(CExpression *pexpr);

			// translate a window frame
			CDXLWindowFrame *GetWindowFrame(CWindowFrame *pwf);

			CDXLTableDescr *MakeDXLTableDescr(const CTableDescriptor *ptabdesc, const ColRefArray *pdrgpcrOutput);

			// compute physical properties like operator cost from the expression
			CDXLPhysicalProperties *GetProperties(const CExpression *pexpr);
					
			// translate a colref set of output col into a dxl proj list
			CDXLNode *PdxlnProjList(const CColRefSet *pcrsOutput, ColRefArray *colref_array);
			
			// translate a project list expression into a DXL proj list node 
			// according to the order specified in the dynamic array 
			CDXLNode *PdxlnProjList
						(
						const CExpression *pexprProjList, 
						const CColRefSet *pcrsOutput, 
						ColRefArray *colref_array
						);			

			// translate a project list expression into a DXL proj list node
			CDXLNode *PdxlnProjList
						(
						const CExpression *pexprProjList, 
						const CColRefSet *pcrsOutput
						);			

			// create a project list for a result node from a tuple of a 
			// const table get operator
			CDXLNode *PdxlnProjListFromConstTableGet
				(
				ColRefArray *pdrgpcrReqOutput, 
				ColRefArray *pdrgpcrCTGOutput,
				IDatumArray *pdrgpdatumValues
				);

			// create a DXL project elem node from a proj element expression
			CDXLNode *PdxlnProjElem(const CExpression *pexprProjElem);
								
			// create a project element for a computed column from a column reference
			// and m_bytearray_value expresison
			CDXLNode *PdxlnProjElem(const CColRef *colref,CDXLNode *pdxlnValue);
			
			// create a DXL sort col list node from an order spec
			CDXLNode *GetSortColListDXL(const COrderSpec *pos);
			
			// create a DXL sort col list node for a Motion expression
			CDXLNode *GetSortColListDXL(CExpression *pexprMotion);

			// create a DXL hash expr list from an array of hash columns
			CDXLNode *PdxlnHashExprList(const ExpressionArray *pdrgpexpr);
			
			// create a DXL filter node with the given scalar expression
			CDXLNode *PdxlnFilter(CDXLNode *pdxlnCond);
			
			// construct an array with input segment ids for the given motion expression
			IntPtrArray *GetInputSegIdsArray(CExpression *pexprMotion);
			
			// construct an array with output segment ids for the given motion expression
			IntPtrArray *GetOutputSegIdsArray(CExpression *pexprMotion);

			// find the position of the given colref in the array
			ULONG UlPosInArray(const CColRef *colref, const ColRefArray *colref_array) const;
			
			// return hash join type
			static
			EdxlJoinType EdxljtHashJoin(CPhysicalHashJoin *popHJ);

			// main translation routine for Expr tree -> DXL tree
			CDXLNode *CreateDXLNode(CExpression *pexpr, ColRefArray *colref_array, DrgPds *pdrgpdsBaseTables, ULONG *pulNonGatherMotions, BOOL *pfDML, BOOL fRemap, BOOL fRoot);

			// translate expression children and add them as children of the DXL node
			void TranslateScalarChildren(CExpression *pexpr, CDXLNode *dxlnode);

			// add a result node, if required a materialize node is added below result node to avoid deadlock hazard
			CDXLNode *PdxlnResult(CDXLPhysicalProperties *dxl_properties, CDXLNode *pdxlnPrL, CDXLNode *child_dxlnode);
		
			// add a materialize node
			CDXLNode *PdxlnMaterialize(CDXLNode *dxlnode);

			// add result node if necessary
			CDXLNode *PdxlnRemapOutputColumns
						(
						CExpression *pexpr,
						CDXLNode *dxlnode,
						ColRefArray *pdrgpcrRequired,
						ColRefArray *pdrgpcrOrder
						);

			// combines the ordered columns and required columns into a single list
			ColRefArray *PdrgpcrMerge
					(
					IMemoryPool *mp,
					ColRefArray *pdrgpcrOrder,
					ColRefArray *pdrgpcrRequired
					);

			// helper to add a project of bool constant
			CDXLNode *PdxlnProjectBoolConst(CDXLNode *dxlnode, BOOL value);

			// helper to build a Result expression with project list restricted to required column
			CDXLNode *PdxlnRestrictResult(CDXLNode *dxlnode, CColRef *colref);

			//	helper to build subplans from correlated LOJ
			void BuildSubplansForCorrelatedLOJ
				(
				CExpression *pexprCorrelatedLOJ,
				DXLColRefArray *dxl_colref_array,
				CDXLNode **ppdxlnScalar, // output: scalar condition after replacing inner child reference with subplan
				DrgPds *pdrgpdsBaseTables,
				ULONG *pulNonGatherMotions,
				BOOL *pfDML
				);

			// helper to build subplans of different types
			void BuildSubplans
				(
				CExpression *pexprCorrelatedNLJoin,
				DXLColRefArray *dxl_colref_array,
				CDXLNode **ppdxlnScalar, // output: scalar condition after replacing inner child reference with subplan
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);

			// helper to build scalar subplans from inner column references and store them
			// in subplan map
			void BuildScalarSubplans
				(
				ColRefArray *pdrgpcrInner,
				CExpression *pexprInner,
				DXLColRefArray *dxl_colref_array,
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);
			
			// helper to build subplans for quantified (ANY/ALL) subqueries
			CDXLNode *PdxlnQuantifiedSubplan
				(
				ColRefArray *pdrgpcrInner,
				CExpression *pexprCorrelatedNLJoin,
				DXLColRefArray *dxl_colref_array,
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);

			// helper to build subplans for existential subqueries
			CDXLNode *PdxlnExistentialSubplan
				(
				ColRefArray *pdrgpcrInner,
				CExpression *pexprCorrelatedNLJoin,
				DXLColRefArray *dxl_colref_array,
				DrgPds *pdrgpdsBaseTables, 
				ULONG *pulNonGatherMotions, 
				BOOL *pfDML
				);

			// compute the direct dispatch info for the given DML expression
			CDXLDirectDispatchInfo *GetDXLDirectDispatchInfo(CExpression *pexprDML);
			
			// check if the motion node is valid
			void CheckValidity(CDXLPhysicalMotion *motion);

			// check if result node imposes a motion hazard
			BOOL FNeedsMaterializeUnderResult(CDXLNode *proj_list_dxlnode, CDXLNode *child_dxlnode);

			// helper to find subplan type from a correlated left outer join expression
			static
			EdxlSubPlanType EdxlsubplantypeCorrelatedLOJ(CExpression *pexprCorrelatedLOJ);

			// helper to find subplan type from a correlated join expression
			static
			EdxlSubPlanType Edxlsubplantype(CExpression *pexprCorrelatedNLJoin);

			// add used columns in the bitmap re-check and the remaining scalar filter condition to the
			// required output column
			static
			void AddBitmapFilterColumns
				(
				IMemoryPool *mp,
				CPhysicalScan *pop,
				CExpression *pexprRecheckCond,
				CExpression *pexprScalar,
				CColRefSet *pcrsReqdOutput // append the required column reference
				);
	public:
			// ctor
			CTranslatorExprToDXL
				(
				IMemoryPool *mp,
				CMDAccessor *md_accessor,
				IntPtrArray *pdrgpiSegments,
				BOOL fInitColumnFactory = true
				);
			
			// dtor
			~CTranslatorExprToDXL();
			
			// main driver
			CDXLNode *PdxlnTranslate(CExpression *pexpr, ColRefArray *colref_array, MDNameArray *pdrgpmdname);

			// translate a scalar expression into a DXL scalar node
			// if the expression is not a scalar, an UnsupportedOp exception is raised
			CDXLNode *PdxlnScalar(CExpression *pexpr);
	};
}

#endif // !GPOPT_CTranslatorExprToDXL_H

// EOF
