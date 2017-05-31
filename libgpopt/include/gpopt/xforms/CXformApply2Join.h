//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformApply2Join.h
//
//	@doc:
//		Base class for transforming Apply to Join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformApply2Join_H
#define GPOPT_CXformApply2Join_H

#include "gpos/base.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "gpopt/xforms/CDecorrelator.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformApply2Join
	//
	//	@doc:
	//		Transform Apply into Join by decorrelating the inner side
	//
	//---------------------------------------------------------------------------
	template<class TApply, class TJoin>
	class CXformApply2Join : public CXformExploration
	{

		private:

			// check if we can create a correlated apply expression from the given expression
			static
			BOOL FCanCreateCorrelatedApply(IMemoryPool *, CExpression *pexprApply)
            {
                GPOS_ASSERT(NULL != pexprApply);

                COperator::EOperatorId eopid = pexprApply->Pop()->Eopid();

                // consider only Inner/Outer/Left (Anti) Semi Apply here,
                // correlated left anti semi apply (with ALL/NOT-IN semantics) can only be generated by SubqueryHandler
                return
                COperator::EopLogicalInnerApply == eopid ||
                COperator::EopLogicalLeftOuterApply == eopid ||
                COperator::EopLogicalLeftSemiApply == eopid ||
                COperator::EopLogicalLeftSemiApplyIn == eopid ||
                COperator::EopLogicalLeftAntiSemiApply == eopid;
            }

			// create correlated apply expression
			static
			void CreateCorrelatedApply(IMemoryPool *pmp, CExpression *pexprApply, CXformResult *pxfres)
            {
                if (!FCanCreateCorrelatedApply(pmp, pexprApply))
                {
                    return;
                }

                CExpression *pexprInner = (*pexprApply)[1];
                CExpression *pexprOuter = (*pexprApply)[0];
                CExpression *pexprScalar = (*pexprApply)[2];

                pexprOuter->AddRef();
                pexprInner->AddRef();
                pexprScalar->AddRef();
                CExpression *pexprResult = NULL;

                TApply *popApply = TApply::PopConvert(pexprApply->Pop());
                DrgPcr *pdrgpcr = popApply->PdrgPcrInner();
                GPOS_ASSERT(NULL != pdrgpcr);
                GPOS_ASSERT(1 == pdrgpcr->UlLength());

                pdrgpcr->AddRef();
                COperator::EOperatorId eopidSubq = popApply->EopidOriginSubq();
                COperator::EOperatorId eopid = pexprApply->Pop()->Eopid();
                switch (eopid)
                {
                    case COperator::EopLogicalInnerApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalInnerCorrelatedApply>(pmp, pexprOuter, pexprInner, pdrgpcr, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftOuterApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(pmp, pexprOuter, pexprInner, pdrgpcr, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftSemiApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApply>(pmp, pexprOuter, pexprInner, pdrgpcr, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftSemiApplyIn:
                        pexprResult = CUtils::PexprLogicalCorrelatedQuantifiedApply<CLogicalLeftSemiCorrelatedApplyIn>(pmp, pexprOuter, pexprInner, pdrgpcr, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftAntiSemiApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApply>(pmp, pexprOuter, pexprInner, pdrgpcr, eopidSubq, pexprScalar);
                        break;

                    default:
                        GPOS_ASSERT(!"Unexpected Apply operator");
                        return;
                }

                pxfres->Add(pexprResult);
            }

			// private copy ctor
			CXformApply2Join(const CXformApply2Join &);
			
		protected:

			// helper function to attempt decorrelating Apply's inner child
			static
			BOOL FDecorrelate(IMemoryPool *pmp, CExpression *pexprApply, CExpression **ppexprInner, DrgPexpr **ppdrgpexpr)
            {
                GPOS_ASSERT(NULL != pexprApply);
                GPOS_ASSERT(NULL != ppexprInner);
                GPOS_ASSERT(NULL != ppdrgpexpr);

                *ppdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

                CExpression *pexprPredicateOrig = (*pexprApply)[2];

                // add original predicate to array
                pexprPredicateOrig->AddRef();
                (*ppdrgpexpr)->Append(pexprPredicateOrig);

                // since properties of inner child have been copied from
                // groups that may had subqueries that were decorrelated later, we reset
                // properties here to allow re-computing them during decorrelation
                (*pexprApply)[1]->ResetDerivedProperties();

                // decorrelate inner child
                if (!CDecorrelator::FProcess(pmp, (*pexprApply)[1], false /*fEqualityOnly*/, ppexprInner, *ppdrgpexpr))
                {
                    // decorrelation filed
                    (*ppdrgpexpr)->Release();
                    return false;
                }

                // check for valid semi join correlations
                if ((COperator::EopLogicalLeftSemiJoin == pexprApply->Pop()->Eopid() ||
                     COperator::EopLogicalLeftAntiSemiJoin == pexprApply->Pop()->Eopid()) &&
                    !CPredicateUtils::FValidSemiJoinCorrelations(pmp, (*pexprApply)[0], (*ppexprInner), (*ppdrgpexpr))
                    )
                {
                    (*ppdrgpexpr)->Release();

                    return false;
                }

                return true;
            }

			// helper function to decorrelate apply expression and insert alternative into results container
			static
			void Decorrelate(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexprApply)
            {
                GPOS_ASSERT(CUtils::FHasOuterRefs((*pexprApply)[1])
                            && "Apply's inner child must have outer references");

                if (CUtils::FHasSubqueryOrApply((*pexprApply)[1]))
                {
                    // Subquery/Apply must be unnested before reaching here
                    return;
                }

                IMemoryPool *pmp = pxfctxt->Pmp();
                DrgPexpr *pdrgpexpr = NULL;
                CExpression *pexprInner = NULL;
                if (!FDecorrelate(pmp, pexprApply, &pexprInner, &pdrgpexpr))
                {
                    // decorrelation failed, create correlated apply expression if possible
                    CreateCorrelatedApply(pmp, pexprApply, pxfres);

                    return;
                }

                // build substitute
                GPOS_ASSERT(NULL != pexprInner);
                (*pexprApply)[0]->AddRef();
                CExpression *pexprOuter = (*pexprApply)[0];
                CExpression *pexprPredicate = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);

                CExpression *pexprResult =
                GPOS_NEW(pmp) CExpression
                (
                 pmp,
                 GPOS_NEW(pmp) TJoin(pmp), // join operator
                 pexprOuter,
                 pexprInner,
                 pexprPredicate
                 );

                // add alternative to results
                pxfres->Add(pexprResult);
            }

			// helper function to create a join expression from an apply expression and insert alternative into results container
			static
			void CreateJoinAlternative(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexprApply)
            {
#ifdef GPOS_DEBUG
                CExpressionHandle exprhdl(pxfctxt->Pmp());
                exprhdl.Attach(pexprApply);
                GPOS_ASSERT_IMP(CUtils::FHasOuterRefs((*pexprApply)[1]), !exprhdl.Pdprel(1)->PcrsOuter()->FSubset(exprhdl.Pdprel(0)->PcrsOutput())
                                && "Apply's inner child can only use external columns");
#endif // GPOS_DEBUG

                IMemoryPool *pmp = pxfctxt->Pmp();
                CExpression *pexprOuter = (*pexprApply)[0];
                CExpression *pexprInner = (*pexprApply)[1];
                CExpression *pexprPred = (*pexprApply)[2];
                pexprOuter->AddRef();
                pexprInner->AddRef();
                pexprPred->AddRef();
                CExpression *pexprResult =
                GPOS_NEW(pmp) CExpression
                (
                 pmp,
                 GPOS_NEW(pmp) TJoin(pmp), // join operator
                 pexprOuter,
                 pexprInner,
                 pexprPred
                 );

                // add alternative to results
                pxfres->Add(pexprResult);
            }

		public:

			// ctor for deep pattern
			explicit
			CXformApply2Join<TApply, TJoin>(IMemoryPool *pmp, BOOL )
                :
                // pattern
                CXformExploration
                (
                 GPOS_NEW(pmp) CExpression
                 (
                  pmp,
                  GPOS_NEW(pmp) TApply(pmp),
                  GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
                  GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)), // right child
                  GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)) // predicate
                  )
                 )
            {}

			// ctor for shallow pattern
			explicit
			CXformApply2Join<TApply, TJoin>(IMemoryPool *pmp)
                :
                // pattern
                CXformExploration
                (
                 GPOS_NEW(pmp) CExpression
                 (
                  pmp,
                  GPOS_NEW(pmp) TApply(pmp),
                  GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
                  GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
                  GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // predicate
                  )
                 )
            {}

			// ctor for passed pattern
			CXformApply2Join<TApply, TJoin>
				(
				IMemoryPool *, // pmp
				CExpression *pexprPattern
				)
				:
				CXformExploration(pexprPattern)
			{}

			// dtor
			virtual
			~CXformApply2Join<TApply, TJoin>()
			{}

			// is transformation an Apply decorrelation (Apply To Join) xform?
			virtual
			BOOL FApplyDecorrelating() const
			{
				return true;
			}

	}; // class CXformApply2Join

}

#endif // !GPOPT_CXformApply2Join_H

// EOF
