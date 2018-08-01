//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSplit.h
//
//	@doc:
//		Logical split operator used for DML updates
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalSplit_H
#define GPOPT_CLogicalSplit_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{

	// fwd declarations
	class CTableDescriptor;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalSplit
	//
	//	@doc:
	//		Logical split operator
	//
	//---------------------------------------------------------------------------
	class CLogicalSplit : public CLogical
	{

		private:

			// deletion columns
			ColRefArray *m_pdrgpcrDelete;

			// insertion columns
			ColRefArray *m_pdrgpcrInsert;

			// ctid column
			CColRef *m_pcrCtid;

			// segmentId column
			CColRef *m_pcrSegmentId;

			// action column
			CColRef *m_pcrAction;
			
			// tuple oid column
			CColRef *m_pcrTupleOid;

			// private copy ctor
			CLogicalSplit(const CLogicalSplit &);

		public:

			// ctor
			explicit
			CLogicalSplit(IMemoryPool *memory_pool);

			// ctor
			CLogicalSplit
				(
				IMemoryPool *memory_pool,
				ColRefArray *pdrgpcrDelete,
				ColRefArray *pdrgpcrInsert,
				CColRef *pcrCtid,
				CColRef *pcrSegmentId,
				CColRef *pcrAction,
				CColRef *pcrTupleOid
				);

			// dtor
			virtual
			~CLogicalSplit();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalSplit;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalSplit";
			}

			// deletion columns
			ColRefArray *PdrgpcrDelete() const
			{
				return m_pdrgpcrDelete;
			}

			// insertion columns
			ColRefArray *PdrgpcrInsert() const
			{
				return m_pdrgpcrInsert;
			}

			// ctid column
			CColRef *PcrCtid() const
			{
				return m_pcrCtid;
			}

			// segmentId column
			CColRef *PcrSegmentId() const
			{
				return m_pcrSegmentId;
			}

			// action column
			CColRef *PcrAction() const
			{
				return m_pcrAction;
			}
			
			// tuple oid column
			CColRef *PcrTupleOid() const
			{
				return m_pcrTupleOid;
			}

			// operator specific hash function
			virtual
			ULONG HashValue() const;

			// match function
			BOOL Matches(COperator *pop) const;

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *memory_pool, UlongColRefHashMap *colref_mapping, BOOL must_exist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *memory_pool, CExpressionHandle &exprhdl);


			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, // memory_pool
				CExpressionHandle &exprhdl
				)
				const
			{
				return CLogical::PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
			}

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *, // memory_pool,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpartinfoPassThruOuter(exprhdl);
			}

			// compute required stats columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsInput,
				ULONG child_index
				)
				const
			{
				return PcrsReqdChildStats
						(
						memory_pool,
						exprhdl,
						pcrsInput,
						exprhdl.GetDrvdScalarProps(1)->PcrsUsed(),
						child_index
						);
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			CXformSet *PxfsCandidates(IMemoryPool *memory_pool) const;

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *memory_pool,
						CExpressionHandle &exprhdl,
						StatsArray *stats_ctxt
						)
						const;

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalSplit *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalSplit == pop->Eopid());

				return dynamic_cast<CLogicalSplit*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalSplit
}

#endif // !GPOPT_CLogicalSplit_H

// EOF
