//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CPhysicalUnionAll_H
#define GPOPT_CPhysicalUnionAll_H

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDistributionSpecHashed.h"

#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/COperator.h"


namespace gpopt
{
	class CPhysicalUnionAll : public CPhysical
	{
		private:

			// output column array
			ColRefArray *const m_pdrgpcrOutput;

			// input column array
			ColRefArrays *const m_pdrgpdrgpcrInput;

			// if this union is needed for partial indexes then store the scan
			// id, otherwise this will be gpos::ulong_max
			const ULONG m_ulScanIdPartialIndex;

			// set representation of input columns
			ColRefSetArray *m_pdrgpcrsInput;

			// array of child hashed distributions -- used locally for distribution derivation
			DrgPds *const m_pdrgpds;

			// map given array of scalar ident expressions to positions of UnionAll input columns in the given child;
			ULongPtrArray *PdrgpulMap(IMemoryPool *memory_pool, ExpressionArray *pdrgpexpr, ULONG child_index) const;

			// derive hashed distribution from child operators
			CDistributionSpecHashed *PdshashedDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// compute output hashed distribution matching the outer child's hashed distribution
			CDistributionSpecHashed *PdsMatching(IMemoryPool *memory_pool, const ULongPtrArray *pdrgpulOuter) const;

			// derive output distribution based on child distribution
			CDistributionSpec *PdsDeriveFromChildren(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

		protected:

			CColRefSet *PcrsInput(ULONG child_index);

			// compute required hashed distribution of the n-th child
			CDistributionSpecHashed *
			PdshashedPassThru(IMemoryPool *memory_pool, CDistributionSpecHashed *pdshashedRequired, ULONG child_index) const;

		public:

			CPhysicalUnionAll
				(
					IMemoryPool *memory_pool,
					ColRefArray *pdrgpcrOutput,
					ColRefArrays *pdrgpdrgpcrInput,
					ULONG ulScanIdPartialIndex
				);

			virtual
			~CPhysicalUnionAll();

			// match function
			virtual
			BOOL Matches(COperator *) const;

			// ident accessors
			virtual
			EOperatorId Eopid() const = 0;

			virtual
			const CHAR *SzId() const = 0;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const;

			// accessor of output column array
			ColRefArray *PdrgpcrOutput() const;

			// accessor of input column array
			ColRefArrays *PdrgpdrgpcrInput() const;

			// if this unionall is needed for partial indexes then return the scan
			// id, otherwise return gpos::ulong_max
			ULONG UlScanIdPartialIndex() const;

			// is this unionall needed for a partial index
			BOOL IsPartialIndex() const;

			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const;

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required output columns of the n-th child
			virtual
			CColRefSet *PcrsRequired
				(
					IMemoryPool *memory_pool,
					CExpressionHandle &exprhdl,
					CColRefSet *pcrsRequired,
					ULONG child_index,
					DrgPdp *pdrgpdpCtxt,
					ULONG ulOptReq
				);

			// compute required ctes of the n-th child
			virtual
			CCTEReq *PcteRequired
				(
					IMemoryPool *memory_pool,
					CExpressionHandle &exprhdl,
					CCTEReq *pcter,
					ULONG child_index,
					DrgPdp *pdrgpdpCtxt,
					ULONG ulOptReq
				)
			const;

			// compute required sort order of the n-th child
			virtual
			COrderSpec *PosRequired
				(
					IMemoryPool *memory_pool,
					CExpressionHandle &exprhdl,
					COrderSpec *posRequired,
					ULONG child_index,
					DrgPdp *pdrgpdpCtxt,
					ULONG ulOptReq
				)
			const;

			// compute required rewindability of the n-th child
			virtual
			CRewindabilitySpec *PrsRequired
				(
					IMemoryPool *memory_pool,
					CExpressionHandle &exprhdl,
					CRewindabilitySpec *prsRequired,
					ULONG child_index,
					DrgPdp *pdrgpdpCtxt,
					ULONG ulOptReq
				)
			const;

			// compute required partition propagation of the n-th child
			virtual
			CPartitionPropagationSpec *PppsRequired
				(
					IMemoryPool *memory_pool,
					CExpressionHandle &exprhdl,
					CPartitionPropagationSpec *pppsRequired,
					ULONG child_index,
					DrgPdp *pdrgpdpCtxt,
					ULONG ulOptReq
				);

			// conversion function
			static
			CPhysicalUnionAll *PopConvert
				(
					COperator *pop
				);


			// check if required columns are included in output columns
			virtual
			BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const;

			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive sort order
			virtual
			COrderSpec *PosDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt) const;

			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
					IMemoryPool *memory_pool,
					CExpressionHandle &exprhdl
				)
			const;

			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			//-------------------------------------------------------------------------------------
			// Enforced Properties
			//-------------------------------------------------------------------------------------


			// return order property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetOrder
				(
					CExpressionHandle &exprhdl,
					const CEnfdOrder *peo
				)
			const;

			// return rewindability property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetRewindability
				(
					CExpressionHandle &, // exprhdl
					const CEnfdRewindability * // per
				)
			const;

			// return partition propagation property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetPartitionPropagation
				(
					CExpressionHandle &exprhdl,
					const CEnfdPartitionPropagation *pepp
				)
			const;
	};
}

#endif
