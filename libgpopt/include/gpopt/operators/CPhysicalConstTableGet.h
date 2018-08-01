//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalConstTableGet.h
//
//	@doc:
//		Physical const table get
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalConstTableGet_H
#define GPOPT_CPhysicalConstTableGet_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/CLogicalConstTableGet.h"

namespace gpopt
{
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalConstTableGet
	//
	//	@doc:
	//		Physical const table get operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalConstTableGet : public CPhysical
	{

		private:

			// array of column descriptors: the schema of the const table
			DrgPcoldesc *m_pdrgpcoldesc;
		
			// array of datum arrays
			DrgPdrgPdatum *m_pdrgpdrgpdatum;
			
			// output columns
			DrgPcr *m_pdrgpcrOutput;
			
			// private copy ctor
			CPhysicalConstTableGet(const CPhysicalConstTableGet &);

		public:
		
			// ctor
			CPhysicalConstTableGet
				(
				IMemoryPool *memory_pool,
				DrgPcoldesc *pdrgpcoldesc,
				DrgPdrgPdatum *pdrgpdrgpconst,
				DrgPcr *pdrgpcrOutput
				);

			// dtor
			virtual 
			~CPhysicalConstTableGet();

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopPhysicalConstTableGet;
			}
			
			virtual 
			const CHAR *SzId() const
			{
				return "CPhysicalConstTableGet";
			}

			// col descr accessor
			DrgPcoldesc *Pdrgpcoldesc() const
			{
				return m_pdrgpcoldesc;
			}
			
			// const table values accessor
			DrgPdrgPdatum *Pdrgpdrgpdatum () const
			{
				return m_pdrgpdrgpdatum;
			}
			
			// output columns accessors
			DrgPcr *PdrgpcrOutput() const
			{
				return m_pdrgpcrOutput;
			}


			// match function
			virtual
			BOOL Matches(COperator *) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
				return false;
			}

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
		
			// compute required distribution of the n-th child
			virtual
			CDistributionSpec *PdsRequired
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsRequired,
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
				IMemoryPool *, //memory_pool,
				CExpressionHandle &, //exprhdl,
				CPartitionPropagationSpec *, //pppsRequired,
				ULONG , //child_index,
				DrgPdp *, //pdrgpdpCtxt,
				ULONG //ulOptReq
				)
			{
				GPOS_ASSERT(!"CPhysicalConstTableGet has no relational children");
				return NULL;
			}
			
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
			
			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive cte map
			virtual
			CCTEMap *PcmDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &, //exprhdl
				CDrvdPropCtxt * //pdpctxt
				)
				const
			{
				return GPOS_NEW(memory_pool) CPartIndexMap(memory_pool);
			}
			
			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
				IMemoryPool *memory_pool,
				CExpressionHandle & // exprhdl
				)
				const
			{
				// return empty part filter map
				return GPOS_NEW(memory_pool) CPartFilterMap(memory_pool);
			}

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
				CExpressionHandle &, // exprhdl,
				const CEnfdPartitionPropagation * // pepp
				) 
				const
			{
				return CEnfdProp::EpetRequired;
			}
			
			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const
			{
				return false;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CPhysicalConstTableGet *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalConstTableGet == pop->Eopid());
				
				return dynamic_cast<CPhysicalConstTableGet*>(pop);
			}
		
			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CPhysicalConstTableGet

}

#endif // !GPOPT_CPhysicalConstTableGet_H

// EOF
