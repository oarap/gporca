//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintDisjunction.h
//
//	@doc:
//		Representation of a disjunction constraint. A disjunction is a number
//		of ORed constraints
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintDisjunction_H
#define GPOPT_CConstraintDisjunction_H

#include "gpos/base.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CRange.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CConstraintDisjunction
	//
	//	@doc:
	//		Representation of a disjunction constraint
	//
	//---------------------------------------------------------------------------
	class CConstraintDisjunction : public CConstraint
	{
		private:

			// array of constraints
			ConstraintArray *m_pdrgpcnstr;

			// mapping colref -> array of child constraints
			ColRefToConstraintArrayMap *m_phmcolconstr;

			// hidden copy ctor
			CConstraintDisjunction(const CConstraintDisjunction&);

		public:

			// ctor
			CConstraintDisjunction(IMemoryPool *memory_pool, ConstraintArray *pdrgpcnstr);

			// dtor
			virtual
			~CConstraintDisjunction();

			// constraint type accessor
			virtual
			EConstraintType Ect() const
			{
				return CConstraint::EctDisjunction;
			}

			// all constraints in disjunction
			ConstraintArray *Pdrgpcnstr() const
			{
				return m_pdrgpcnstr;
			}

			// is this constraint a contradiction
			virtual
			BOOL FContradiction() const;

			// return a copy of the constraint with remapped columns
			virtual
			CConstraint *PcnstrCopyWithRemappedColumns(IMemoryPool *memory_pool, UlongColRefHashMap *colref_mapping, BOOL must_exist);

			// scalar expression
			virtual
			CExpression *PexprScalar(IMemoryPool *memory_pool);

			// check if there is a constraint on the given column
			virtual
			BOOL FConstraint(const CColRef *colref) const;

			// return constraint on a given column
			virtual
			CConstraint *Pcnstr(IMemoryPool *memory_pool, const CColRef *colref);

			// return constraint on a given column set
			virtual
			CConstraint *Pcnstr(IMemoryPool *memory_pool, CColRefSet *pcrs);

			// return a clone of the constraint for a different column
			virtual
			CConstraint *PcnstrRemapForColumn(IMemoryPool *memory_pool, CColRef *colref) const;

			// print
			virtual
			IOstream &OsPrint
						(
						IOstream &os
						)
						const
			{
				return PrintConjunctionDisjunction(os, m_pdrgpcnstr);
			}

	}; // class CConstraintDisjunction
}

#endif // !GPOPT_CConstraintDisjunction_H

// EOF
