//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CStatisticsTest.h
//
//	@doc:
//		Test for CPoint
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatisticsTest_H
#define GPNAUCRATES_CStatisticsTest_H

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredDisj.h"

// fwd declarations
namespace gpopt
{
	class CTableDescriptor;
}

namespace gpmd
{
	class IMDTypeInt4;
}

namespace gpnaucrates
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatisticsTest
	//
	//	@doc:
	//		Static unit tests for statistics objects
	//
	//---------------------------------------------------------------------------
	class CStatisticsTest
	{
		private:

			// test case for union all evaluation
			struct SStatsUnionAllSTestCase
			{
				// input stats dxl file
				const CHAR *m_szInputFile;

				// output stats dxl file
				const CHAR *m_szOutputFile;
			};

			// create filter on int4 types
			static
			void StatsFilterInt4(IMemoryPool *memory_pool, ULONG col_id, INT iLower, INT iUpper, StatsPredPtrArry *pgrgpstatspred);

			// create filter on boolean types
			static
			void StatsFilterBool(IMemoryPool *memory_pool, ULONG col_id, BOOL fValue, StatsPredPtrArry *pgrgpstatspred);

			// create filter on numeric types
			static
			void
			StatsFilterNumeric
				(
				IMemoryPool *memory_pool,
				ULONG col_id,
				CWStringDynamic *pstrLowerEncoded,
				CWStringDynamic *pstrUpperEncoded,
				CDouble dValLower,
				CDouble dValUpper,
				StatsPredPtrArry *pdrgpstatspred
				);

			// create filter on generic types
			static
			void StatsFilterGeneric
				(
				IMemoryPool *memory_pool,
				ULONG col_id,
				OID oid,
				CWStringDynamic *pstrLowerEncoded,
				CWStringDynamic *pstrUpperEncoded,
				LINT lValLower,
				LINT lValUpper,
				StatsPredPtrArry *pgrgpstatspred
				);

			static
			CHistogram* PhistExampleInt4Dim(IMemoryPool *memory_pool);

			// helper function that generates an array of ULONG pointers
			static
			ULongPtrArray *
					Pdrgpul(IMemoryPool *memory_pool,
					ULONG ul1,
					ULONG ul2 = gpos::ulong_max
					)
			{
				ULongPtrArray *pdrgpul = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
				pdrgpul->Append(GPOS_NEW(memory_pool) ULONG (ul1));

				if (gpos::ulong_max != ul2)
				{
					pdrgpul->Append(GPOS_NEW(memory_pool) ULONG (ul2));
				}

				return pdrgpul;
			}

			// create a table descriptor with two columns having the given names
			static
			CTableDescriptor *PtabdescTwoColumnSource
				(
				IMemoryPool *memory_pool,
				const CName &nameTable,
				const IMDTypeInt4 *pmdtype,
				const CWStringConst &strColA,
				const CWStringConst &strColB
				);

		public:

			// example filter
			static
			StatsPredPtrArry *Pdrgpstatspred1(IMemoryPool *memory_pool);

			static
			StatsPredPtrArry *Pdrgpstatspred2(IMemoryPool *memory_pool);

			// unittests
			static
			GPOS_RESULT EresUnittest();

			// union all tests
			static
			GPOS_RESULT EresUnittest_UnionAll();

			// statistics basic tests
			static
			GPOS_RESULT EresUnittest_CStatisticsBasic();

			// exercise stats derivation during optimization
			static
			GPOS_RESULT EresUnittest_CStatisticsSelectDerivation();

			// GbAgg test when grouping on repeated columns
			static
			GPOS_RESULT EresUnittest_GbAggWithRepeatedGbCols();


	}; // class CStatisticsTest
}

#endif // !GPNAUCRATES_CStatisticsTest_H


// EOF
