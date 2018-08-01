//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CFilterStatsProcessor.h
//
//	@doc:
//		Compute statistics for filter operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CFilterStatsProcessor_H
#define GPNAUCRATES_CFilterStatsProcessor_H

#include "gpos/base.h"
#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CScaleFactorUtils.h"

namespace gpnaucrates
{

	class CFilterStatsProcessor
	{
		private:

			// create a new histogram after applying the filter that is not an AND/OR predicate
			static
			CHistogram *MakeHistSimpleFilter
				(
				IMemoryPool *memory_pool,
				CStatsPred *pred_stats,
				CBitSet *filter_col_ids,
				CHistogram *hist_before,
				CDouble *last_scale_factor,
				ULONG *target_last_col_id
				);

			// create a new histogram after applying a point filter
			static
			CHistogram *MakeHistPointFilter
				(
				IMemoryPool *memory_pool,
				CStatsPredPoint *pred_stats,
				CBitSet *filter_col_ids,
				CHistogram *hist_before,
				CDouble *last_scale_factor,
				ULONG *target_last_col_id
				);

			// create a new histogram after applying a LIKE filter
			static
			CHistogram *MakeHistLikeFilter
				(
				IMemoryPool *memory_pool,
				CStatsPredLike *pred_stats,
				CBitSet *filter_col_ids,
				CHistogram *hist_before,
				CDouble *last_scale_factor,
				ULONG *target_last_col_id
				);

			// create a new histogram for an unsupported predicate
			static
			CHistogram *MakeHistUnsupportedPred
				(
				IMemoryPool *memory_pool,
				CStatsPredUnsupported *pred_stats,
				CBitSet *filter_col_ids,
				CHistogram *hist_before,
				CDouble *last_scale_factor,
				ULONG *target_last_col_id
				);

			// create a new hash map of histograms after applying a conjunctive or disjunctive filter
			static
			UlongHistogramHashMap *MakeHistHashMapConjOrDisjFilter
				(
				IMemoryPool *memory_pool,
				const CStatisticsConfig *stats_config,
				UlongHistogramHashMap *input_histograms,
				CDouble input_rows,
				CStatsPred *pred_stats,
				CDouble *scale_factor
				);

			// create new hash map of histograms after applying the conjunction predicate
			static
			UlongHistogramHashMap *MakeHistHashMapConjFilter
				(
				IMemoryPool *memory_pool,
				const CStatisticsConfig *stats_config,
				UlongHistogramHashMap *intermediate_histograms,
				CDouble input_rows,
				CStatsPredConj *conjunctive_pred_stats,
				CDouble *scale_factor
				);

			// create new hash map of histograms after applying the disjunctive predicate
			static
			UlongHistogramHashMap *MakeHistHashMapDisjFilter
				(
				IMemoryPool *memory_pool,
				const CStatisticsConfig *stats_config,
				UlongHistogramHashMap *input_histograms,
				CDouble input_rows,
				CStatsPredDisj *pred_stats,
				CDouble *scale_factor
				);

			// check if the column is a new column for statistic calculation
			static
			BOOL IsNewStatsColumn(ULONG col_id, ULONG last_col_id);

		public:

		// filter
		static
		CStatistics *MakeStatsFilter(IMemoryPool *memory_pool, const CStatistics *input_stats, CStatsPred *base_pred_stats, BOOL do_cap_NDVs);

		// derive statistics for filter operation based on given scalar expression
		static
		IStatistics *MakeStatsFilterForScalarExpr
						(
						IMemoryPool *memory_pool,
						CExpressionHandle &exprhdl,
						IStatistics *child_stats,
						CExpression *local_scalar_expr, // filter expression on local columns only
						CExpression *outer_refs_scalar_expr, // filter expression involving outer references
						StatsArray *all_outer_stats
						);
	};
}

#endif // !GPNAUCRATES_CFilterStatsProcessor_H

// EOF

